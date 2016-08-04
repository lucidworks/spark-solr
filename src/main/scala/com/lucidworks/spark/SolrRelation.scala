package com.lucidworks.spark

import java.util.UUID

import com.lucidworks.spark.query.sql.SolrSQLSupport
import com.lucidworks.spark.util._
import com.lucidworks.spark.rdd.SolrRDD
import org.apache.http.entity.StringEntity
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.SolrQuery.SortClause
import org.apache.solr.client.solrj.io.stream.expr._
import org.apache.solr.client.solrj.request.schema.SchemaRequest.{Update, AddField, MultiUpdate}
import org.apache.solr.common.SolrException.ErrorCode
import org.apache.solr.common.{SolrException, SolrInputDocument}
import org.apache.solr.common.params.{CommonParams, ModifiableSolrParams}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ParserDialect
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.Logging

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
import scala.reflect.runtime.universe._

import com.lucidworks.spark.util.QueryConstants._
import com.lucidworks.spark.util.ConfigurationConstants._

class SolrRelation(
    val parameters: Map[String, String],
    override val sqlContext: SQLContext,
    val dataFrame: Option[DataFrame])(
  implicit
    val conf: SolrConf = new SolrConf(parameters))
  extends BaseRelation
  with Serializable
  with TableScan
  with PrunedFilteredScan
  with InsertableRelation
  with Logging {

  def this(parameters: Map[String, String], sqlContext: SQLContext) {
    this(parameters, sqlContext, None)
  }

  checkRequiredParams()
  var collection = conf.getCollection.get
  // Warn about unknown parameters
  val unknownParams = SolrRelation.checkUnknownParams(parameters.keySet)
  if (unknownParams.nonEmpty)
    log.warn("Unknown parameters passed to query: " + unknownParams.toString())
  val sc = sqlContext.sparkContext

  if (!conf.partition_by.isEmpty && conf.partition_by.get=="time") {
    val feature = new PartitionByTimeQueryParams(conf)
    val p = new PartitionByTimeQuerySupport(feature,conf)
    val allCollections = p.getPartitionsForQuery()
    collection = allCollections mkString ","
  }

  val solrRDD = {
    var rdd = new SolrRDD(
      conf.getZkHost.get,
      collection,
      sc,
      requestHandler = Some(conf.requestHandler))

    if (conf.splits.isDefined && conf.getSplitsPerShard.isDefined) {
      rdd = rdd.doSplits().splitsPerShard(conf.getSplitsPerShard.get)
    } else if (conf.splits.isDefined) {
      rdd = rdd.doSplits()
    }

    if (conf.getSplitField.isDefined && conf.getSplitsPerShard.isDefined) {
      rdd = rdd.splitField(conf.getSplitField.get).splitsPerShard(conf.getSplitsPerShard.get)
    } else if (conf.getSplitField.isDefined) {
      rdd = rdd.splitField(conf.getSplitField.get)
    }

    rdd
  }

  val arbitraryParams = conf.getArbitrarySolrParams
  val solrFields: Array[String] = {
    if (arbitraryParams.getParameterNames.contains(CommonParams.FL)) {
      arbitraryParams.getParams(CommonParams.FL)
    } else {
      conf.getFields
    }
  }

  // we don't need the baseSchema for streaming expressions, so we wrap it in an optional
  var baseSchema : Option[StructType] = None

  val query: SolrQuery = buildQuery
  // Preserve the initial filters if any present in arbitrary config
  var queryFilters: Array[String] = if (query.getFilterQueries != null) query.getFilterQueries else Array.empty[String]

  val querySchema: StructType = {
    if (dataFrame.isDefined) {
      dataFrame.get.schema
    } else {
      if (query.getFields != null) {
        baseSchema = Some(getBaseSchemaFromConfig(collection, solrFields))
        SolrRelationUtil.deriveQuerySchema(query.getFields.split(","), baseSchema.get)
      } else if (conf.requestHandler == QT_STREAM) {
        // we have to figure out the schema of the streaming expression
        var streamingExpr = StreamExpressionParser.parse(query.get(SOLR_STREAMING_EXPR))
        var streamOutputFields = new ListBuffer[StreamFields]
        findStreamingExpressionFields(streamingExpr, streamOutputFields)
        logDebug(s"Found ${streamOutputFields.size} stream output fields: ${streamOutputFields}")
        var fieldSet: scala.collection.mutable.Set[StructField] = scala.collection.mutable.Set[StructField]()
        for (sf <- streamOutputFields) {
          val streamSchema: StructType =
            SolrRelationUtil.getBaseSchema(
              sf.fields.toSet,
              conf.getZkHost.get,
              sf.collection,
              conf.escapeFieldNames.getOrElse(false),
              conf.flattenMultivalued.getOrElse(true))
          logDebug(s"Got stream schema: ${streamSchema} for ${sf}")
          streamSchema.fields.foreach(fld => fieldSet.add(fld))
          sf.metrics.foreach(m => fieldSet.add(toMetricStructField(m)))
        }
        if (fieldSet.isEmpty) {
          throw new IllegalStateException("Failed to extract schema fields for streaming expression: " + streamingExpr)
        }
        var exprSchema = new StructType(fieldSet.toArray.sortBy(f => f.name))
        logDebug(s"Created combined schema with ${exprSchema.fieldNames.size} fields for streaming expression: ${exprSchema}: ${exprSchema.fields}")
        exprSchema
      } else if (conf.requestHandler == QT_SQL) {
        val sqlStmt = query.get(SOLR_SQL_STMT)
        logInfo(s"Determining schema for Solr SQL: ${sqlStmt}")

        val allFieldsSchema : StructType = getBaseSchemaFromConfig(collection, Array.empty)
        baseSchema = Some(allFieldsSchema)

        var fieldSet: scala.collection.mutable.Set[StructField] = scala.collection.mutable.Set[StructField]()

        val sqlColumns = SolrSQLSupport.parseColumns(sqlStmt).asScala
        logInfo(s"Parsed SQL fields: ${sqlColumns}")
        if (sqlColumns.isEmpty)
          throw new IllegalArgumentException(s"Cannot determine schema for DataFrame backed by Solr SQL query: ${sqlStmt}; be sure to specify desired columns explicitly instead of relying on the 'SELECT *' syntax.")

        sqlColumns.foreach((kvp) => {
          var lower = kvp._1.toLowerCase
          var col = kvp._2
          if (lower.startsWith("count(")) {
            fieldSet.add(new StructField(kvp._2, LongType))
          } else if (lower.startsWith("avg(") || lower.startsWith("min(") || lower.startsWith("max(") || lower.startsWith("sum(")) {
            fieldSet.add(new StructField(kvp._2, DoubleType))

            // todo: this is hacky but needed to work around SOLR-9372 where the type returned from Solr differs
            // based on the aggregation mode used to execute the SQL statement
            var promoteFields = query.get("promote_to_double")
            if (promoteFields == null) {
              promoteFields = kvp._2
              query.set("promote_to_double", promoteFields)
            } else {
              query.set("promote_to_double", promoteFields+","+kvp._2)
            }
          } else {
            if (allFieldsSchema.fieldNames.contains(kvp._2)) {
              val existing = allFieldsSchema.fields(allFieldsSchema.fieldIndex(kvp._2))
              fieldSet.add(existing)
              logDebug(s"Found existing field ${kvp._2}: ${existing}")
            } else {
              fieldSet.add(new StructField(kvp._2, StringType))
            }
          }
        })
        val sqlSchema = new StructType(fieldSet.toArray.sortBy(f => f.name))
        logInfo(s"Created schema ${sqlSchema} for SQL: ${sqlStmt}")
        sqlSchema
      } else {
        baseSchema = Some(getBaseSchemaFromConfig(collection, solrFields))
        baseSchema.get
      }
    }
  }

  def getSQLDialect(dialectClassName: String): ParserDialect = {
    val clazz = Utils.classForName(dialectClassName)
    clazz.newInstance().asInstanceOf[ParserDialect]
  }

  def toMetricStructField(m: String) : StructField = {
    if (m.toLowerCase.startsWith("count(")) {
      return new StructField(m, LongType)
    } else {
      return new StructField(m, DoubleType)
    }
  }

  def getBaseSchemaFromConfig(collection: String, solrFields: Array[String]) : StructType = {
    SolrRelationUtil.getBaseSchema(
      solrFields.toSet,
      conf.getZkHost.get,
      collection.split(",")(0),
      conf.escapeFieldNames.getOrElse(false),
      conf.flattenMultivalued.getOrElse(true))
  }

  def findStreamingExpressionFields(expr: StreamExpressionParameter, streamOutputFields: ListBuffer[StreamFields]) : Unit = {
    expr match {
      case subExpr : StreamExpression =>
        val funcName = subExpr.getFunctionName
        if (funcName == "search" || funcName == "random" || funcName == "facet") {
          extractSearchFields(subExpr).foreach(fld => streamOutputFields += fld)
        } else {
          subExpr.getParameters.asScala.foreach(subParam => findStreamingExpressionFields(subParam, streamOutputFields))
        }
      case namedParam : StreamExpressionNamedParameter => findStreamingExpressionFields(namedParam.getParameter, streamOutputFields)
      case _ => // no op
    }
  }

  def extractSearchFields(subExpr: StreamExpression) : Option[StreamFields] = {
    logDebug(s"Extracting search fields from ${subExpr.getFunctionName} stream expression ${subExpr} of type ${subExpr.getClass.getName}")
    var collection : Option[String] = Option.empty[String]
    var fields = scala.collection.mutable.ListBuffer.empty[String]
    var metrics = scala.collection.mutable.ListBuffer.empty[String]
    subExpr.getParameters.asScala.foreach(sub => {
      sub match {
        case p : StreamExpressionNamedParameter =>
          if (p.getName == "fl" || p.getName == "buckets" && subExpr.getFunctionName == "facet") {
            p.getParameter match {
              case value : StreamExpressionValue => value.getValue.split(",").foreach(v => fields += v)
              case _ => // ugly!
            }
          }
        case v : StreamExpressionValue => collection = Some(v.getValue)
        case e : StreamExpression =>
          if (subExpr.getFunctionName == "facet") {
            // a metric for a facet stream
            metrics += e.toString
          }
        case _ => // ugly!
      }
    })
    if (collection.isDefined && !fields.isEmpty) {
      val streamFields = new StreamFields(collection.get, fields, metrics)
      logDebug(s"extracted $streamFields for $subExpr")
      return Some(streamFields)
    }
    None
  }

  override def schema: StructType = querySchema

  override def buildScan(): RDD[Row] = buildScan(Array.empty, Array.empty)

  override def buildScan(fields: Array[String], filters: Array[Filter]): RDD[Row] = {

    val rq = solrRDD.requestHandler.getOrElse(DEFAULT_REQUEST_HANDLER)
    if (rq == QT_STREAM || rq == QT_SQL) {
      // ignore any fields / filters when processing a streaming expression
      return SolrRelationUtil.toRows(querySchema, solrRDD.query(query))
    }

    log.info("Fields passed down from scanner: " + fields.mkString(","))
    log.info("Filters passed down from scanner: " + filters.mkString(","))

    // this check is probably unnecessary, but I'm putting it here so that it's clear to other devs
    // that the baseSchema must be defined if we get to this point
    if (!baseSchema.isDefined) {
      throw new IllegalStateException("No base schema defined for collection "+conf.getCollection.get)
    }

    val collectionBaseSchema = baseSchema.get
    if (fields != null && fields.length > 0) {
      // If all the fields in the base schema are here, we probably don't need to explicitly add them to the query
      if (collectionBaseSchema.size == fields.length) {
        // Special case for docValues. Not needed after upgrading to Solr 5.5.0 (unless to maintain back-compat)
        if (conf.docValues.getOrElse(false)) {
          fields.zipWithIndex.foreach({ case (field, i) => fields(i) = field.replaceAll("`", "") })
          query.setFields(fields: _*)
        } else {
          // Reset any existing fields from previous query and set the fields from 'config' option
          query.setFields(solrFields:_*)
        }
      } else {
        fields.zipWithIndex.foreach({ case (field, i) => fields(i) = field.replaceAll("`", "") })
        query.setFields(fields: _*)
      }
    }

    // We use aliasing to retrieve docValues (that are indexed and not stored). This can be removed after upgrading to 5.5
    if (query.getFields != null && query.getFields.length > 0) {
      if (conf.docValues.getOrElse(false)) {
        SolrRelationUtil.setAliases(query.getFields.split(","), query, collectionBaseSchema)
      }
    }

    // Clear all existing filters except the original filters set in the config.
    if (!filters.isEmpty) {
      query.setFilterQueries(queryFilters:_*)
      filters.foreach(filter => SolrRelationUtil.applyFilter(filter, query, collectionBaseSchema))
    } else {
      query.setFilterQueries(queryFilters:_*)
    }

    if (conf.sampleSeed.isDefined) {
      // can't support random sampling & intra-shard splitting
      if (conf.splits.getOrElse(false) || conf.getSplitField.isDefined) {
        throw new IllegalStateException("Cannot do sampling if intra-shard splitting feature is enabled!");
      }

      query.addSort(SolrQuery.SortClause.asc("random_"+conf.sampleSeed.get))
      query.addSort(SolrQuery.SortClause.asc(solrRDD.uniqueKey))
      query.add(ConfigurationConstants.SAMPLE_PCT, conf.samplePct.getOrElse(0.1f).toString)
    }

    logInfo(s"Constructed SolrQuery: ${query}")

    try {
      val querySchema = if (!fields.isEmpty) SolrRelationUtil.deriveQuerySchema(fields, collectionBaseSchema) else schema
      if (!requiresExportHandler(rq) && !conf.useCursorMarks.getOrElse(false)) {
        logDebug(s"Checking the query and sort fields to determine if streaming is possible for ${collection}")
        // Determine whether to use Streaming API (/export handler) if 'use_export_handler' or 'use_cursor_marks' options are not set
        val hasUnsupportedExportTypes : Boolean = SolrRelation.checkQueryFieldsForUnsupportedExportTypes(querySchema)
        val isFDV: Boolean = SolrRelation.checkQueryFieldsForDV(querySchema)
        val sortClauses: List[SortClause] = query.getSorts.asScala.toList
        if (sortClauses.isEmpty) {
          val sortParams = query.getParams(CommonParams.SORT)
          if (sortParams != null && sortParams.nonEmpty) {
            for (sortString <- sortParams) {
              val sortStringParams = sortString.split(" ")
              if (sortStringParams.nonEmpty && sortStringParams.size == 2) {
                sortClauses.::(new SortClause(sortStringParams(0), sortStringParams(1)))
              }
            }
          }
        }

        logDebug(s"Existing sort clauses: ${sortClauses.mkString}")
        val isSDV: Boolean =
          if (sortClauses.nonEmpty)
            SolrRelation.checkSortFieldsForDV(collectionBaseSchema, sortClauses)
          else
            if (isFDV && !hasUnsupportedExportTypes) {
              SolrRelation.addSortField(querySchema, query)
              true
            }
            else
              false

        var requestHandler = rq
        if (requestHandler != QT_EXPORT && isFDV && isSDV && !hasUnsupportedExportTypes) {
          requestHandler = QT_EXPORT
          logInfo("Using the /export handler because docValues are enabled for all fields and no unsupported field types have been requested.")
        } else {
          logDebug(s"Using requestHandler: $rq isFDV? $isFDV and isSDV? $isSDV and hasUnsupportedExportTypes? $hasUnsupportedExportTypes")
        }
        val docs = solrRDD.requestHandler(requestHandler).query(query)
        val rows = SolrRelationUtil.toRows(querySchema, docs)
        rows
      } else {
        val docs = solrRDD.query(query)
        val rows = SolrRelationUtil.toRows(querySchema, docs)
        rows
      }

    } catch {
      case e: Throwable => throw new RuntimeException(e)
    }
  }

  def requiresExportHandler(rq: String): Boolean = {
    return rq == QT_EXPORT || rq == QT_STREAM || rq == QT_SQL
  }

  def toSolrType(dataType: DataType): String = {
    dataType match {
      case bi: BinaryType => "binary"
      case b: BooleanType => "boolean"
      case dt: DateType => "tdate"
      case db: DoubleType => "tdouble"
      case dec: DecimalType => "tdouble"
      case ft: FloatType => "tfloat"
      case i: IntegerType => "tint"
      case l: LongType => "tlong"
      case s: ShortType => "tint"
      case t: TimestampType => "tdate"
      case _ => "string"
    }
  }

  def toAddFieldMap(sf: StructField): Map[String,AnyRef] = {
    val map = scala.collection.mutable.Map[String,AnyRef]()
    map += ("name" -> sf.name)
    map += ("indexed" -> "true")
    map += ("stored" -> "true")
    map += ("docValues" -> "true")
    val dataType = sf.dataType
    dataType match {
      case at: ArrayType =>
        map += ("multiValued" -> "true")
        map += ("type" -> toSolrType(at.elementType))
      case _ => map += ("type" -> toSolrType(dataType))
    }
    map.toMap
  }

  override def insert(df: DataFrame, overwrite: Boolean): Unit = {

    val zkHost = conf.getZkHost.get
    val collectionId = conf.getCollection.get
    val dfSchema = df.schema
    val solrBaseUrl = SolrSupport.getSolrBaseUrl(zkHost)
    val solrFields : Map[String, SolrFieldMeta] =
      SolrQuerySupport.getFieldTypes(Set(), solrBaseUrl, collectionId)

    // build up a list of updates to send to the Solr Schema API
    val fieldsToAddToSolr = new ListBuffer[Update]()
    dfSchema.fields.foreach(f => {
      // TODO: we should load all dynamic field extensions from Solr for making a decision here
      if (!solrFields.contains(f.name) && !f.name.endsWith("_txt") && !f.name.endsWith("_txt_en"))
        fieldsToAddToSolr += new AddField(toAddFieldMap(f).asJava)
    })

    val cloudClient = SolrSupport.getCachedCloudClient(zkHost)
    val solrParams = new ModifiableSolrParams()
    solrParams.add("updateTimeoutSecs","30")
    val addFieldsUpdateRequest = new MultiUpdate(fieldsToAddToSolr.asJava, solrParams)

    if (fieldsToAddToSolr.nonEmpty) {
      logInfo(s"Sending request to Solr schema API to add ${fieldsToAddToSolr.size} fields.")

      val updateResponse : org.apache.solr.client.solrj.response.schema.SchemaResponse.UpdateResponse =
        addFieldsUpdateRequest.process(cloudClient, collectionId)
      if (updateResponse.getStatus >= 400) {
        val errMsg = "Schema update request failed due to: "+updateResponse
        logError(errMsg)
        throw new SolrException(ErrorCode.getErrorCode(updateResponse.getStatus), errMsg)
      }
    }

    if (conf.softAutoCommitSecs.isDefined) {
      logInfo("softAutoCommitSecs? "+conf.softAutoCommitSecs)
      val softAutoCommitSecs = conf.softAutoCommitSecs.get
      val softAutoCommitMs = softAutoCommitSecs * 1000
      var configApi = solrBaseUrl
      if (!configApi.endsWith("/")) {
        configApi += "/"
      }
      configApi += collectionId+"/config"

      val postRequest = new org.apache.http.client.methods.HttpPost(configApi)
      val configJson = "{\"set-property\":{\"updateHandler.autoSoftCommit.maxTime\":\""+softAutoCommitMs+"\"}}";
      postRequest.setEntity(new StringEntity(configJson))
      logInfo("POSTing: "+configJson+" to "+configApi)
      SolrJsonSupport.doJsonRequest(cloudClient.getLbClient.getHttpClient, configApi, postRequest)
    }

    val batchSize: Int = if (conf.batchSize.isDefined) conf.batchSize.get else 500
    val generateUniqKey: Boolean = conf.genUniqKey.getOrElse(false)
    val uniqueKey: String = solrRDD.uniqueKey

    // Convert RDD of rows in to SolrInputDocuments
    val docs = df.rdd.map(row => {
      val schema: StructType = row.schema
      val doc = new SolrInputDocument
      schema.fields.foreach(field => {
        val fname = field.name
        breakable {
          if (fname.equals("_version")) break()
        }
        val fieldIndex = row.fieldIndex(fname)
        val fieldValue : Option[Any] = if (row.isNullAt(fieldIndex)) None else Some(row.get(fieldIndex))
        if (fieldValue.isDefined) {
          val value = fieldValue.get
          value match {
            //TODO: Do we need to check explicitly for ArrayBuffer and WrappedArray
            case v: Iterable[Any] =>
              val it = v.iterator
              while (it.hasNext) doc.addField(fname, it.next())
            case bd: java.math.BigDecimal =>
              doc.setField(fname, bd.doubleValue())
            case _ => doc.setField(fname, value)
          }
        }
      })
      // Generate unique key if the document doesn't have one
      if (generateUniqKey) {
        if (!doc.containsKey(uniqueKey)) {
          doc.setField(uniqueKey, UUID.randomUUID().toString)
        }
      }
      doc
    })
    SolrSupport.indexDocs(solrRDD.zkHost, solrRDD.collection, batchSize, docs, conf.commitWithin)
  }



  private def buildQuery: SolrQuery = {
    val query = SolrQuerySupport.toQuery(conf.getQuery.getOrElse("*:*"))

    if (conf.getStreamingExpr.isDefined) {
      query.setRequestHandler(QT_STREAM)
      query.set(SOLR_STREAMING_EXPR, conf.getStreamingExpr.get.replaceAll("\\s+", " "))
    }

    if (conf.getSqlStmt.isDefined) {
      query.setRequestHandler(QT_SQL)
      query.set(SOLR_SQL_STMT, conf.getSqlStmt.get.replaceAll("\\s+", " "))
    }

    if (solrFields.nonEmpty) {
      query.setFields(solrFields:_*)
    }

    query.setRows(scala.Int.box(conf.getRows.getOrElse(DEFAULT_PAGE_SIZE)))
    query.add(conf.getArbitrarySolrParams)
    query.set("collection", collection)
    query
  }

  private def checkRequiredParams(): Unit = {
    require(conf.getZkHost.isDefined, "Param '" + SOLR_ZK_HOST_PARAM + "' is required")
    require(conf.getCollection.isDefined, "Param '" + SOLR_COLLECTION_PARAM + "' is required")
  }
}

object SolrRelation extends Logging {
  def checkUnknownParams(keySet: Set[String]): Set[String] = {
    var knownParams = Set.empty[String]
    var unknownParams = Set.empty[String]

    // Use reflection to get all the members of [ConfigurationConstants] except 'CONFIG_PREFIX'
    val rm = scala.reflect.runtime.currentMirror
    val accessors = rm.classSymbol(ConfigurationConstants.getClass).toType.members.collect {
      case m: MethodSymbol if m.isGetter && m.isPublic => m
    }
    val instanceMirror = rm.reflect(ConfigurationConstants)

    for(acc <- accessors) {
      knownParams += instanceMirror.reflectMethod(acc).apply().toString
    }

    // Check for any unknown options
    keySet.foreach(key => {
      if (!knownParams.contains(key)) {
        unknownParams += key
      }
    })
    unknownParams
  }

  def checkQueryFieldsForDV(querySchema: StructType) : Boolean = {
    // Check if all the fields in the querySchema have docValues enabled
    for (structField <- querySchema.fields) {
      val metadata = structField.metadata
      if (!metadata.contains("docValues"))
        return false
      if (metadata.contains("docValues") && !metadata.getBoolean("docValues"))
        return false
    }
    true
  }

  def checkSortFieldsForDV(baseSchema: StructType, sortClauses: List[SortClause]): Boolean = {

    if (sortClauses.nonEmpty) {
      // Check if the sorted field (if exists) has docValue enabled
      for (sortClause: SortClause <- sortClauses) {
        val sortField = sortClause.getItem
        if (baseSchema.fieldNames.contains(sortField)) {
          val sortFieldMetadata = baseSchema(sortField).metadata
          if (!sortFieldMetadata.contains("docValues"))
            return false
          if (sortFieldMetadata.contains("docValues") && !sortFieldMetadata.getBoolean("docValues"))
            return false
        } else {
          log.warn("The sort field '" + sortField + "' does not exist in the base schema")
          return false
        }
      }
      true
    } else {
      false
   }
  }

  def addSortField(querySchema: StructType, query: SolrQuery): Unit = {
    query.addSort(querySchema.fields(0).name, SolrQuery.ORDER.asc)
    log.info("Added sort field '" + query.getSortField + "' to the query")
  }

  // TODO: remove this check when https://issues.apache.org/jira/browse/SOLR-9187 is fixed
  def checkQueryFieldsForUnsupportedExportTypes(querySchema: StructType) : Boolean = {
    for (structField <- querySchema.fields) {
      if (structField.dataType == BooleanType || structField.dataType == TimestampType)
        return true
    }
    false
  }
}

case class StreamFields(collection:String,fields:ListBuffer[String],metrics:ListBuffer[String])

