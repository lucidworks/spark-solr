package com.lucidworks.spark

import java.io.IOException
import java.util.UUID
import java.util.regex.Pattern

import com.lucidworks.spark.query.sql.SolrSQLSupport
import com.lucidworks.spark.rdd.{SolrRDD, StreamingSolrRDD}
import com.lucidworks.spark.util.ConfigurationConstants._
import com.lucidworks.spark.util.QueryConstants._
import com.lucidworks.spark.util._
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.solr.client.solrj.SolrQuery.SortClause
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.io.stream.expr._
import org.apache.solr.client.solrj.request.schema.SchemaRequest.{AddField, MultiUpdate, Update}
import org.apache.solr.client.solrj.{SolrQuery, SolrServerException}
import org.apache.solr.common.SolrException.ErrorCode
import org.apache.solr.common.params.{CommonParams, ModifiableSolrParams}
import org.apache.solr.common.{SolrException, SolrInputDocument}
import org.apache.spark.SparkFirehoseListener
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerEvent}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.breakOut
import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._
import scala.util.control.Breaks._

class SolrRelation(
    val parameters: Map[String, String],
    val dataFrame: Option[DataFrame],
    @transient val sparkSession: SparkSession)(
  implicit
    val conf: SolrConf = new SolrConf(parameters))
  extends BaseRelation
  with Serializable
  with TableScan
  with PrunedFilteredScan
  with InsertableRelation
  with LazyLogging {

  override val sqlContext: SQLContext = sparkSession.sqlContext

  sparkSession.sparkContext.addSparkListener(new SparkFirehoseListener() {
    override def onEvent(event: SparkListenerEvent): Unit = event match {
      case e: SparkListenerApplicationEnd =>
        logger.debug(s"Invalidating cloud client and http client caches for event ${e}")
        CacheCloudSolrClient.cache.invalidateAll()
        CacheHttpSolrClient.cache.invalidateAll()
      case _ =>
    }
  })

  def this(parameters: Map[String, String], sparkSession: SparkSession) {
    this(parameters, None, sparkSession)
  }

  checkRequiredParams()

  lazy val solrVersion : String = SolrSupport.getSolrVersion(conf.getZkHost.get)
  lazy val initialQuery: SolrQuery = SolrRelation.buildQuery(conf)
  // we don't need the baseSchema for streaming expressions, so we wrap it in an optional
  var baseSchema : Option[StructType] = None

  lazy val collection = initialQuery.get("collection")

  var optimizedPlan = false

  // loaded lazily to avoid going to Solr until it's necessary, mainly to assist with mocking this class for unit tests
  lazy val uniqueKey: String = SolrQuerySupport.getUniqueKey(conf.getZkHost.get, collection.split(",")(0))

  // to handle field aliases and function queries, we need to parse the fields option
  // from the user into QueryField objects
  lazy val userRequestedFields : Option[Array[QueryField]] = if (!conf.getFields.isEmpty) {
    val solrFields = conf.getFields
    logger.debug(s"Building userRequestedFields from solrFields: ${solrFields.mkString(", ")}")
    val qf = SolrRelationUtil.parseQueryFields(solrFields)
    if (!qf.filter(_.funcReturnType.isDefined).isEmpty) {
      // must drop the additional type info from any func queries in the field list ...
      initialQuery.setFields(qf.map(_.fl):_*)
    }
    logger.info(s"userRequestedFields: ${qf.mkString(", ")}")
    Some(qf)
  } else {
    None
  }

  // Preserve the initial filters if any present in arbitrary config
  lazy val queryFilters: Array[String] = if (initialQuery.getFilterQueries != null) initialQuery.getFilterQueries else Array.empty[String]

  lazy val dynamicSuffixes: Set[String] = SolrQuerySupport.getFieldTypes(
    Set.empty,
    SolrSupport.getSolrBaseUrl(conf.getZkHost.get),
    SolrSupport.getCachedCloudClient(conf.getZkHost.get),
    collection.split(",")(0),
    skipDynamicExtensions = false
  ).keySet
      .filter(f => f.startsWith("*_") || f.endsWith("_*"))
      .map(f => if (f.startsWith("*_")) f.substring(1) else f.substring(0, f.length-1))

  lazy val querySchema: StructType = {
    if (dataFrame.isDefined) {
      dataFrame.get.schema
    } else {
      if (userRequestedFields.isDefined) {
        // filter out any function query invocations from the base schema list
        val baseSchemaFields = userRequestedFields.get.filter(!_.funcReturnType.isDefined).map(_.name) ++ Array(uniqueKey)
        if (conf.schema.isEmpty) {
          baseSchema = Some(getBaseSchemaFromConfig(collection, baseSchemaFields))
        } else {
          val fieldSet = SolrRelation.parseSchemaExprSchemaToStructFields(conf.schema.get)
          if (fieldSet.isEmpty) {
            throw new IllegalStateException(s"Failed to extract schema fields from schema config ${schema}")
          }
          baseSchema = Some(new StructType(fieldSet.toArray.sortBy(f => f.name)))
        }
        SolrRelationUtil.deriveQuerySchema(userRequestedFields.get, baseSchema.get)
      } else if (initialQuery.getRequestHandler == QT_STREAM) {
        var fieldSet: scala.collection.mutable.Set[StructField] = scala.collection.mutable.Set[StructField]()
        var streamingExpr = StreamExpressionParser.parse(initialQuery.get(SOLR_STREAMING_EXPR))
        if (conf.getStreamingExpressionSchema.isDefined) {
          // the user is telling us the schema of the expression
          fieldSet.++=(SolrRelation.parseSchemaExprSchemaToStructFields(conf.getStreamingExpressionSchema.get))
        } else {
          // we have to figure out the schema of the streaming expression
          var streamOutputFields = new ListBuffer[StreamFields]
          findStreamingExpressionFields(streamingExpr, streamOutputFields, 0)
          logger.info(s"Found ${streamOutputFields.size} stream output fields: ${streamOutputFields}")
          for (sf <- streamOutputFields) {
            val streamSchema: StructType =
              SolrRelationUtil.getBaseSchema(
                sf.fields.map(f => f.name).toSet,
                conf.getZkHost.get,
                sf.collection,
                conf.getMaxShardsForSchemaSampling,
                conf.escapeFieldNames.getOrElse(false),
                conf.flattenMultivalued,
                conf.skipNonDocValueFields.getOrElse(false),
                dynamicSuffixes)
            logger.debug(s"Got stream schema: ${streamSchema} for ${sf}")
            sf.fields.foreach(fld => {
              val fieldName = fld.alias.getOrElse(fld.name)
              if (fld.hasReplace) {
                // completely ignore the Solr type ... force to the replace type
                fieldSet.add(new StructField(fieldName, fld.dataType))
              } else {
                if (streamSchema.fieldNames.contains(fieldName)) {
                  fieldSet.add(streamSchema.apply(fieldName))
                } else {
                  // ugh ... this field coming out of the streaming expression isn't known to solr, so likely a select
                  // expression here with some renaming going on ... just assume string and keep going
                  fieldSet.add(new StructField(fieldName, fld.dataType))
                }
              }
            })

            sf.metrics.foreach(m => {
              val metricName = m.alias.getOrElse(m.name)
              // crazy hack here but Solr might return a long which we registered as a double in the schema
              if (!m.name.startsWith("count(")) {
                val metadata = new MetadataBuilder().putBoolean(Constants.PROMOTE_TO_DOUBLE, value = true).build()
                logger.info(s"Set ${Constants.PROMOTE_TO_DOUBLE} for metric $metricName")
                fieldSet.add(StructField(metricName, DoubleType, metadata = metadata))
              } else {
                fieldSet.add(StructField(metricName, m.dataType))
              }
            })
          }
        }

        if (fieldSet.isEmpty) {
          throw new IllegalStateException("Failed to extract schema fields for streaming expression: " + streamingExpr)
        }
        // Remove presence of any duplicates in the set
        val fieldList: List[StructField] = fieldSet.groupBy(_.name).map(_._2.head)(breakOut)
        var exprSchema = new StructType(fieldList.toArray.sortBy(f => f.name))
        logger.info(s"Created combined schema with ${exprSchema.fieldNames.size} fields for streaming expression: ${exprSchema}: ${exprSchema.fields}")
        exprSchema
      } else if (initialQuery.getRequestHandler == QT_SQL) {
        val sqlStmt = initialQuery.get(SOLR_SQL_STMT)
        val fieldSet: scala.collection.mutable.Set[StructField] = scala.collection.mutable.Set[StructField]()
        logger.info(s"Determining schema for Solr SQL: ${sqlStmt}")
        if (conf.getSolrSQLSchema.isDefined) {
          val solrSQLSchema = conf.getSolrSQLSchema.get
          logger.info(s"Using '$solrSQLSchema' from config property '$SOLR_SQL_SCHEMA' to compute the SQL schema")
          solrSQLSchema.split(',').foreach(f => {
            val pair : Array[String] = f.split(':')
            fieldSet.add(new StructField(pair.apply(0), DataType.fromJson("\""+pair.apply(1)+"\"")))
          })
        } else {
          val allFieldsSchema : StructType = getBaseSchemaFromConfig(collection, Array.empty)
          baseSchema = Some(allFieldsSchema)
          val sqlColumns = SolrSQLSupport.parseColumns(sqlStmt).asScala
          logger.info(s"Parsed SQL fields: ${sqlColumns}")
          if (sqlColumns.isEmpty)
            throw new IllegalArgumentException(s"Cannot determine schema for DataFrame backed by Solr SQL query: ${sqlStmt}; be sure to specify desired columns explicitly instead of relying on the 'SELECT *' syntax.")

          sqlColumns.foreach((kvp) => {
            var lower = kvp._1.toLowerCase
            var col = kvp._2
            if (lower.startsWith("count(")) {
              fieldSet.add(new StructField(kvp._2, LongType))
            } else if (lower.startsWith("avg(") || lower.startsWith("min(") || lower.startsWith("max(") || lower.startsWith("sum(")) {
              val column = kvp._2
              // todo: this is hacky but needed to work around SOLR-9372 where the type returned from Solr differs
              // based on the aggregation mode used to execute the SQL statement
              val metadata = new MetadataBuilder().putBoolean(Constants.PROMOTE_TO_DOUBLE, value = true).build()
              logger.info(s"Set ${Constants.PROMOTE_TO_DOUBLE} for col: $column")
              fieldSet.add(new StructField(column, DoubleType, metadata = metadata))
            }  else if (col.equals("score")) {
              fieldSet.add(new StructField(col, DoubleType))
            } else {
              if (allFieldsSchema.fieldNames.contains(kvp._2)) {
                val existing = allFieldsSchema.fields(allFieldsSchema.fieldIndex(kvp._2))
                fieldSet.add(existing)
                logger.debug(s"Found existing field ${kvp._2}: ${existing}")
              } else {
                fieldSet.add(new StructField(kvp._2, StringType))
              }
            }
          })
        }
        if (fieldSet.isEmpty) {
          throw new IllegalStateException("Failed to extract schema fields for streaming expression: " + sqlStmt)
        }
        val sqlSchema = new StructType(fieldSet.toArray.sortBy(f => f.name))
        logger.info(s"Created schema ${sqlSchema} for SQL: ${sqlStmt}")
        sqlSchema
      } else {
        // don't return the _version_ field unless specifically asked for by the user
        val solrFields = conf.getFields
        baseSchema = Some(getBaseSchemaFromConfig(collection, if (solrFields.isEmpty) solrFields else solrFields ++ Array(uniqueKey)))
        if (solrFields.contains("_version_")) {
          // user specifically requested _version_, so keep it
          var tmp = applyExcludeFieldsToSchema(baseSchema.get)
          StructType(tmp.fields.sortBy(f => f.name))
        } else {
          var tmp = baseSchema.get
          if (tmp.fieldNames.contains("_version_")) {
            tmp = StructType(tmp.filter(p => p.name != "_version_"))
          }
          tmp = applyExcludeFieldsToSchema(tmp)
          StructType(tmp.fields.sortBy(f => f.name))
        }
      }
    }
  }

  private def applyExcludeFieldsToSchema(querySchema: StructType) : StructType = {
    if (!conf.getExcludeFields.isDefined)
      return querySchema

    val excludeFields = conf.getExcludeFields.get.trim()
    if (excludeFields.isEmpty)
      return querySchema

    logger.debug(s"Found field name exclusion patterns: ${excludeFields}")
    val excludePatterns = excludeFields.split(",").map(pat => {
      var namePattern = pat.trim()
      val len = namePattern.length()
      // since leading or trailing wildcards are so common, we'll convert those to proper regex for the user
      // otherwise, newbies will get the ol' "Dangling meta character '*' near index 0" error
      if (namePattern.startsWith("*") && namePattern.indexOf("*", 1) == -1) {
        namePattern = "^.*"+namePattern.substring(1)+"$"
      } else if (namePattern.endsWith("*") && namePattern.substring(0,len-1).indexOf("*") == -1) {
        namePattern = "^"+namePattern.substring(0,len-1)+".*$"
      }
      Pattern.compile(namePattern, Pattern.CASE_INSENSITIVE)
    })

    return StructType(querySchema.filter(p => {
      var isExcluded = false
      for (regex <- excludePatterns) {
        if (regex.matcher(p.name).matches()) {
          isExcluded = true
          logger.debug(s"Excluding ${p.name} from the query schema because it matches exclude pattern: ${regex.pattern()}")
        }
      }
      !isExcluded
    }))
  }

  def getBaseSchemaFromConfig(collection: String, solrFields: Array[String]) : StructType = {
    SolrRelationUtil.getBaseSchema(
      solrFields.toSet,
      conf.getZkHost.get,
      collection.split(",")(0),
      conf.getMaxShardsForSchemaSampling,
      conf.escapeFieldNames.getOrElse(false),
      conf.flattenMultivalued,
      conf.skipNonDocValueFields.getOrElse(false),
      dynamicSuffixes)
  }

  def findStreamingExpressionFields(expr: StreamExpressionParameter, streamOutputFields: ListBuffer[StreamFields], depth: Int) : Unit = {

    logger.info(s"findStreamingExpressionFields(depth=$depth): expr = $expr")

    var currDepth = depth

    expr match {
      case subExpr : StreamExpression =>
        val funcName = subExpr.getFunctionName
        if (funcName == "search" || funcName == "random" || funcName == "facet") {
          extractSearchFields(subExpr).foreach(fld => streamOutputFields += fld)
        } else if (funcName == "select") {
          // the top-level select is the schema we care about so we don't need to scan expressions below the
          // top-level select for fields in the schema, hence the special casing here
          if (depth == 0) {

            currDepth += 1

            logger.info(s"Extracting search fields from top-level select")
            var exprCollection : Option[String] = Option.empty[String]
            var fields = scala.collection.mutable.ListBuffer.empty[StreamField]
            var metrics = scala.collection.mutable.ListBuffer.empty[StreamField]
            var replaceFields = scala.collection.mutable.Map[String, DataType]()
            subExpr.getParameters.asScala.foreach(sub => {
              sub match {
                case v : StreamExpressionValue => {
                  val selectFieldName = v.getValue
                  val asAt = selectFieldName.indexOf(" as ")
                  if (asAt != -1) {
                    val key = selectFieldName.substring(0, asAt).trim()
                    val alias = selectFieldName.substring(asAt + 4).trim()
                    if (key.indexOf("(") != -1 && key.endsWith(")")) {
                      metrics += getStreamMetricField(key, Some(alias))
                    } else {
                      fields += new StreamField(key, StringType, Some(alias))
                    }
                  } else {
                    if (selectFieldName.indexOf("(") != -1 && selectFieldName.endsWith(")")) {
                      metrics += getStreamMetricField(selectFieldName, None)
                    } else {
                      fields += new StreamField(selectFieldName, StringType, None)
                    }
                  }
                }
                case e : StreamExpression => {
                  val exprFuncName = e.getFunctionName
                  if (exprFuncName == "replace") {
                    // we have to handle type-conversion from the Solr type to the replace type
                    logger.debug(s"Found a replace expression in select: $e")
                    val params = e.getParameters.asScala
                    val tmp = params.apply(0)
                    tmp match {
                      case v: StreamExpressionValue => replaceFields += (v.getValue -> StringType)
                    }
                  } else if (exprFuncName == "search" || exprFuncName == "random" || exprFuncName == "facet") {
                    val maybeSF = extractSearchFields(e)
                    if (maybeSF.isDefined) {
                      exprCollection = Some(maybeSF.get.collection)
                      logger.debug(s"Found exprCollection name ")
                    }
                  }
                }
                case _ => // ugly, but let's not fail b/c of unhandled stuff as the expression stuff is changing rapidly
              }
            })

            // for any fields that have a replace function applied, we need to override the
            fields = fields.map(f => {
              if (replaceFields.contains(f.name)) new StreamField(f.name, replaceFields.getOrElse(f.name, StringType), None, true) else f
            })

            val streamFields = new StreamFields(exprCollection.getOrElse(collection), fields, metrics)
            logger.info(s"Extracted $streamFields for $subExpr")
            streamOutputFields += streamFields
          } else {
            // not a top-level select, so just push it down to find fields
            extractSearchFields(subExpr).foreach(fld => streamOutputFields += fld)
          }
        } else {
          subExpr.getParameters.asScala.foreach(subParam => findStreamingExpressionFields(subParam, streamOutputFields, currDepth))
        }
      case namedParam : StreamExpressionNamedParameter => findStreamingExpressionFields(namedParam.getParameter, streamOutputFields, currDepth)
      case _ => // no op
    }
  }

  private def getStreamMetricField(key: String, alias: Option[String]) : StreamField = {
    return if (key.startsWith("count(")) {
      new StreamField(key, LongType, alias)
    } else {
      // just treat all other metrics as double type
      new StreamField(key, DoubleType, alias)
    }
  }

  def extractSearchFields(subExpr: StreamExpression) : Option[StreamFields] = {
    logger.debug(s"Extracting search fields from ${subExpr.getFunctionName} stream expression ${subExpr} of type ${subExpr.getClass.getName}")
    var collection : Option[String] = Option.empty[String]
    var fields = scala.collection.mutable.ListBuffer.empty[StreamField]
    var metrics = scala.collection.mutable.ListBuffer.empty[StreamField]
    subExpr.getParameters.asScala.foreach(sub => {
      logger.debug(s"Next expression param is $sub of type ${sub.getClass.getName}")
      sub match {
        case p : StreamExpressionNamedParameter =>
          if (p.getName == "fl" || p.getName == "buckets" && subExpr.getFunctionName == "facet") {
            p.getParameter match {
              case value : StreamExpressionValue => value.getValue.split(",").foreach(v => fields += new StreamField(v, StringType, None))
              case _ => // ugly!
            }
          }
        case v : StreamExpressionValue => collection = Some(v.getValue)
        case e : StreamExpression => if (subExpr.getFunctionName == "facet") {
            // a metric for a facet stream
            metrics += getStreamMetricField(e.toString, None)
          }
        case _ => // ugly!
      }
    })
    if (collection.isDefined && !fields.isEmpty) {
      val streamFields = new StreamFields(collection.get, fields, metrics)
      logger.info(s"Extracted $streamFields for $subExpr")
      return Some(streamFields)
    }
    None
  }

  override def schema: StructType = querySchema

  override def buildScan(): RDD[Row] = buildScan(Array.empty, Array.empty)

  override def buildScan(fields: Array[String], filters: Array[Filter]): RDD[Row] = {
    logger.debug(s"buildScan: uniqueKey: ${uniqueKey}, querySchema: ${querySchema}, baseSchema: ${baseSchema}, push-down fields: [${fields.mkString(",")}], filters: [${filters.mkString(",")}]")
    val query = initialQuery.getCopy
    var qt = query.getRequestHandler

    // ignore any filters when processing a streaming expression
    if (qt == QT_STREAM || qt == QT_SQL) {
      // we still have to adapt the querySchema to the requested projected fields if avaiable
      val projectedQuerySchema = if (fields != null && fields.length > 0) StructType(fields.map(f => querySchema.apply(f))) else querySchema
      return SolrRelationUtil.toRows(projectedQuerySchema, new StreamingSolrRDD(
        conf.getZkHost.get,
        collection,
        sqlContext.sparkContext,
        Some(qt),
        uKey = Some(uniqueKey)
      ).query(query))
    }

    // this check is probably unnecessary, but I'm putting it here so that it's clear to other devs
    // that the baseSchema must be defined if we get to this point
    if (baseSchema.isEmpty) {
      throw new IllegalStateException("No base schema defined for collection "+collection)
    }

    val collectionBaseSchema = baseSchema.get
    var scanSchema = schema

    // we need to know this so that we don't try to use the /export handler if the user requested a function query or
    // is using field aliases
    var hasFuncQueryOrFieldAlias = userRequestedFields.isDefined &&
      userRequestedFields.get.filter(qf => qf.alias.isDefined || qf.funcReturnType.isDefined).size > 0

    if (fields != null && fields.length > 0) {
      fields.zipWithIndex.foreach({ case (field, i) => fields(i) = field.replaceAll("`", "") })

      // userRequestedFields is only defined if the user set the "fields" option explicitly
      // create the schema for the push-down scan results using QueryFields to account for aliases and function queries
      if (userRequestedFields.isDefined) {
        val scanQueryFields = ListBuffer[QueryField]()
        fields.foreach(f => {
          userRequestedFields.get.foreach(qf => {
            // if the field refers to an alias, then we need to add the alias:field to the query
            if (f == qf.name) {
              scanQueryFields += qf
            } else if (qf.alias.isDefined && f == qf.alias.get) {
              scanQueryFields += qf
            }
          })
        })
        val scanFields = scanQueryFields.toArray
        query.setFields(scanFields.map(_.fl):_*)
        scanSchema = SolrRelationUtil.deriveQuerySchema(scanFields, collectionBaseSchema)
      } else {
        query.setFields(fields:_*)
        scanSchema = SolrRelationUtil.deriveQuerySchema(fields.map(QueryField(_)), collectionBaseSchema)
      }
      logger.debug(s"Set query fl=${query.getFields} from table scan fields=${fields.mkString(", ")}")
    }

    // Clear all existing filters except the original filters set in the config.
    if (!filters.isEmpty) {
      query.setFilterQueries(queryFilters:_*)
      filters.foreach(filter => SolrRelationUtil.applyFilter(filter, query, querySchema))
    } else {
      query.setFilterQueries(queryFilters:_*)
    }

    // spark 2.1 started sending in an unnecessary NOT NULL fq, remove those if present
    val fqs = removeSuperfluousNotNullFilterQuery(query)
    if (!fqs.isEmpty) query.setFilterQueries(fqs.toSeq:_*)

    if (conf.sampleSeed.isDefined) {
      // can't support random sampling & intra-shard splitting
      if (conf.splits.getOrElse(false) || conf.getSplitField.isDefined) {
        throw new IllegalStateException("Cannot do sampling if intra-shard splitting feature is enabled!");
      }

      query.addSort(SolrQuery.SortClause.asc("random_" + conf.sampleSeed.get))
      query.addSort(SolrQuery.SortClause.asc(uniqueKey))
      query.add(ConfigurationConstants.SAMPLE_PCT, conf.samplePct.getOrElse(0.1f).toString)
    }

    if (scanSchema.fields.length == 0) {
      throw new IllegalStateException(s"No fields defined in query schema for query: ${query}. This is likely an issue with the Solr collection ${collection}, does it have data?")
    }

    // prevent users from trying to export a func query as /export doesn't do that
    if (hasFuncQueryOrFieldAlias && qt == "/export") {
      throw new IllegalStateException(s"Can't request function queries using the /export handler!")
    }

    try {
      // Determine the request handler to use if not explicitly set by the user
      if (!hasFuncQueryOrFieldAlias &&
          conf.requestHandler.isEmpty &&
          !requiresStreamingRDD(qt) &&
          !conf.useCursorMarks.getOrElse(false) &&
          conf.maxRows.isEmpty) {
        logger.debug(s"Checking the query and sort fields to determine if streaming is possible for ${collection}")
        // Determine whether to use Streaming API (/export handler) if 'use_export_handler' or 'use_cursor_marks' options are not set
        val isFDV: Boolean = if (fields.isEmpty && query.getFields == null) true else SolrRelation.checkQueryFieldsForDV(scanSchema)
        var sortClauses: ListBuffer[SortClause] = ListBuffer.empty
        if (!query.getSorts.isEmpty) {
          for (sort: SortClause <- query.getSorts.asScala) {
            sortClauses += sort
          }
        } else {
          val sortParams = query.getParams(CommonParams.SORT)
          if (sortParams != null && sortParams.nonEmpty) {
            for (sortString <- sortParams) {
              sortClauses = sortClauses ++ SolrRelation.parseSortParamFromString(sortString)
            }
          }
        }

        logger.debug(s"Existing sort clauses: ${sortClauses.mkString}")

        val isSDV: Boolean =
          if (sortClauses.nonEmpty)
            SolrRelation.checkSortFieldsForDV(collectionBaseSchema, sortClauses.toList)
          else
            if (isFDV) {
              SolrRelation.addSortField(querySchema, scanSchema, query, uniqueKey)
              logger.debug(s"Added sort field '${query.getSortField}' to the query")
              true
            }
            else
              false

        if (isFDV && isSDV) {
          qt = QT_EXPORT
          query.setRequestHandler(qt)
          logger.debug("Using the /export handler because docValues are enabled for all fields and no unsupported field types have been requested.")
        } else {
          logger.debug(s"Using requestHandler: $qt isFDV? $isFDV and isSDV? $isSDV")
        }
      }

      // For DataFrame operations like count(), no fields are passed down but the export handler only works when fields are present
      if (query.getFields == null)
        query.setFields(uniqueKey)
      if (qt == QT_EXPORT) {
        if (query.getSorts.isEmpty && (query.getParams(CommonParams.SORT) == null || query.getParams(CommonParams.SORT).isEmpty))
          query.setSort(uniqueKey, SolrQuery.ORDER.asc)
      }
      logger.info(s"Sending ${query} to SolrRDD using ${qt} with maxRows: ${conf.maxRows}")
      // Register an accumulator for counting documents
      val acc: SparkSolrAccumulator = new SparkSolrAccumulator
      val accName = if (conf.getAccumulatorName.isDefined) conf.getAccumulatorName.get else "Records Read"
      sparkSession.sparkContext.register(acc, accName)
      SparkSolrAccumulatorContext.add(accName, acc.id)

      val modifiedCollection =
        if (collection.indexOf(",") != -1) {
          new TimePartitioningQuery(conf, query, Some(collection.split(",").toList))
            .getPartitionsForQuery()
            .mkString(",")
        } else {
          collection
        }
      // Construct the SolrRDD based on the request handler
      val solrRDD: SolrRDD[_] = SolrRDD.apply(
        conf.getZkHost.get,
        modifiedCollection,
        sqlContext.sparkContext,
        Some(qt),
        splitsPerShard = conf.getSplitsPerShard,
        splitField = getSplitField(conf),
        uKey = Some(uniqueKey),
        maxRows = conf.maxRows,
        accumulator = Some(acc))
      val docs = solrRDD.query(query)
      logger.debug(s"Converting SolrRDD of type ${docs.getClass.getName} to rows matching schema: ${scanSchema}")
      val rows = SolrRelationUtil.toRows(scanSchema, docs)
      rows
    } catch {
      case e: Throwable => throw new RuntimeException(e)
    }
  }

  // this method scans over the filter queries looking for superfluous is not null fqs, such as:
  // gender:[* TO *] and gender:F being in the query, we don't need the not null check
  def removeSuperfluousNotNullFilterQuery(solrQuery: SolrQuery) : Set[String] = {
    val fqs = solrQuery.getFilterQueries
    if (fqs == null || fqs.isEmpty)
      return Set.empty[String]

    val fqList = fqs.map(filterQueryAsTuple(_)) // get a key/value tuple for the fq
    val fqSet = fqList.map(t => {
      var fq: Option[String] = Some(t._1+":"+t._2)
      if (t._2 == "[* TO *]") {
        fqList.foreach(u => {
          if (t._1 == u._1 && t._2 != u._2) {
            // same key different value, don't need this null check
            fq = None
          }
        })
      }
      fq
    }).filter(_.isDefined)
    return fqSet.map(_.get).toSet
  }

  def filterQueryAsTuple(fq: String) : (String,String) = {
    val firstColonAt = fq.indexOf(':')
    (fq.substring(0, firstColonAt), fq.substring(firstColonAt + 1))
  }

  def getSplitField(conf: SolrConf): Option[String] = {
    if (conf.getSplitField.isDefined) conf.getSplitField
    else if (conf.splits.getOrElse(false)) Some(DEFAULT_SPLIT_FIELD)
    else None
  }

  def requiresStreamingRDD(rq: String): Boolean = {
    return rq == QT_EXPORT || rq == QT_STREAM || rq == QT_SQL
  }

  override def insert(df: DataFrame, overwrite: Boolean): Unit = {

    val zkHost = conf.getZkHost.get
    val collectionId = conf.getCollection.get
    val dfSchema = df.schema
    val solrBaseUrl = SolrSupport.getSolrBaseUrl(zkHost)
    val cloudClient = SolrSupport.getCachedCloudClient(zkHost)

    // build up a list of updates to send to the Solr Schema API
    val fieldsToAddToSolr = SolrRelation.getFieldsToAdd(dfSchema, conf, solrVersion, dynamicSuffixes)

    if (fieldsToAddToSolr.nonEmpty) {
      SolrRelation.addFieldsForInsert(fieldsToAddToSolr, collectionId, cloudClient)
    }

    if (conf.softAutoCommitSecs.isDefined) {
      logger.info("softAutoCommitSecs? "+conf.softAutoCommitSecs)
      val softAutoCommitSecs = conf.softAutoCommitSecs.get
      val softAutoCommitMs = softAutoCommitSecs * 1000
      SolrRelationUtil.setAutoSoftCommit(zkHost, collectionId, softAutoCommitMs)
    }

    val batchSize: Int = if (conf.batchSize.isDefined) conf.batchSize.get else 1000

    // Convert RDD of rows in to SolrInputDocuments
    val uk = SolrQuerySupport.getUniqueKey(zkHost, collectionId)
    logger.info(s"Converting rows to input docs with zkHost=${zkHost}, collectionId=${collectionId}, uniqueKey=${uk}")
    val docs = df.rdd.map(row => SolrRelation.convertRowToSolrInputDocument(row, conf, uk))

    val acc: SparkSolrAccumulator = new SparkSolrAccumulator
    val accName = if (conf.getAccumulatorName.isDefined) conf.getAccumulatorName.get else "Records Written"
    sparkSession.sparkContext.register(acc, accName)
    SparkSolrAccumulatorContext.add(accName, acc.id)
    SolrSupport.indexDocs(zkHost, collectionId, batchSize, docs, conf.commitWithin, Some(acc))
    logger.info("Written {} documents to Solr collection {}", acc.value, collectionId)
  }

  private def checkRequiredParams(): Unit = {
    require(conf.getZkHost.isDefined, "Param '" + SOLR_ZK_HOST_PARAM + "' is required")
  }
}

object SolrRelation extends LazyLogging {

  val solrCollectionInSqlPattern = Pattern.compile("\\sfrom\\s([\\w\\-\\.]+)\\s?", Pattern.CASE_INSENSITIVE)

  def parseSchemaExprSchemaToStructFields(streamingExprSchema: String): Set[StructField] = {
    var fieldBuffer = ListBuffer.empty[StructField]
    SolrRelationUtil.parseCommaSeparatedValuesToList(streamingExprSchema).foreach(f => {
      val colonIndex = f.indexOf(":")
      if (colonIndex != -1) { 
        val fieldName = f.substring(0, colonIndex)
        var fieldValue = StringEscapeUtils.unescapeJson(f.substring(colonIndex+1))
        if (fieldValue.startsWith("\"{") && fieldValue.endsWith("}\"")) {
          fieldValue = fieldValue.substring(1, fieldValue.length-1)
        } else if (!fieldValue.startsWith("\"") || !fieldValue.endsWith("\"")){
          fieldValue = s""""${fieldValue}""""
        }
        val dataType = DataType.fromJson(fieldValue)
        dataType match {
          case _ : ArrayType =>
            val metadataBuilder = new MetadataBuilder
            metadataBuilder.putBoolean("multiValued",  true)
            fieldBuffer.+=(StructField(fieldName, dataType, metadata=metadataBuilder.build()))
          case _ => fieldBuffer.+=(StructField(fieldName, DataType.fromJson(fieldValue)))
        }
      }
    })
    fieldBuffer.toSet
  }

  def resolveCollection(conf: SolrConf, initialQuery: SolrQuery): Unit = {
    var collection = conf.getCollection.getOrElse({
      var coll = Option.empty[String]
      if (conf.getSqlStmt.isDefined) {
        val collectionFromSql = SolrRelation.findSolrCollectionNameInSql(conf.getSqlStmt.get)
        if (collectionFromSql.isDefined) {
          coll = collectionFromSql
        }
      }
      if (coll.isDefined) {
        logger.info(s"resolved collection option from sql to be ${coll.get}")
        coll.get
      } else {
        throw new IllegalArgumentException("collection option is required!")
      }
    })
    if (conf.partitionBy.isDefined && conf.partitionBy.get == "time" && conf.getStreamingExpr.isEmpty) {
      if (collection.indexOf(",") != -1) {
        new TimePartitioningQuery(conf, initialQuery, Some(collection.split(",").toList))
          .getPartitionsForQuery()
          .mkString(",")
      } else {
        val timePartitionQuery = new TimePartitioningQuery(conf, initialQuery)
        val allCollections = timePartitionQuery.getPartitionsForQuery()
        logger.info(s"Collection rewritten from ${collection} to ${allCollections}")
        collection =  allCollections.mkString(",")
      }
    }
    initialQuery.set("collection", collection)
  }

  def buildQuery(conf: SolrConf): SolrQuery = {
    val query = SolrQuerySupport.toQuery(conf.getQuery.getOrElse("*:*"))
    val solrParams = conf.getArbitrarySolrParams

    if (conf.getStreamingExpr.isDefined) {
      query.setRequestHandler(QT_STREAM)
      query.set(SOLR_STREAMING_EXPR, conf.getStreamingExpr.get.replaceAll("\\s+", " "))
    } else if (conf.getSqlStmt.isDefined) {
      query.setRequestHandler(QT_SQL)
      query.set(SOLR_SQL_STMT, conf.getSqlStmt.get.replaceAll("\\s+", " "))
    } else if (conf.requestHandler.isDefined) {
      query.setRequestHandler(conf.requestHandler.get)
    } else {
      query.setRequestHandler(DEFAULT_REQUEST_HANDLER)
    }

    query.setFields(conf.getFields:_*)
    query.setFilterQueries(conf.getFilters:_*)

    val fqParams = solrParams.remove("fq")
    if (fqParams != null && fqParams.nonEmpty) {
      for (fq <- fqParams) {
        query.addFilterQuery(fq)
      }
    }

    query.setRows(scala.Int.box(conf.getRows.getOrElse(DEFAULT_PAGE_SIZE)))
    if (conf.getSort.isDefined) {
      val sortClauses = SolrRelation.parseSortParamFromString(conf.getSort.get)
      for (sortClause <- sortClauses) {
        query.addSort(sortClause)
      }
    }

    val sortParams = solrParams.remove("sort")
    if (sortParams != null && sortParams.nonEmpty) {
      for (p <- sortParams) {
        val sortClauses = SolrRelation.parseSortParamFromString(p)
        for (sortClause <- sortClauses) {
          query.addSort(sortClause)
        }
      }
    }
    query.add(solrParams)
    // Resolve time series partitions
    resolveCollection(conf, query)
    query
  }

  def findSolrCollectionNameInSql(sqlText: String): Option[String] = {
    val collectionIdMatcher = solrCollectionInSqlPattern.matcher(sqlText)
    if (!collectionIdMatcher.find()) {
      logger.warn(s"No push-down to Solr! Cannot determine collection name from Solr SQL query: ${sqlText}")
      return None
    }
    Some(collectionIdMatcher.group(1))
  }

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
          logger.warn(s"The sort field '${sortField}' does not exist in the base schema")
          return false
        }
      }
      true
    } else {
      false
   }
  }

  def addSortField(baseSchema: StructType, querySchema: StructType, query: SolrQuery, uniqueKey: String): Unit = {
    // if doc values enabled for the uniqueKey field, use that for sorting
    if (baseSchema.fieldNames.contains(uniqueKey)) {
      if (baseSchema(uniqueKey).metadata.getBoolean("docValues")) {
        query.addSort(uniqueKey, SolrQuery.ORDER.asc)
        return
      }
    }

    querySchema.fields.foreach(field => {
      val solrFieldName = if (field.metadata.contains("name")) field.metadata.getString("name") else field.name
      // The field only contains 'multiValued' in the schema if 'flattenMultivalued' is false (it is true by default)
      // Hence, this is not always reliable. It is possible a multiValued field ends up being a sort field
      if (field.metadata.contains("multiValued")) {
        if (!field.metadata.getBoolean("multiValued")) {
          query.addSort(solrFieldName, SolrQuery.ORDER.asc)
          return
        }
      } else {
        query.addSort(solrFieldName, SolrQuery.ORDER.asc)
        return
      }
    })
  }

  def parseSortParamFromString(sortParam: String):  List[SortClause] = {
    val sortClauses: ListBuffer[SortClause] = ListBuffer.empty
    for (pair <- sortParam.split(",")) {
      val sortStringParams = pair.trim.split(" ")
      if (sortStringParams.nonEmpty) {
        if (sortStringParams.size == 2) {
          sortClauses += new SortClause(sortStringParams(0), sortStringParams(1))
        } else {
          sortClauses += SortClause.asc(pair)
        }
      }
    }
    sortClauses.toList
  }

  def addNewFieldsToSolrIfNeeded(dfSchema: StructType, zkHost: String, collectionId: String, solrVersion: String): Set[String] = {
    val solrBaseUrl = SolrSupport.getSolrBaseUrl(zkHost)
    val cloudClient = SolrSupport.getCachedCloudClient(zkHost)
    val solrFields : Map[String, SolrFieldMeta] =
      SolrQuerySupport.getFieldTypes(Set(), solrBaseUrl + collectionId + "/", cloudClient, collectionId)
    val fieldsToAddToSolr = scala.collection.mutable.HashMap.empty[String,AddField]
    dfSchema.fields.foreach(f => {
      if (!solrFields.contains(f.name)) {
        fieldsToAddToSolr += (f.name -> new AddField(SolrRelation.toAddFieldMap(f, solrVersion).asJava))
      }
    })
    if (fieldsToAddToSolr.nonEmpty) {
      SolrRelation.addFieldsForInsert(fieldsToAddToSolr.toMap, collectionId, cloudClient)
    }
    fieldsToAddToSolr.keySet.toSet
  }

  def getFieldsToAdd(
      dfSchema: StructType,
      conf: SolrConf,
      solrVersion: String,
      dynamicSuffixes: Set[String]): Map[String, AddField] = {
    val zkHost = conf.getZkHost.get
    val collectionId = conf.getCollection.get
    val solrBaseUrl = SolrSupport.getSolrBaseUrl(zkHost)
    val cloudClient = SolrSupport.getCachedCloudClient(zkHost)

    val solrFields : Map[String, SolrFieldMeta] =
      SolrQuerySupport.getFieldTypes(Set(), solrBaseUrl + collectionId + "/", cloudClient, collectionId, skipFieldCheck = true)
    val fieldNameForChildDocuments = conf.getChildDocFieldName.getOrElse(DEFAULT_CHILD_DOC_FIELD_NAME)

    val customFieldTypes =
      if (conf.getSolrFieldTypes.isDefined)
        SolrRelation.parseUserSuppliedFieldTypes(conf.getSolrFieldTypes.get)
      else
        Map.empty[String, String]

    // build up a list of updates to send to the Solr Schema API
    val fieldsToAddToSolr = scala.collection.mutable.HashMap.empty[String,AddField]
    dfSchema.fields.foreach(f => {
      if (!solrFields.contains(f.name) && !SolrRelationUtil.isValidDynamicFieldName(f.name, dynamicSuffixes)) {
        if(f.name == fieldNameForChildDocuments) {
          val e = f.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
          e.foreach(ef => {
            val addFieldMap = SolrRelation.toAddFieldMap(ef, solrVersion, customFieldTypes.get(ef.name))
            logger.info(s"adding new field: ${addFieldMap.mkString(", ")}")
            fieldsToAddToSolr += (ef.name -> new AddField(addFieldMap.asJava))
          })
        } else {
          val addFieldMap = SolrRelation.toAddFieldMap(f, solrVersion, customFieldTypes.get(f.name))
          logger.info(s"adding new field: ${addFieldMap.mkString(", ")}")
          fieldsToAddToSolr += (f.name -> new AddField(addFieldMap.asJava))
        }
      }
    })
    fieldsToAddToSolr.toMap
  }

  def addFieldsForInsert(fieldsToAddToSolr: Map[String,AddField], collectionId: String, cloudClient: CloudSolrClient) = {
    logger.info(s"Sending request to Solr schema API to add ${fieldsToAddToSolr.size} fields.")
    val solrParams = new ModifiableSolrParams()
    solrParams.add("updateTimeoutSecs","30")
    val updateList : java.util.ArrayList[Update] = new java.util.ArrayList[Update]
    fieldsToAddToSolr.values.foreach(v => updateList.add(v))
    val addFieldsUpdateRequest = new MultiUpdate(updateList, solrParams)

    val updateResponse : org.apache.solr.client.solrj.response.schema.SchemaResponse.UpdateResponse =
      addFieldsUpdateRequest.process(cloudClient, collectionId)
    if (updateResponse.getStatus >= 400) {
      val errMsg = "Schema update request failed due to: "+updateResponse
      logger.error(errMsg)
      throw new SolrException(ErrorCode.getErrorCode(updateResponse.getStatus), errMsg)
    } else {
      logger.info(s"Request to add ${fieldsToAddToSolr.size} fields returned: ${updateResponse}")
      val respNL = updateResponse.getResponse
      if (respNL != null) {
        val errors = respNL.get("errors")
        if (errors != null) {
          logger.error(s"Request to add ${fieldsToAddToSolr.size} fields failed with errors: ${errors}. Will re-try each add individually ...")
          fieldsToAddToSolr.foreach((pair) => {
            try {
              val resp = pair._2.process(cloudClient, collectionId)
              if (resp.getResponse.get("errors") != null) {
                val e2 = resp.getResponse.get("errors")
                logger.warn(s"Add field ${pair._1} failed due to: ${e2}")
              } else {
                logger.info(s"Add field ${pair._1} returned: $resp")
              }
            } catch {
              case se: SolrServerException => logger.warn(s"Add schema field ${pair._1} failed due to: $se")
              case ioe: IOException => logger.warn(s"Add schema field ${pair._1} failed due to: $ioe")
            }
          })
        }
      }
    }
  }

  def convertRowToSolrInputDocument(row: Row, conf: SolrConf, idField: String): SolrInputDocument = {
    val generateUniqKey: Boolean = conf.genUniqKey.getOrElse(false)
    val generateUniqChildKey: Boolean = conf.genUniqChildKey.getOrElse(false)
    val fieldNameForChildDocuments = conf.getChildDocFieldName.getOrElse(DEFAULT_CHILD_DOC_FIELD_NAME)
    val schema: StructType = row.schema
    val doc = new SolrInputDocument
    schema.fields.foreach(field => {
      val fname = field.name
      breakable {
        if (fname.equals("_version_")) break()
        val isChildDocument = (fname == fieldNameForChildDocuments)
        val fieldIndex = row.fieldIndex(fname)
        val fieldValue : Option[Any] = if (row.isNullAt(fieldIndex)) None else Some(row.get(fieldIndex))
        if (fieldValue.isDefined) {
          val value = fieldValue.get

          if(isChildDocument) {
            val it = value.asInstanceOf[Iterable[GenericRowWithSchema]].iterator
            while (it.hasNext) {
              val elem = it.next()
              val childDoc = new SolrInputDocument
              for (i <- 0 until elem.schema.fields.size) {
                childDoc.setField(elem.schema.fields(i).name, elem.get(i))
              }

              // Generate unique key if the child document doesn't have one
              if (generateUniqChildKey) {
                if (!childDoc.containsKey(idField)) {
                  childDoc.setField(idField, UUID.randomUUID().toString)
                }
              }

              doc.addChildDocument(childDoc)
            }
            break()
          }

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
      }
    })

    // Generate unique key if the document doesn't have one
    if (generateUniqKey) {
      if (!doc.containsKey(idField)) {
        doc.setField(idField, UUID.randomUUID().toString)
      }
    }
    doc
  }

  def toAddFieldMap(sf: StructField, solrVersion: String, solrType: Option[String] = None): Map[String,AnyRef] = {
    val map = scala.collection.mutable.Map[String,AnyRef]()
    map += ("name" -> sf.name)
    map += ("indexed" -> "true")
    map += ("stored" -> "true")
    val dataType = sf.dataType
    if (solrType.isEmpty) {
      map += ("docValues" -> isDocValuesSupported(dataType))
    }
    dataType match {
      case at: ArrayType =>
        map += ("multiValued" -> "true")
        if (SolrSupport.isSolrVersionAtleast(solrVersion, 7, 0, 0))
          map += ("type" -> solrType.getOrElse(toNewSolrType(at.elementType)))
        else
          map += ("type" -> solrType.getOrElse(toOldSolrType(at.elementType)))
      case _ =>
        map += ("multiValued" -> "false")
        if (SolrSupport.isSolrVersionAtleast(solrVersion, 7, 0, 0))
          map += ("type" -> solrType.getOrElse(toNewSolrType(dataType)))
        else
          map += ("type" -> solrType.getOrElse(toOldSolrType(dataType)))
    }
    map.toMap
  }

  def toOldSolrType(dataType: DataType): String = {
    dataType match {
      case BinaryType => "binary"
      case BooleanType => "boolean"
      case DateType | TimestampType => "tdate"
      case DoubleType | _: DecimalType => "tdouble"
      case FloatType => "tfloat"
      case IntegerType | ShortType => "tint"
      case LongType => "tlong"
      case _ => "string"
    }
  }

  def isDocValuesSupported(dataType: DataType): String = {
    dataType match {
      case BinaryType => "false"
      case _ => "true"
    }
  }

  def toNewSolrType(dataType: DataType): String = {
    dataType match {
      case BinaryType => "binary"
      case BooleanType => "boolean"
      case DateType | TimestampType => "pdate"
      case DoubleType | _: DecimalType => "pdouble"
      case FloatType => "pfloat"
      case IntegerType | ShortType => "pint"
      case LongType => "plong"
      case _ => "string"
    }
  }

  def parseUserSuppliedFieldTypes(solrFieldTypes: String): Map[String, String] = {
    val fieldTypeMapBuffer = scala.collection.mutable.Map.empty[String, String]
    if (solrFieldTypes != null) {
      solrFieldTypes.split(",").foreach(f => {
          val nameAndType = f.split(":")
          if (nameAndType.length == 2) {
            val fieldName = nameAndType(0).trim
            val fieldType = nameAndType(1).trim
            if (StringUtils.isNotEmpty(fieldName) && StringUtils.isNotEmpty(fieldType)) {
              fieldTypeMapBuffer.put(fieldName, fieldType)
            }
          }
        }
      )
    }
    fieldTypeMapBuffer.toMap
  }
}

case class StreamField(name:String, dataType: DataType, alias:Option[String], hasReplace: Boolean = false)
case class StreamFields(collection:String,fields:ListBuffer[StreamField],metrics:ListBuffer[StreamField])
