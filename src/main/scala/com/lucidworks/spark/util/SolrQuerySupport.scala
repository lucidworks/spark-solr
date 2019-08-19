package com.lucidworks.spark.util

import java.net.URLDecoder
import java.util

import com.lucidworks.spark.{JsonFacetUtil, LazyLogging, SolrShard}
import com.lucidworks.spark.query._
import com.lucidworks.spark.rdd.SolrRDD
import org.apache.solr.client.solrj.SolrRequest.METHOD
import org.apache.solr.client.solrj._
import org.apache.solr.client.solrj.impl._
import org.apache.solr.client.solrj.request.CollectionAdminRequest.ListAliases
import org.apache.solr.client.solrj.request.schema.SchemaRequest
import org.apache.solr.client.solrj.request.schema.SchemaRequest.UniqueKey
import org.apache.solr.client.solrj.request.{CoreAdminRequest, LukeRequest, QueryRequest}
import org.apache.solr.client.solrj.response.schema.SchemaResponse
import org.apache.solr.client.solrj.response.schema.SchemaResponse.UniqueKeyResponse
import org.apache.solr.client.solrj.response.{CoreAdminResponse, QueryResponse}
import org.apache.solr.common.SolrDocument
import org.apache.solr.common.params.{CommonParams, CoreAdminParams, ModifiableSolrParams, SolrParams}
import org.apache.solr.common.util.NamedList
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.util.control.Breaks._

// Should we support all other additional Solr field tags?
case class SolrFieldMeta(
    fieldType: String,
    dynamicBase: Option[String],
    isRequired: Option[Boolean],
    isMultiValued: Option[Boolean],
    isDocValues: Option[Boolean],
    isStored: Option[Boolean],
    fieldTypeClass: Option[String])

case class PivotField(
    solrField: String,
    prefix: String,
    otherSuffix: String,
    maxCols: Int) {

  def this(solrField: String, prefix: String, maxCols: Int) {
    this(solrField, prefix, "other", maxCols)
  }

  def this(solrField: String, prefix: String) {
    this(solrField, prefix, 10)
  }

}

class QueryResultsIterator(
    solrClient: SolrClient,
    solrQuery: SolrQuery,
    cursorMark: String)
  extends PagedResultsIterator[SolrDocument](solrClient, solrQuery, cursorMark) {
  override protected def processQueryResponse(resp: QueryResponse): util.List[SolrDocument] = resp.getResults
}

/**
  * SolrJ's {@link LukeRequest} doesn't support the 'includeIndexFieldFlags', so this stub class hardcodes it into the
  * underlying SolrParams until this can be fixed in Solr. This can be removed once our SolrJ has the fix for
  * SOLR-13362
  */
class LukeRequestWithoutIndexFlags extends LukeRequest {
  override def getParams: SolrParams = {
    val params = new ModifiableSolrParams(super.getParams)
    params.add("includeIndexFieldFlags", "false")

    params
  }
}

object SolrQuerySupport extends LazyLogging {

  val SOLR_DATA_TYPES: Map[String, DataType] = HashMap(
    "solr.StrField" -> DataTypes.StringType,
    "solr.TextField" -> DataTypes.StringType,
    "solr.BoolField" -> DataTypes.BooleanType,
    "solr.BinaryField" -> DataTypes.BinaryType,

    "solr.TrieIntField" -> DataTypes.LongType,
    "solr.TrieLongField" -> DataTypes.LongType,
    "solr.TrieFloatField" -> DataTypes.DoubleType,
    "solr.TrieDoubleField" -> DataTypes.DoubleType,
    "solr.TrieDateField" -> DataTypes.TimestampType,

    "solr.IntPointField" -> DataTypes.LongType,
    "solr.LongPointField" -> DataTypes.LongType,
    "solr.FloatPointField" -> DataTypes.DoubleType,
    "solr.DoublePointField" -> DataTypes.DoubleType,
    "solr.DatePointField" -> DataTypes.TimestampType
  )

  def getUniqueKey(zkHost: String, collection: String): String = {
    val uniqueKeyRequest = new UniqueKey()
    val client = SolrSupport.getCachedCloudClient(zkHost)
    val uniqueKeyResponse: UniqueKeyResponse = uniqueKeyRequest.process(client, collection)
    if (uniqueKeyResponse.getStatus != 0) {
      throw new RuntimeException(
        "Solr request returned with status code '" + uniqueKeyResponse.getStatus + "'. Response: '" + uniqueKeyResponse.getResponse.toString)
    }
    uniqueKeyResponse.getUniqueKey
  }

  def toQuery(queryString: String): SolrQuery = {

    var solrQuery: SolrQuery = new SolrQuery
    if (queryString == null || queryString.isEmpty) {
      solrQuery = solrQuery.setQuery("*:*")
    } else {
      // Check to see if the query contains additional parameters. E.g., q=*:*&fl=id&sort=id asc
      if (!queryString.contains("q=")) {
        // q= is required if passing list of name/value pairs, so if not there, whole string is the query
        solrQuery.setQuery(queryString)
      } else {
        val paramsNL = new NamedList[Object]()
        val params = queryString.split("&")
        for (param <- params) {
          // only care about the first equals as value may also contain equals
          val eqAt = param.indexOf('=')
          if (eqAt != -1) {
            val key = param.substring(0, eqAt)
            val value = URLDecoder.decode(param.substring(eqAt + 1), "UTF-8")
            if (key == "sort") {
              if (!value.contains(" ")) {
                solrQuery.addSort(SolrQuery.SortClause.asc(value))
              } else {
                val split = value.split(" ")
                solrQuery.addSort(SolrQuery.SortClause.create(split(0), split(1)))
              }
            } else {
              paramsNL.add(key, value)
            }
          }
        }
        if (!paramsNL.isEmpty) {
          solrQuery.add(SolrParams.toSolrParams(paramsNL))
        }
      }
    }
    val rows = solrQuery.getRows
    if (rows == null)
      solrQuery.setRows(QueryConstants.DEFAULT_PAGE_SIZE)

    solrQuery
  }

  def addDefaultSort(solrQuery: SolrQuery, uniqueKey: String): Unit = {
    if (solrQuery.getSortField == null || solrQuery.getSortField.isEmpty) {
      solrQuery.addSort(SolrQuery.SortClause.asc(uniqueKey))
      logger.debug(s"Added default sort clause on uniqueKey field $uniqueKey to query $solrQuery")
    }
  }

  def querySolr(
      solrClient: SolrClient,
      solrQuery: SolrQuery,
      startIndex: Int,
      cursorMark: String): Option[QueryResponse] =
    querySolr(solrClient, solrQuery, startIndex, cursorMark, null)

  // Use this method instead of [[SolrClient.queryAndStreamResponse]] to use POST method for queries
  def queryAndStreamResponsePost(params: SolrParams, callback: StreamingResponseCallback, cloudClient: SolrClient): QueryResponse = {
    val parser: ResponseParser = new StreamingBinaryResponseParser(callback)
    val req: QueryRequest = new QueryRequest(params, METHOD.POST)
    req.setStreamingResponseCallback(callback)
    req.setResponseParser(parser)
    req.process(cloudClient)
  }

  /*
    Query solr and retry on Socket or network exceptions
   */
  def querySolr(
      solrClient: SolrClient,
      solrQuery: SolrQuery,
      startIndex: Int,
      cursorMark: String,
      callback: StreamingResponseCallback): Option[QueryResponse] = {
    var resp: Option[QueryResponse] = None

    try {
      if (cursorMark != null) {
        solrQuery.setStart(0)
        solrQuery.set("cursorMark", cursorMark)
        if (solrQuery.get("sort") == null || solrQuery.get("sort").isEmpty) {
          addDefaultSort(solrQuery, QueryConstants.DEFAULT_REQUIRED_FIELD)
        }
      } else {
        solrQuery.setStart(startIndex)
      }

      if (solrQuery.getRows == null)
        solrQuery.setRows(QueryConstants.DEFAULT_PAGE_SIZE)

      if (callback != null) {
        resp = Some(queryAndStreamResponsePost(solrQuery, callback, solrClient))
      } else {
        resp = Some(solrClient.query(solrQuery, METHOD.POST))
      }
    } catch {
      case e: Exception =>
        logger.error(s"Query [$solrQuery] failed due to: $e")

        //re-try once in the event of a communications error with the server
        if (SolrSupport.shouldRetry(e)) {
          try {
            Thread.sleep(2000L)
          } catch {
            case ie: InterruptedException => Thread.interrupted()
          }

          try {
            if (callback != null) {
              resp = Some(queryAndStreamResponsePost(solrQuery, callback, solrClient))
            } else {
              resp = Some(solrClient.query(solrQuery, METHOD.POST))
            }
          } catch {
            case execOnRetry: SolrServerException =>
              logger.error(s"Query on retry [$solrQuery] failed due to: $execOnRetry")
              throw execOnRetry
            case execOnRetry1: Exception =>
              logger.error(s"Query on retry [$solrQuery] failed due to: $execOnRetry1")
              throw new SolrServerException(execOnRetry1)
          }
        } else {
          e match {
            case e1: SolrServerException => throw e1
            case e2: Exception => throw new SolrServerException(e2)
          }
        }
    }
    resp
  }

  def setQueryDefaultsForShards(solrQuery: SolrQuery, uniqueKey: String) = {
    solrQuery.set("distrib", "false")
    solrQuery.setStart(0)
    if (solrQuery.getRows == null) {
      solrQuery.setRows(QueryConstants.DEFAULT_PAGE_SIZE)
    }

    SolrQuerySupport.addDefaultSort(solrQuery, uniqueKey)
  }

  def setQueryDefaultsForTV(solrQuery: SolrQuery, field: String, uniqueKey: String): Unit = {
    if (solrQuery.getRequestHandler == null)
      solrQuery.setRequestHandler("/tvrh")

    solrQuery.set("shards.qt", solrQuery.getRequestHandler)
    solrQuery.set("tv.fl", field)
    solrQuery.set("fq", field + ":[* TO *]")
    solrQuery.set("tv.tf_idf", "true")

    solrQuery.set("distrib", false)
    solrQuery.setStart(0)
    if (solrQuery.getRows == null) {
      solrQuery.setRows(QueryConstants.DEFAULT_PAGE_SIZE)
    }

    SolrQuerySupport.addDefaultSort(solrQuery, uniqueKey)
  }

  def getFieldTypes(
      fields: Set[String],
      solrUrl: String,
      cloudClient: CloudSolrClient,
      collection: String,
      skipDynamicExtensions: Boolean=true,
      skipFieldCheck: Boolean = false): Map[String, SolrFieldMeta] = {
    val fieldTypeMap = new mutable.HashMap[String, SolrFieldMeta]()
    val fieldTypeToClassMap = getFieldTypeToClassMap(cloudClient, collection)
    logger.debug("Get field types for fields: {} ", fields.mkString(","))
    val fieldDefinitionsFromSchema = getFieldDefinitionsFromSchema(solrUrl, fields.toSeq, cloudClient, collection)
    fieldDefinitionsFromSchema.filterKeys(k => if (skipDynamicExtensions) !k.startsWith("*_") && !k.endsWith("_*") else true).foreach {
      case(name, payloadRef) =>
      payloadRef match {
        case m: Map[_, _] if m.keySet.forall(_.isInstanceOf[String])=>
          val payload = m.asInstanceOf[Map[String, Any]]
          // No valid checks for name and value :(
          val name = payload.get("name").get.asInstanceOf[String]
          val fieldType = payload.get("type").get.asInstanceOf[String]

          val isRequired: Option[Boolean] = {
            if (payload.contains("required")) {
              if (payload.get("required").isDefined) {
                payload.get("required").get match {
                  case v: Boolean => Some(v)
                  case v1: AnyRef => Some(String.valueOf(v1).equals("true"))
                }
              } else None
            } else None
          }

          val isMultiValued: Option[Boolean] = {
            if (payload.contains("multiValued")) {
              if (payload.get("multiValued").isDefined) {
                payload.get("multiValued").get match {
                  case v: Boolean => Some(v)
                  case v1: AnyRef => Some(String.valueOf(v1).equals("true"))
                }
              } else None
            } else None
          }

          val isStored: Option[Boolean] = {
            if (payload.contains("stored")) {
              if (payload.get("stored").isDefined) {
                payload.get("stored").get match {
                  case v: Boolean => Some(v)
                  case v1: AnyRef => Some(String.valueOf(v1).equals("true"))
                }
              } else None
            } else None
          }

          val isDocValues: Option[Boolean] = {
            // location field types are not docValue supported even though schema says so
            if (payload.contains("docValues") && fieldType != "location") {
              if (payload.get("docValues").isDefined) {
                payload.get("docValues").get match {
                  case v: Boolean => Some(v)
                  case v1: AnyRef => Some(String.valueOf(v1).equals("true"))
                }
              } else None
            } else None
          }

          val dynamicBase: Option[String] = {
            if (payload.contains("dynamicBase")) {
              if (payload.get("dynamicBase").isDefined) {
                payload.get("dynamicBase").get match {
                  case v: String => Some(v)
                }
              } else None
            } else None
          }

          val fieldClassType: Option[String] = {
            if (fieldTypeToClassMap.contains(fieldType)) {
              if (fieldTypeToClassMap.get(fieldType).isDefined) {
                Some(fieldTypeToClassMap.get(fieldType).get)
              } else None
            } else None
          }

          val solrFieldMeta = SolrFieldMeta(fieldType, dynamicBase, isRequired, isMultiValued, isDocValues, isStored, fieldClassType)

          if (skipFieldCheck) {
            fieldTypeMap.put(name, solrFieldMeta)
          } else {
            if ((solrFieldMeta.isStored.isDefined && !solrFieldMeta.isStored.get) &&
                (solrFieldMeta.isDocValues.isDefined && !solrFieldMeta.isDocValues.get)) {
              logger.trace(s"Can't retrieve an index only field: '$name'. Field info $payload")
            } else if ((solrFieldMeta.isStored.isDefined && !solrFieldMeta.isStored.get) &&
                (solrFieldMeta.isMultiValued.isDefined && solrFieldMeta.isMultiValued.get) &&
                (solrFieldMeta.isDocValues.isDefined && solrFieldMeta.isDocValues.get)) {
              logger.trace(s"Can't retrieve a non-stored multiValued docValues field: '$name'. The payload info is $payload")
            } else {
              fieldTypeMap.put(name, solrFieldMeta)
            }
          }
        case somethingElse: Any => logger.warn(s"Unknown class type '${somethingElse.getClass.toString}'")
      }
    }

    if (fieldTypeMap.isEmpty)
      logger.warn("No readable fields found!")
    fieldTypeMap.toMap
  }

  /**
    * Do multiple requests if the length of url exceeds limit size (2048).
    * We need this to retrieve schema of dynamic fields
    * @param solrUrl
    * @param fieldNames
    * @param fieldDefs
    * @return
    */
  def getFieldDefinitionsFromSchema(
      solrUrl: String,
      fieldNames: Seq[String],
      cloudSolrClient: CloudSolrClient,
      collection: String,
      fieldDefs: Map[String, Any] = Map.empty): Map[String, Any] = {
    val fieldsUrlBase = solrUrl + "schema/fields?showDefaults=true&includeDynamic=true"
    logger.debug("Requesting schema for fields: {} ", fieldNames.mkString(","))


    if (fieldNames.isEmpty && fieldDefs.isEmpty)
      return fetchFieldSchemaInfoFromSolr("", cloudSolrClient, collection)

    if (fieldNames.isEmpty && fieldDefs.nonEmpty)
      return fieldDefs

    val allowedUrlLimit = 2048 - fieldsUrlBase.length
    var flLength = 0
    val sb = new StringBuilder()

    for (i <- fieldNames.indices) {
      val fieldName = fieldNames(i)
      if (flLength + fieldName.length + 1 < allowedUrlLimit) {
        if (fieldName != null || fieldName.nonEmpty) {
          sb.append(fieldName)
          if (i < fieldNames.size) sb.append(",")
          flLength = flLength + fieldName.length + 1
        }
      } else {
        val defs: Map[String, Any] = fetchFieldSchemaInfoFromSolr(sb.toString(), cloudSolrClient, collection)
        return getFieldDefinitionsFromSchema(solrUrl, fieldNames.takeRight(fieldNames.length - i), cloudSolrClient, collection, defs ++ fieldDefs)
      }
    }
    val defs = fetchFieldSchemaInfoFromSolr(sb.toString(), cloudSolrClient, collection)
    getFieldDefinitionsFromSchema(solrUrl, Seq.empty, cloudSolrClient, collection, defs ++ fieldDefs)
  }

  def fetchFieldSchemaInfoFromSolr(fl: String, cloudSolrClient: CloudSolrClient, collection: String) : Map[String, Any] = {
    try {
      val params = new ModifiableSolrParams()
      params.set("showDefaults", "true")
      params.set("includeDynamic", "true")
      if (fl != null && fl.nonEmpty) {
        params.set("fl", fl)
      }

      val schemaRequest = new SchemaRequest.Fields(params)
      val response: SchemaResponse.FieldsResponse = schemaRequest.process(cloudSolrClient, collection)

      logger.trace("Schema response from Solr: {}", response.getFields)
      if (response.getStatus != 0) {
        throw new RuntimeException(
          "Solr request returned with status code '" + response.getStatus + "'. Response: '" + response.getResponse.toString)
      }
      constructFieldInfoMap(asScalaBuffer(response.getFields).toList)
    } catch {
      case e: Exception =>
        logger.error(s"Can't get field metadata from Solr using request due to exception $e")
        e match {
          case e1: RuntimeException => throw e1
          case e2: Exception => throw new RuntimeException(e2)
        }
    }
  }

  def constructFieldInfoMap(fieldsInfoList: List[Any]): Map[String, Any] = {
    val fieldInfoMap = new mutable.HashMap[String, AnyRef]()
    fieldsInfoList.foreach {
      case m: Map[_, _] if m.keySet.forall(_.isInstanceOf[String])=>
        val fieldInfo = m.asInstanceOf[Map[String, Any]]
        if (fieldInfo.contains("name")) {
          val fieldName = fieldInfo.get("name")
          if (fieldName.isDefined) {
            fieldInfoMap.put(fieldName.get.asInstanceOf[String], fieldInfo)
          } else {
            logger.info(s"value for key 'name' is not defined in the payload $fieldInfo")
          }
        } else {
          logger.info(s"'name' is not defined in the payload $fieldInfo")
        }
      case m: util.Map[_, _] if mapAsScalaMap(m).keySet.forall(_.isInstanceOf[String]) =>
        val fieldInfo = mapAsScalaMap(m).toMap.asInstanceOf[Map[String, Any]]
        if (fieldInfo.contains("name")) {
          val fieldName = fieldInfo.get("name")
          if (fieldName.isDefined) {
            fieldInfoMap.put(fieldName.get.asInstanceOf[String], fieldInfo)
          } else {
            logger.info(s"value for key 'name' is not defined in the payload $fieldInfo")
          }
        } else {
          logger.info(s"'name' is not defined in the payload $fieldInfo")
        }
      case somethingElse: Any => throw new Exception("Unknown type '" + somethingElse.getClass)
    }
    fieldInfoMap.toMap
  }

  def getFieldsFromLuke(zkHost: String, collection: String, maxShardsToSample: Option[Int]): Set[String] = {
    val allShardList = SolrSupport.buildShardList(zkHost, collection, false)
    val sampledShardList = randomlySampleShardList(allShardList, maxShardsToSample.getOrElse(10))
    val fieldSetBuffer = sampledShardList.par.map(shard => {
      val randomReplica = SolrRDD.randomReplica(shard)
      val replicaHttpClient = SolrSupport.getCachedHttpSolrClient(randomReplica.replicaUrl, zkHost)
      getFieldsFromLukePerShard(zkHost, replicaHttpClient)
    }).flatten
    fieldSetBuffer.toSet.seq
  }

  private def randomlySampleShardList(fullShardList: List[SolrShard], maxShardsToSample: Int): List[SolrShard] = {
    if (fullShardList.size > maxShardsToSample) {
      Random.shuffle(fullShardList).slice(0, maxShardsToSample)
    } else {
      fullShardList
    }
  }

  def getFieldsFromLukePerShard(zkHost: String, httpSolrClient: HttpSolrClient): Set[String] = {
    val lukeRequest = new LukeRequestWithoutIndexFlags()
    lukeRequest.setNumTerms(0)
    val lukeResponse = lukeRequest.process(httpSolrClient)
    if (lukeResponse.getStatus != 0) {
      throw new RuntimeException(
        "Solr request returned with status code '" + lukeResponse.getStatus + "'. Response: '" + lukeResponse.getResponse.toString)
    }
    mapAsScalaMap(lukeResponse.getFieldInfo).toMap.keySet
  }

  def getCollectionsForAlias(zkHost: String, alias: String): Option[List[String]] = {
    val listAliases = new ListAliases()
    val collectionAdminResp = listAliases.process(SolrSupport.getCachedCloudClient(zkHost))
    if (collectionAdminResp.getStatus == 0) {
      val allAliases = collectionAdminResp.getAliases.toMap
      if (allAliases.contains(alias)) {
        val aliases = allAliases(alias).split(",").toList.sorted
        logger.debug(s"Resolved alias ${alias} to ${aliases}")
        return Some(aliases)
      }
    } else {
      logger.error(s"Failed to get alias list from Solr. Response text ${collectionAdminResp.getResponse.toString}")
    }
    None
  }

  def validateExportHandlerQuery(solrServer: SolrClient, solrQuery: SolrQuery) = {
    var cloneQuery = solrQuery.getCopy
    cloneQuery = cloneQuery.setRows(0)
    val queryRequest = new QueryRequest(cloneQuery)
    queryRequest.setResponseParser(new InputStreamResponseParser("json"))
    queryRequest.setMethod(SolrRequest.METHOD.POST)
    try {
      val queryResponse = queryRequest.process(solrServer)
      if (queryResponse.getStatus != 0) {
        throw new RuntimeException(
          "Solr request returned with status code '" + queryResponse.getStatus + "'. Response: '" + queryResponse.getResponse.toString)
      }
    } catch {
      case e: Any =>
        logger.error(s"Error while validating query request: ${cloneQuery.toString}")
        throw e
    }
  }

  def getNumDocsFromSolr(collection: String, zkHost: String, query: Option[SolrQuery]): Long = {
    val solrQuery = if (query.isDefined) query.get else new SolrQuery().setQuery("*:*")
    val cloneQuery = solrQuery.getCopy
    cloneQuery.set("distrib", "true")
    cloneQuery.set(CommonParams.QT, "/select")
    cloneQuery.setRows(0)
    val cloudClient = SolrSupport.getCachedCloudClient(zkHost)
    val response = cloudClient.query(collection, cloneQuery, SolrRequest.METHOD.POST)
    response.getResults.getNumFound
  }

  /*
    Return solr field types along with their actual class types.
    E.g. { "binary": "solr.BinaryField",
           "boolean": "solr.BooleanField"
         }
   */
  def getFieldTypeToClassMap(cloudSolrClient: CloudSolrClient, collection: String) : Map[String, String] = {
    val fieldTypeToClassMap: mutable.Map[String, String] = new mutable.HashMap[String, String]

    val fieldTypeRequest = new SchemaRequest.FieldTypes()
    val fieldTypeResponse = fieldTypeRequest.process(cloudSolrClient, collection)
    if (fieldTypeResponse.getStatus != 0) {
      throw new RuntimeException(
        "Solr request returned with status code '" + fieldTypeResponse.getStatus + "'. Response: '" + fieldTypeResponse.getResponse.toString)
    }
    for (fieldType <- asScalaBuffer(fieldTypeResponse.getFieldTypes)) {
      val fieldTypeName = fieldType.getAttributes.get("name").toString
      val fieldTypeClass = fieldType.getAttributes.get("class").asInstanceOf[String]
      fieldTypeToClassMap.put(fieldTypeName, fieldTypeClass)
    }
    fieldTypeToClassMap.toMap
  }

  def getPivotFieldRange(schema: StructType, pivotPrefix: String): Array[Int] = {
    val schemaFields: Array[StructField] = schema.fields
    var startAt: Int = -1
    var endAt: Int = -1

    schemaFields.zipWithIndex.map{case(schemaField, i) =>
      val name = schemaField.name
      if (startAt == -1 && name.startsWith(pivotPrefix)) {
        startAt = i
      }
      if (startAt != -1 && !name.startsWith(pivotPrefix)) {
        endAt = i-1 // we saw the last field in the range before this field
        break()
      }
      (startAt, endAt)
    }
    Array(startAt, endAt)
  }

  def fillPivotFieldValues(rawValue: String, row: Array[Any], schema: StructType, pivotPrefix: String): Unit = {
    val range = getPivotFieldRange(schema, pivotPrefix)
    for (i <- range(0) to range(1)) row(i) = 0
    try {
      row(schema.fieldIndex(pivotPrefix + rawValue.toLowerCase)) = 1
    } catch {
      case e: IllegalArgumentException => row(range(1)) = 1
    }
  }

  /**
   * Allows you to pivot a categorical field into multiple columns that can be aggregated into counts, e.g.
   * a field holding HTTP method (http_verb=GET) can be converted into: http_method_get=1, which is a common
   * task when creating aggregations.
   */
  def withPivotFields(
      solrData: DataFrame,
      pivotFields: Array[PivotField],
      solrRDD: SolrRDD[_],
      escapeFieldNames: Boolean): DataFrame = {
    val schema = SolrRelationUtil.getBaseSchema(solrRDD.zkHost, solrRDD.collection, None, escapeFieldNames, Some(true), false, Set.empty)
    val schemaWithPivots = toPivotSchema(solrData.schema, pivotFields, solrRDD.collection, schema, solrRDD.uniqueKey, solrRDD.zkHost)

    val withPivotFields: RDD[Row] = solrData.rdd.map(row => {
      val fields = Array.empty[Any]
      for (i <- 0 to row.length-1) fields(i) = row.get(i)

      for (pf <- pivotFields)
        SolrQuerySupport.fillPivotFieldValues(row.getString(row.fieldIndex(pf.solrField)), fields, schemaWithPivots, pf.prefix)

      Row(fields:_*)
    })

    solrData.sqlContext.createDataFrame(withPivotFields, schemaWithPivots)
  }

  def toPivotSchema(
      baseSchema: StructType,
      pivotFields: Array[PivotField],
      collection: String,
      schema: StructType,
      uniqueKey: String,
      zkHost: String): StructType = {
    val pivotSchemaFields = new ListBuffer[StructField]
    pivotSchemaFields.addAll(baseSchema.fields.toList)
    for (pf: PivotField <- pivotFields) {
      val structFields = getPivotSchema(pf.solrField, pf.maxCols, pf.prefix, pf.otherSuffix, collection, schema, uniqueKey, zkHost)
      pivotSchemaFields.addAll(structFields)
    }
    DataTypes.createStructType(pivotSchemaFields)
  }

  def getPivotSchema(
      fieldName: String,
      maxCols: Int,
      fieldPrefix: String,
      otherName: String,
      collection: String,
      schema: StructType,
      uniqueKey: String,
      zkHost: String): List[StructField] = {
    val listOfFields = new ListBuffer[StructField]
    val solrQuery = new SolrQuery("*:*")
    solrQuery.set("collection", collection)
    solrQuery.setFacet(true)
    solrQuery.addFacetField(fieldName)
    solrQuery.setFacetMinCount(1)
    solrQuery.setFacetLimit(maxCols)
    solrQuery.setRows(0)
    val resp = querySolr(SolrSupport.getCachedCloudClient(zkHost), solrQuery, 0, null)
    if (resp.isDefined) {
      val ff = resp.get.getFacetField(fieldName)
      for (f <- ff.getValues) {
        listOfFields.add(DataTypes.createStructField(fieldPrefix + f.getName .toLowerCase, DataTypes.IntegerType, false))
        if (otherName != null) {
          listOfFields.add(DataTypes.createStructField(fieldPrefix + otherName, DataTypes.IntegerType, false))
        }
      }
    } else {
      throw new Exception("No response found for query '" + solrQuery)
    }
    listOfFields.toList
  }

  def getMaxVersion(solrClient: SolrClient, collection: String, solrQuery: SolrQuery, fieldName: String): Option[Long] = {

    // Do not do this for collection aliases
    if (collection.split(",").length > 1)
      return None

    val statsQuery = solrQuery.getCopy()
    statsQuery.setRows(1)
    statsQuery.setStart(0)
    statsQuery.remove("cursorMark")
    statsQuery.remove("distrib")
    statsQuery.setFields(fieldName)
    statsQuery.setSort(fieldName, SolrQuery.ORDER.desc)
    val qr: QueryResponse = solrClient.query(collection, statsQuery, SolrRequest.METHOD.POST)
    if (qr.getResults.getNumFound != 0) {
      val maxO = qr.getResults.get(0).getFirstValue(fieldName)
      val max = java.lang.Long.parseLong(maxO.toString)
      return Some(max)
    }
    None
  }

  def getDataframeFromFacetQuery(solrQuery: SolrQuery, collection: String, zkhost: String, spark: SparkSession): DataFrame = {
    try {
      getDataframeFromFacetQueryWithoutRetry(solrQuery, collection, zkhost, spark)
    } catch {
      case sse : SolrServerException =>
        logger.info(s"Retrying request due to ${sse.getMessage}")
        getDataframeFromFacetQueryWithoutRetry(solrQuery, collection, zkhost, spark)
      case e: Exception =>
        if (SolrSupport.shouldRetry(e)) {
          logger.info(s"Retrying request due to ${e.getMessage}")
          getDataframeFromFacetQueryWithoutRetry(solrQuery, collection, zkhost, spark)
        } else {
          throw e
        }
      case e : Throwable => throw e
    }
  }

  def getDataframeFromFacetQueryWithoutRetry(solrQuery: SolrQuery, collection: String, zkhost: String, spark: SparkSession): DataFrame = {
    implicit val formats: DefaultFormats.type = DefaultFormats // needed for json4s

    val solrClient: CloudSolrClient = SolrSupport.getCachedCloudClient(zkhost)
    val cloneQuery = solrQuery.getCopy.set("rows", 0).set("wt", "json").set("distrib", "true")
    logger.debug(s"Facet Query: ${cloneQuery}")
    val queryRequest: QueryRequest = new QueryRequest(cloneQuery)
    queryRequest.setMethod(SolrRequest.METHOD.POST)

    queryRequest.setResponseParser(new NoOpResponseParser("json"))
    val namedList = solrClient.request(queryRequest, collection)

    if (namedList.get("response") != null) {
      logger.trace(s"Query response for JSON facet query: ${namedList}")
      namedList.get("response") match {
        case rawString : String =>
          val jsonResp = parse(rawString)
          if ((jsonResp \ "responseHeader" \ "status") != JNothing) {
            val status = (jsonResp \ "responseHeader" \ "status").extract[Integer]
            if (status != 0) {
              throw new RuntimeException("Solr request returned with status code '" + status + "'. Response: '" + rawString)
            }
          }
          if ((jsonResp \ "facets") != JNothing) {
            return JsonFacetUtil.parseFacetResponse(jsonResp \ "facets", spark)
          }
        case a: Any => logger.info(s"Response is not a String. Response type ${a.getClass}. Returning empty dataframe")
      }
      spark.emptyDataFrame
    } else {
      spark.emptyDataFrame
    }
  }

  case class ResponseHeader(status: Int, QTime: Int)
  case class SolrCoreAdminResponse(responseHeader: ResponseHeader, initFailures: Map[_, _], status: Map[String, Map[String, _]])

  def getSolrCores(cloudSolrClient: CloudSolrClient): SolrCoreAdminResponse = {
    implicit val formats: DefaultFormats.type = DefaultFormats // needed for json4s
    val coreAdminRequest = new CoreAdminRequest()
    coreAdminRequest.setAction(CoreAdminParams.CoreAdminAction.STATUS)
    coreAdminRequest.setResponseParser(new NoOpResponseParser("json"))
    val coreAdminResponse : CoreAdminResponse = coreAdminRequest.process(cloudSolrClient)
    val rawString : String = coreAdminResponse.getResponse.get("response").asInstanceOf[String]
    parse(rawString).extract[SolrCoreAdminResponse]
  }
}
