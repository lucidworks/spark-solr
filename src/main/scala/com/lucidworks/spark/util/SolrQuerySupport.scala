package com.lucidworks.spark.util

import java.nio.charset.StandardCharsets
import java.util
import com.lucidworks.spark.query._
import com.lucidworks.spark.rdd.SolrRDD
import org.apache.http.client.utils.URLEncodedUtils
import org.apache.solr.client.solrj.SolrRequest.METHOD
import org.apache.solr.client.solrj.impl.{InputStreamResponseParser, StreamingBinaryResponseParser}
import org.apache.solr.client.solrj.request.QueryRequest
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.client.solrj._
import org.apache.solr.common.{SolrDocument, SolrException}
import org.apache.solr.common.params.SolrParams
import org.apache.solr.common.util.NamedList
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.sql.types.{StructField, StructType, DataTypes, DataType}

import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

import com.lucidworks.spark.util.JsonUtil._
import org.json4s._
import org.json4s.jackson.JsonMethods._

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

object SolrQuerySupport extends Logging {

  val SOLR_DATA_TYPES: Map[String, DataType] = HashMap(
    "solr.StrField" -> DataTypes.StringType,
    "solr.TextField" -> DataTypes.StringType,
    "solr.BoolField" -> DataTypes.BooleanType,
    "solr.TrieIntField" -> DataTypes.LongType,
    "solr.TrieLongField" -> DataTypes.LongType,
    "solr.TrieFloatField" -> DataTypes.DoubleType,
    "solr.TrieDoubleField" -> DataTypes.DoubleType,
    "solr.TrieDateField" -> DataTypes.TimestampType,
    "solr.BinaryField" -> DataTypes.BinaryType
  )

  def getUniqueKey(zkHost: String, collection: String): String = {
    try {
      val solrBaseUrl = SolrSupport.getSolrBaseUrl(zkHost)
      // Hit Solr Schema API to get base information
      val schemaUrl: String = solrBaseUrl + collection + "/schema"
      try {
        val schemaMeta = SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient, schemaUrl, 2)
        if (schemaMeta.has("schema") && (schemaMeta \ "schema").has("uniqueKey")) {
          schemaMeta \ "schema" \ "uniqueKey" match {
            case v: JString => return v.s
            case v: Any => throw new Exception("Unexpected type '" + v.getClass + "' other than JString for uniqueKey '" + v + "'");
          }
        }
      }
      catch {
        case solrExc: SolrException =>
          log.warn("Can't get uniqueKey for " + collection + " due to solr: " + solrExc)
      }
    } catch {
      case e: Exception => log.warn("Can't get uniqueKey for " + collection + " due to: " + e)
    }
    QueryConstants.DEFAULT_REQUIRED_FIELD
  }

  def toQuery(queryString: String): SolrQuery = {

    var solrQuery: SolrQuery = new SolrQuery
    if (queryString == null || queryString.isEmpty) {
      solrQuery = solrQuery.setQuery("*:*")
    } else {
      // Check to see if the query contains additional parameters. E.g., q=*:*&fl=id&sort=id asc
      if (!queryString.contains("=")) {
        // no name-value pairs ... just assume this single clause is the q part
        solrQuery.setQuery(queryString)
      } else {
        val params = new NamedList[Object]()
        for(nvp <- URLEncodedUtils.parse(queryString, StandardCharsets.UTF_8)) {
          val value = nvp.getValue
          if (value != null && value.length > 0) {
            val name = nvp.getName
            if ("sort".equals(name)) {
              if (!value.contains(" ")) {
                solrQuery.addSort(SolrQuery.SortClause.asc(value))
              } else {
                val split = value.split(" ")
                solrQuery.addSort(SolrQuery.SortClause.create(split(0), split(1)))
              }
            } else {
              params.add(name, value)
            }
          }
        }
        solrQuery.add(SolrParams.toSolrParams(params))
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
      logInfo(s"Added default sort clause on uniqueKey field $uniqueKey to query $solrQuery")
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
        log.error("Query [" + solrQuery + "] failed due to: " + e)

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
              log.error("Query on retry [" + solrQuery + "] failed due to: " + execOnRetry)
              throw execOnRetry
            case execOnRetry1: Exception =>
              log.error("Query on retry [" + solrQuery + "] failed due to: " + execOnRetry1)
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

  def getFieldTypes(fields: Set[String], solrBaseUrl: String, collection: String): Map[String, SolrFieldMeta] =
    getFieldTypes(fields, solrBaseUrl + collection + "/")

  def getFieldTypes(fields: Set[String], solrUrl: String): Map[String, SolrFieldMeta] = {
    val fieldTypeMap = new mutable.HashMap[String, SolrFieldMeta]()
    val fieldTypeToClassMap = getFieldTypeToClassMap(solrUrl)
    val fieldNames = if (fields == null || fields.isEmpty) getFieldsFromLuke(solrUrl) else fields
    val fieldDefinitionsFromSchema = getFieldDefinitionsFromSchema(solrUrl, fieldNames)
    fieldDefinitionsFromSchema.foreach {
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
            if (payload.contains("docValues")) {
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

          if ((solrFieldMeta.isStored.isDefined && !solrFieldMeta.isStored.get) &&
            (solrFieldMeta.isDocValues.isDefined && !solrFieldMeta.isDocValues.get)) {
              if (log.isDebugEnabled)
                log.debug("Can't retrieve an index only field: '" + name + "'. Field info " + payload)
          } else if ((solrFieldMeta.isStored.isDefined && !solrFieldMeta.isStored.get) &&
            (solrFieldMeta.isMultiValued.isDefined && solrFieldMeta.isMultiValued.get) &&
            (solrFieldMeta.isDocValues.isDefined && solrFieldMeta.isDocValues.get)) {
              if (log.isDebugEnabled)
                log.debug("Can't retrieve a non-stored multiValued docValues field: '" + name + "'. The payload info is " + payload)
          } else {
            fieldTypeMap.put(name, solrFieldMeta)
          }
        case somethingElse: Any => log.warn("Unknown class type '" + somethingElse.getClass.toString + "'; "+somethingElse)
      }
    }

    if (fieldTypeMap.isEmpty)
      log.warn("No readable fields found!")
    fieldTypeMap.toMap
  }

  def getFieldDefinitionsFromSchema(solrUrl: String, fieldNames: Set[String]): Map[String, Any] = {
    val fl: Option[String] = if (fieldNames.nonEmpty) {
      val sb = new StringBuilder
      sb.append("&fl=")
      fieldNames.zipWithIndex.foreach{ case(name, index) =>
        sb.append(name)
        if (index < fieldNames.size) sb.append(",")
      }
      Some(sb.toString())
    } else None

    val fieldsUrlBase = solrUrl + "schema/fields?showDefaults=true&includeDynamic=true"
    val flList = fl.getOrElse("")
    if (flList.length > (2048 - fieldsUrlBase.length)) {
      val fieldDefs = scala.collection.mutable.HashMap.empty[String,Any]
      // go get all fields from Solr and then prune from there
      val allFields = fetchFieldSchemaInfoFromSolr(fieldsUrlBase)
      fieldNames.foreach(fname => {
        if (allFields.containsKey(fname)) {
          fieldDefs.put(fname, allFields.get(fname).get)
        }
      })
      fieldDefs.toMap
    } else {
      fetchFieldSchemaInfoFromSolr(fieldsUrlBase+flList)
    }
  }

  def fetchFieldSchemaInfoFromSolr(fieldsUrl: String) : Map[String, Any] = {
    try {
      SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient, fieldsUrl, 2).values match {
        case m: Map[_, _] if m.keySet.forall(_.isInstanceOf[String])=>
          val payload = m.asInstanceOf[Map[String, Any]]
          if (payload.contains("fields")) {
            if (payload.get("fields").isDefined) {
              payload.get("fields").get match {
                case fields: List[Any] =>
                  constructFieldInfoMap(fields)
              }
            } else {
              throw new Exception("No fields payload inside the response: " + payload)
            }
          } else {
            throw new Exception("No fields payload inside the response: " + payload)
          }
        case somethingElse: Any => throw new Exception("Unknown type '" + somethingElse.getClass + "' from schema object " + somethingElse)
      }
    } catch {
      case e: Exception =>
        log.error("Can't get field metadata from Solr using request '" + fieldsUrl + "' due to exception " + e)
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
            log.info("value for key 'name' is not defined in the payload " + fieldInfo)
          }
        } else {
          log.info("'name' is not defined in the payload " + fieldInfo)
        }
      case somethingElse: Any => throw new Exception("Unknown type '" + somethingElse.getClass)
    }
    fieldInfoMap.toMap
  }

  def getFieldsFromLuke(solrUrl: String): Set[String] = {
    val lukeUrl: String = solrUrl + "admin/luke?numTerms=0"
    try {
      val adminMeta: JValue = SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient, lukeUrl, 2)
      if (!adminMeta.has("fields")) {
        throw new Exception("Cannot find 'fields' payload inside Schema: " + compact(adminMeta))
      }
      val fieldsRef = adminMeta \ "fields"
      fieldsRef.values match {
        case m: Map[_, _] if m.keySet.forall(_.isInstanceOf[String]) => m.asInstanceOf[Map[String, Any]].keySet
        case somethingElse: Any =>  throw new Exception("Unknown type '" + somethingElse.getClass + "'")
      }
    } catch {
      case e1: Exception =>
        log.warn("Can't get schema fields from url " + lukeUrl + " due to: " + e1)
        throw e1
    }
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
        log.error("Error while validating query request: " + queryRequest.toString)
        throw e
    }
  }

  def getNumDocsFromSolr(collection: String, zkHost: String, query: Option[SolrQuery]): Long = {
    val solrQuery = if (query.isDefined) query.get else new SolrQuery().setQuery("*:*")
    val cloneQuery = solrQuery.getCopy
    cloneQuery.set("distrib", "true")
    cloneQuery.setRows(0)
    val cloudClient = SolrSupport.getCachedCloudClient(zkHost)
    val response = cloudClient.query(collection, solrQuery)
    response.getResults.getNumFound
  }

  /*
    Return solr field types along with their actual class types.
    E.g. { "binary": "solr.BinaryField",
           "boolean": "solr.BooleanField"
         }
   */
  def getFieldTypeToClassMap(solrUrl: String) : Map[String, String] = {
    val fieldTypeToClassMap: mutable.Map[String, String] = new mutable.HashMap[String, String]
    val fieldTypeUrl = solrUrl + "schema/fieldtypes"
    try {
      val fieldTypeMeta = SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient, fieldTypeUrl, 2)
      if (fieldTypeMeta.has("fieldTypes")) {
        (fieldTypeMeta \ "fieldTypes").values match {
          case types: List[Any] =>
            if (types.nonEmpty) {
              // Get the name, type and add them to the map
              types.foreach {
                case m: Map[_, _] if m.keySet.forall(_.isInstanceOf[String])=>
                  val fieldTypePayload = m.asInstanceOf[Map[String, Any]]
                  if (fieldTypePayload.contains("name") && fieldTypePayload.contains("class")) {
                    val fieldTypeName = fieldTypePayload.get("name")
                    val fieldTypeClass = fieldTypePayload.get("class")
                    if (fieldTypeName.isDefined && fieldTypeClass.isDefined) {
                      fieldTypeName.get match {
                        case name: String =>
                          fieldTypeClass.get match {
                            case typeClass: String =>
                              fieldTypeToClassMap.put(name, typeClass)
                          }
                      }
                    }
                  }
              }
            }
          case t: AnyRef => log.warn("Found unexpected object type '" + t + "' when parsing field types json")
        }
      }
    } catch {
      case e: Exception =>
        log.error("Can't get field type metadata from Solr url " + fieldTypeUrl)
        e match {
          case e1: RuntimeException => throw e1
          case e2: Exception => throw new RuntimeException(e2)
        }
    }

    fieldTypeToClassMap.toMap
  }

  def splitShard(
      sc: SparkContext,
      query: SolrQuery,
      shards: List[String],
      splitFieldName: String,
      splitsPerShard: Int,
      collection: String): RDD[ShardSplit[_]] = {

    // get field type of split field
    var fieldDataType: Option[DataType] = None
    if ("_version_".equals(splitFieldName)) {
      fieldDataType = Some(DataTypes.LongType)
    } else {
      val fieldMetaMap = getFieldTypes(Set(splitFieldName), shards.get(0))
      if (fieldMetaMap.contains(splitFieldName)) {
        if (fieldMetaMap.get(splitFieldName).isDefined) {
          val solrFieldMeta = fieldMetaMap.get(splitFieldName).get
          if (solrFieldMeta.fieldTypeClass.isDefined) {
            if (SOLR_DATA_TYPES.contains(solrFieldMeta.fieldTypeClass.get)) {
              fieldDataType = Some(SOLR_DATA_TYPES.get(solrFieldMeta.fieldTypeClass.get).get)
            } else {
              log.warn("Cannot find spark type for solr field class '" + solrFieldMeta.fieldTypeClass.get + "'. " +
                "Assuming it is a String!. The types dict is " + SOLR_DATA_TYPES)
              fieldDataType = Some(DataTypes.StringType)
            }
          } else {
            log.warn("No field type class found for " + splitFieldName + ", assuming it is a String!")
            fieldDataType = Some(DataTypes.StringType)
          }
        } else {
          log.warn("No field metadata found for " + splitFieldName + ", assuming it is a String!")
          fieldDataType = Some(DataTypes.StringType)
        }
      } else {
        throw new IllegalArgumentException("Cannot find split field '" + splitFieldName + "' in the solr field meta")
      }
    }

    // For each shard, get the list of splits and flat map the list of lists
    sc.parallelize(shards, shards.size).flatMap(shardUrl => {
      var splitStrategy: Option[ShardSplitStrategy] = None
      if (fieldDataType.isEmpty) {
        throw new Exception("data type for field '" + splitFieldName + "' not defined")
      }
      if (fieldDataType.get == DataTypes.LongType || fieldDataType.get == DataTypes.IntegerType) {
        splitStrategy = Some(new NumberFieldShardSplitStrategy)
      } else if (fieldDataType.get == DataTypes.StringType) {
        splitStrategy == Some(new StringFieldShardSplitStrategy)
      }

      if (splitStrategy.isEmpty) {
        throw new IllegalArgumentException("Can only split shards on fields of type: long, int, or string!. The field '" + splitFieldName + "' has field type '" + fieldDataType)
      }

      val splits = splitStrategy.get.getSplits(shardUrl, query, splitFieldName, splitsPerShard)
      log.info("Found " + splits.size + " splits for " + splitFieldName + ": " + splits)

      splits.toList
    })
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
      solrRDD: SolrRDD,
      escapeFieldNames: Boolean): DataFrame = {
    val schema = SolrRelationUtil.getBaseSchema(solrRDD.zkHost, solrRDD.collection, escapeFieldNames, true)
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

}
