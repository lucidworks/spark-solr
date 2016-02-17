package com.lucidworks.spark.port.util

import java.nio.charset.StandardCharsets
import java.util

import com.lucidworks.spark.port.SolrRDD
import com.lucidworks.spark.query.{PagedResultsIterator, ShardSplit}
import com.lucidworks.spark.util.{SolrJsonSupport, QueryConstants}
import org.apache.http.client.utils.URLEncodedUtils
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.client.solrj.{StreamingResponseCallback, SolrClient, SolrQuery}
import org.apache.solr.common.{SolrDocument, SolrException}
import org.apache.solr.common.params.{SolrParams, ModifiableSolrParams}
import org.apache.solr.common.util.NamedList
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.sql.types.{StructField, StructType, DataTypes, DataType}

import scala.collection.immutable.HashMap
import scala.collection.JavaConversions._

case class SolrFieldMeta(fieldType: String,
                          dynamicBase: String,
                          isRequired: Boolean,
                          isMultiValued: Boolean,
                          isDocValues: Boolean,
                          isStored: Boolean,
                          fieldTypeClass: String)

class PivotField(solrField: String,
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

class QueryResultsIterator(solrClient: SolrClient,
                           solrQuery: SolrQuery,
                           cursorMark: String) extends PagedResultsIterator[SolrDocument](solrClient, solrQuery, cursorMark) {
  override protected def processQueryResponse(resp: QueryResponse): util.List[SolrDocument] = resp.getResults
}

object SolrQuerySupport extends Logging {
  val SOLR_DATA_TYPES: Map[String, DataType] = HashMap(
    "solr.StrField" -> DataTypes.StringType,
    "solr.TextField" -> DataTypes.StringType,
    "solr.BoolField" -> DataTypes.BooleanType,
    "solr.TrieIntField" -> DataTypes.IntegerType,
    "solr.TrieLongField" -> DataTypes.LongType,
    "solr.TrieFloatField" -> DataTypes.FloatType,
    "solr.TrieDoubleField" -> DataTypes.DoubleType,
    "solr.TrieDateField" -> DataTypes.TimestampType
  )

  def getUniqueKey(zkHost: String, collection: String): String = {
    try {
      val solrBaseUrl = SolrSupportScala.getSolrBaseUrl(zkHost)
      // Hit Solr Schema API to get base information
      val schemaUrl: String = solrBaseUrl + collection + "/schema"
      try {
        val schemaMeta = SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient, schemaUrl, 2)
        return SolrJsonSupport.asString("/schema/uniqueKey", schemaMeta)
      }
      catch {
        case solrExc: SolrException => {
          log.warn("Can't get uniqueKey for " + collection + " due to solr: " + solrExc)
        }
      }
    } catch {
      case e: Exception => log.warn("Can't get uniqueKey for " + collection + " due to: " + e)
    }
    QueryConstants.DEFAULT_REQUIRED_FIELD
  }

  def toQuery(queryString: String): SolrQuery = {

    val solrQuery: SolrQuery = new SolrQuery
    if (queryString == null || queryString.isEmpty) {
      solrQuery.setQuery("*:*")
    } else {
      if (queryString.contains("=")) {
        // no name-value pairs ... just assume this single clause is the q part
        solrQuery.setQuery(queryString)
      } else {
        val params = new NamedList[Object]()
        for(nvp <- URLEncodedUtils.parse(queryString, StandardCharsets.UTF_8)) {
          val value = nvp.getValue
          if (value != null && value.length > 0) {
            val name = nvp.getName
            if ("sort".equals(name)) {
              if (value.contains(" ")) {
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

      val rows = solrQuery.getRows
      if (rows == null)
        solrQuery.setRows(QueryConstants.DEFAULT_PAGE_SIZE)
    }
    solrQuery
  }

  def addDefaultSort(solrQuery: SolrQuery, uniqueKey: String): Unit = {
    if (solrQuery.getSortField == null || solrQuery.getSortField.isEmpty)
      solrQuery.addSort(SolrQuery.SortClause.asc(uniqueKey))
  }

  def querySolr(solrClient: SolrClient,
                solrQuery: SolrQuery,
                startIndex: Int,
                cursorMark: String): QueryResponse = ???


  def querySolr(solrClient: SolrClient,
                solrQuery: SolrQuery,
                startIndex: Int,
                cursorMark: String,
                callback: StreamingResponseCallback): QueryResponse = ???

  def setQueryDefaultsForShards(solrQuery: SolrQuery, uniqueKey: String) = {
    solrQuery.set("distrib", "false")
    solrQuery.setStart(0)
    if (solrQuery.getRows == null)
      solrQuery.setRows(QueryConstants.DEFAULT_PAGE_SIZE)
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
    if (solrQuery.getRows == null) solrQuery.setRows(QueryConstants.DEFAULT_PAGE_SIZE)

    SolrQuerySupport.addDefaultSort(solrQuery, uniqueKey)
  }

  def getFieldTypes(fields: Array[String],
                     solrBaseUrl: String,
                     collection: String): Map[String, SolrFieldMeta] = ???

  def getFieldTypes(fields: Array[String],
                    solrUrl: String): Map[String, SolrFieldMeta] = ???

  def splitShard(sc: SparkContext,
                  query: SolrQuery,
                  shards: List[String],
                  splitFieldName: String,
                  splitsPerShard: Int,
                  collection: String): RDD[ShardSplit] = ???

  def getPivotFieldRange(schema: StructType, pivotPrefix: String): Array[Integer] = ???

  def fillPivotFieldValues(rawValue: String, row: Array[Object], schema: StructType, pivotPrefix: String) = ???

  /**
   * Allows you to pivot a categorical field into multiple columns that can be aggregated into counts, e.g.
   * a field holding HTTP method (http_verb=GET) can be converted into: http_method_get=1, which is a common
   * task when creating aggregations.
   */
  def withPivotFields(solrData: DataFrame,
                      pivotFields: Array[PivotField],
                      solrRDD: SolrRDD,
                      escapeFieldNames: Boolean): DataFrame = ???

  def toPivotSchema(baseSchema: StructType,
                     pivotFields: Array[PivotField],
                     collection: String,
                     schema: StructType,
                     uniqueKey: String,
                     zkHost: String): StructType = ???

  def getPivotSchema(fieldName: String,
                      maxCols: Int,
                      fieldPrefix: String,
                      otherName: String,
                      collection: String,
                      schema: StructType,
                      uniqueKey: String,
                      zkHost: String): List[StructField] = ???

}
