package com.lucidworks.spark.port

import com.lucidworks.spark.SolrConf
import com.lucidworks.spark.util.{SolrSupport, SolrQuerySupport, SolrSchemaUtil}
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import org.apache.spark.sql.sources._
import com.lucidworks.spark.util.ConfigurationConstants.SOLR_COLLECTION_PARAM
import com.lucidworks.spark.util.ConfigurationConstants.SOLR_ZK_HOST_PARAM

import scala.util.control.Breaks._

class SolrRelation(val parameters: Map[String, String],
                   override val sqlContext: SQLContext,
                   val dataFrame: Option[DataFrame])
                  (implicit val conf: SolrConf = new SolrConf(parameters))
  extends BaseRelation with TableScan with PrunedFilteredScan with InsertableRelation with Logging {

  def this(parameters: Map[String, String], sqlContext: SQLContext) {
    this(parameters, sqlContext, None)
  }

  checkRequiredParams()
  val sc: SparkContext = sqlContext.sparkContext
  val solrRDD: SolrRDD = new SolrRDD(conf.getZKHost, conf.getCollection, sc)
  val baseSchema: StructType = SolrSchemaUtil.getBaseSchema(conf.getZKHost, conf.getCollection, conf.escapeFieldNames())
  val query: SolrQuery = buildQuery
  val querySchema: StructType = if (dataFrame.isDefined) dataFrame.get.schema else SolrSchemaUtil.deriveQuerySchema(query.getFields.split(","), baseSchema)

  override def schema: StructType = querySchema

  override def buildScan(): RDD[Row] = buildScan(null, null)

  override def buildScan(fields: Array[String], filters: Array[Filter]): RDD[Row] = {

    if (fields != null && fields.length > 0) {
      fields.zipWithIndex.foreach({ case (field, i) => fields(i) = field.replaceAll("`", "")})
      query.setFields(fields:_*)
    }

    // Add aliasing to the fields. Why ?? TODO: Examples and tests on how this works
    if (query.getFields != null && query.getFields.length > 0) {
      SolrSchemaUtil.setAliases(query.getFields.split(","), query, baseSchema)
    }

    // Clear all existing filters
    if (!filters.isEmpty) {
      query.remove("fq")
      filters.foreach(filter => SolrSchemaUtil.applyFilter(filter, query, baseSchema))
    }

    if (log.isInfoEnabled)
      log.info("Constructed SolrQuery: " + query)

    try {
      val querySchema = if (!fields.isEmpty) SolrSchemaUtil.deriveQuerySchema(fields, baseSchema) else schema
      val docs = solrRDD.query(query)
      SolrSchemaUtil.toRows(querySchema, docs)
    } catch {
      case e: Throwable => throw new RuntimeException(e)
    }
  }

  override def insert(df: DataFrame, overwrite: Boolean): Unit = {
    val docs = df.rdd.map(row => {
      val schema: StructType = row.schema
      val doc = new SolrInputDocument
      schema.fields.foreach(field => {
        val fname = field.name
        breakable {
          if (fname.equals("_version"))
            break()
        }
        val fieldIndex = row.fieldIndex(fname)
        val fieldValue : Option[Any] = if (row.isNullAt(fieldIndex)) None else Some(row.get(fieldIndex))
        if (fieldValue.isDefined) {
          val value = fieldValue.get
          value match {
            //TODO: Do we need to check explicitly for ArrayBuffer and WrappedArray
            case Iterable => {
              val it = value.asInstanceOf[Iterable[Any]].iterator
              while (it.hasNext) doc.addField(fname, it.next())
            }
            case _ => doc.setField(fname, value)
          }
        }
      })
      doc
    })
    SolrSupport.indexDocs(solrRDD.zkHost, solrRDD.collection, 100, docs) // TODO: Make the numDocs configurable
  }

  private def buildQuery: SolrQuery = {
    val query = SolrQuerySupport.toQuery(conf.getQuery)
    val fields = conf.getFieldList

    if (fields.nonEmpty) {
      query.setFields(fields:_*)
    } else {
      // TODO: Is this really necessary?
      SolrSchemaUtil.applyDefaultFields(baseSchema, query)
    }

    query.setRows(conf.getRows)
    query.add(conf.getSolrParams)
    query.set("collection", conf.getCollection)
    query
  }

  private def checkRequiredParams(): Unit = {
    if (conf.getZKHost == null)
      throw new IllegalArgumentException("Param '" + SOLR_ZK_HOST_PARAM + "' is required")
    if (conf.getCollection == null)
      throw new IllegalArgumentException("Param '" + SOLR_COLLECTION_PARAM + "' is required")
  }
}

