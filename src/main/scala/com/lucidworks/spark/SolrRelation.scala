package com.lucidworks.spark

import com.lucidworks.spark.util.{SolrQuerySupport, SolrSchemaUtil}
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import org.apache.spark.sql.sources._
import com.lucidworks.spark.util.ConfigurationConstants.SOLR_COLLECTION_PARAM
import com.lucidworks.spark.util.ConfigurationConstants.SOLR_ZK_HOST_PARAM

class SolrRelation(parameters: Map[String, String],
                   override val sqlContext: SQLContext)
                  (implicit val conf: SolrConf = new SolrConf(parameters))
  extends BaseRelation with TableScan with PrunedFilteredScan with InsertableRelation with Logging {

  checkRequiredParams()
  val sc: SparkContext = sqlContext.sparkContext
  val solrRDD: SolrScalaRDD = new SolrScalaRDD(conf.getZKHost, conf.getCollection, sc)
  val baseSchema: StructType = SolrSchemaUtil.getBaseSchema(conf.getZKHost, conf.getCollection, conf.escapeFieldNames())
  val query: SolrQuery = buildQuery
  val schema: StructType = SolrSchemaUtil.deriveQuerySchema(query.getFields.split(","), baseSchema)

  override def schema: StructType = ???

  override def buildScan(): RDD[Row] = buildScan(null, null)

  override def buildScan(fields: Array[String], filters: Array[Filter]): RDD[Row] = {

    if (fields != null && fields.length > 0) {
      fields.zipWithIndex.foreach({ case (field, i) => fields(i) = field.replaceAll("`", "")})
      query.setFields(fields:_*)
    }

    // Add aliasing to the fields
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
    df.rdd.map(row => {
      val schema: StructType = row.schema
      val doc = new SolrInputDocument
      for (field: StructField <- schema.fields) {
        val fname = field.name
        if (fname.equals("_version"))

      }
    })
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

