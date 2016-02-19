package com.lucidworks.spark

import com.lucidworks.spark.util.{SolrSupport, SolrQuerySupport, SolrSchemaUtil}
import com.lucidworks.spark.rdd.SolrRDD
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.Logging

import scala.util.control.Breaks._
import com.lucidworks.spark.util.QueryConstants._
import com.lucidworks.spark.util.ConfigurationConstants._

class SolrRelation(val parameters: Map[String, String],
                   override val sqlContext: SQLContext,
                   val dataFrame: Option[DataFrame])
                  (implicit val conf: SolrConf = new SolrConf(parameters))
  extends BaseRelation with TableScan with PrunedFilteredScan with InsertableRelation with Logging {

  def this(parameters: Map[String, String], sqlContext: SQLContext) {
    this(parameters, sqlContext, None)
  }

  checkRequiredParams()
  val sc = sqlContext.sparkContext
  val solrRDD = {
    var rdd = new SolrRDD(conf.getZkHost.get, conf.getCollection.get, sc)

    if (conf.splits.isDefined && conf.getSplitsPerShard.isDefined)
      rdd = rdd.doSplits().splitsPerShard(conf.getSplitsPerShard.get)
    else if (conf.splits.isDefined)
      rdd = rdd.doSplits()

    if (conf.getSplitField.isDefined && conf.getSplitsPerShard.isDefined)
      rdd = rdd.splitField(conf.getSplitField.get).splitsPerShard(conf.getSplitsPerShard.get)
    else if (conf.getSplitField.isDefined)
      rdd = rdd.splitField(conf.getSplitField.get)

    rdd
  }

  val baseSchema: StructType = SolrSchemaUtil.getBaseSchema(conf.getZkHost.get, conf.getCollection.get, conf.escapeFieldNames.getOrElse(false))
  val query: SolrQuery = buildQuery
  val querySchema: StructType = if (dataFrame.isDefined) dataFrame.get.schema else SolrSchemaUtil.deriveQuerySchema(query.getFields.split(","), baseSchema)

  override def schema: StructType = querySchema

  override def buildScan(): RDD[Row] = buildScan(null, null)

  override def buildScan(fields: Array[String], filters: Array[Filter]): RDD[Row] = {

    if (fields != null && fields.length > 0) {
      fields.zipWithIndex.foreach({ case (field, i) => fields(i) = field.replaceAll("`", "")})
      query.setFields(fields:_*)
    }

    // We set aliasing to retrieve docValues from function queries. This can be removed after Solr version 5.5 is released
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
            case v: Iterable[Any] => {
              val it = v.iterator
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
    val query = SolrQuerySupport.toQuery(conf.getQuery.getOrElse("*:*"))
    val fields = conf.getFields

    if (fields.nonEmpty) {
      query.setFields(fields:_*)
    } else {
      // We add all the defaults fields to retrieve docValues that are not stored. We should remove this after 5.5 release
      SolrSchemaUtil.applyDefaultFields(baseSchema, query)
    }

    query.setRows(scala.Int.box(conf.getRows.getOrElse(DEFAULT_PAGE_SIZE)))
    query.add(conf.solrConfigParams)
    query.set("collection", conf.getCollection.get)
    query
  }

  private def checkRequiredParams(): Unit = {
    require(conf.getZkHost.isDefined, "Param '" + SOLR_ZK_HOST_PARAM + "' is required")
    require(conf.getCollection.isDefined, "Param '" + SOLR_COLLECTION_PARAM + "' is required")
  }
}

