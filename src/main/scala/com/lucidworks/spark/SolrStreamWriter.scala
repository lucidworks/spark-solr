package com.lucidworks.spark

import com.lucidworks.spark.util.{SolrQuerySupport, SolrSupport}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode
import com.lucidworks.spark.util.ConfigurationConstants._
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

/**
  * Writes a Spark stream to Solr
  * @param sparkSession
  * @param parameters
  * @param partitionColumns
  * @param outputMode
  * @param solrConf
  */
class SolrStreamWriter(
    val sparkSession: SparkSession,
    parameters: Map[String, String],
    val partitionColumns: Seq[String],
    val outputMode: OutputMode)(
  implicit val solrConf : SolrConf = new SolrConf(parameters))
  extends Sink with LazyLogging {

  require(solrConf.getZkHost.isDefined, s"Parameter ${SOLR_ZK_HOST_PARAM} not defined")
  require(solrConf.getCollection.isDefined, s"Parameter ${SOLR_COLLECTION_PARAM} not defined")

  val collection : String = solrConf.getCollection.get
  val zkhost: String = solrConf.getZkHost.get

  lazy val solrVersion : String = SolrSupport.getSolrVersion(solrConf.getZkHost.get)
  lazy val uniqueKey: String = SolrQuerySupport.getUniqueKey(zkhost, collection.split(",")(0))

  lazy val dynamicSuffixes: Set[String] = SolrQuerySupport.getFieldTypes(
      Set.empty,
      SolrSupport.getSolrBaseUrl(zkhost),
      SolrSupport.getCachedCloudClient(zkhost),
      collection,
      skipDynamicExtensions = false)
    .keySet
    .filter(f => f.startsWith("*_") || f.endsWith("_*"))
    .map(f => if (f.startsWith("*_")) f.substring(1) else f.substring(0, f.length-1))

  @volatile private var latestBatchId: Long = -1L
  val acc: SparkSolrAccumulator = new SparkSolrAccumulator
  val accName = if (solrConf.getAccumulatorName.isDefined) solrConf.getAccumulatorName.get else "Records Written"
  sparkSession.sparkContext.register(acc, accName)
  SparkSolrAccumulatorContext.add(accName, acc.id)

  override def addBatch(batchId: Long, df: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      logger.info(s"Skipping already processed batch $batchId")
    } else {
      val rows = df.collect()
      if (rows.nonEmpty) {
        val schema: StructType = df.schema
        val solrClient = SolrSupport.getCachedCloudClient(zkhost)

        // build up a list of updates to send to the Solr Schema API
        val fieldsToAddToSolr = SolrRelation.getFieldsToAdd(schema, solrConf, solrVersion, dynamicSuffixes)

        if (fieldsToAddToSolr.nonEmpty) {
          SolrRelation.addFieldsForInsert(fieldsToAddToSolr, collection, solrClient)
        }

        val solrDocs = rows.toStream.map(row => SolrRelation.convertRowToSolrInputDocument(row, solrConf, uniqueKey))
        acc.add(solrDocs.length.toLong)
        SolrSupport.sendBatchToSolrWithRetry(zkhost, solrClient, collection, solrDocs, solrConf.commitWithin)
        logger.info(s"Written ${solrDocs.length} documents to Solr collection $collection from batch $batchId")
        latestBatchId = batchId
      }
    }
  }
}
