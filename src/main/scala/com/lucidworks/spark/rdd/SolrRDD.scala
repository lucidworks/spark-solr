package com.lucidworks.spark.rdd

import java.net.InetAddress

import com.lucidworks.spark.query.{ResultsIterator, SolrStreamIterator, StreamingResultsIterator}
import com.lucidworks.spark.util.{SolrQuerySupport, SolrSupport}
import com.lucidworks.spark._
import com.lucidworks.spark.util.QueryConstants._
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.SolrDocument
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters
import scala.util.Random

class SolrRDD(
    val zkHost: String,
    val collection: String,
    @transient sc: SparkContext,
    exportHandler: Option[Boolean] = None,
    query : Option[String] = Option(DEFAULT_QUERY),
    fields: Option[Array[String]] = None,
    rows: Option[Int] = Option(DEFAULT_PAGE_SIZE),
    splitField: Option[String] = None,
    splitsPerShard: Option[Int] = Option(DEFAULT_SPLITS_PER_SHARD),
    solrQuery: Option[SolrQuery] = None)
  extends RDD[SolrDocument](sc, Seq.empty)
  with Logging {

  val uniqueKey = SolrQuerySupport.getUniqueKey(zkHost, collection)

  protected def copy(
      exportHandler: Option[Boolean] = exportHandler,
      query: Option[String] = query,
      fields: Option[Array[String]] = fields,
      rows: Option[Int] = rows,
      splitField: Option[String] = splitField,
      splitsPerShard: Option[Int] = splitsPerShard,
      solrQuery: Option[SolrQuery] = solrQuery): SolrRDD = {
    new SolrRDD(zkHost, collection, sc, exportHandler, query, fields, rows, splitField, splitsPerShard, solrQuery)
  }

  /*
  * Get an Iterator that uses the export handler in Solr
  */
  @throws(classOf[Exception])
  private def getExportHandlerBasedIterator(shardUrl : String, query : SolrQuery) = {

    // Direct the queries to each shard, so we don't want distributed
    query.set("distrib", false)
    new SolrStreamIterator(shardUrl, SolrSupport.getHttpSolrClient(shardUrl), query)
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[SolrDocument] = {
    split match {
      case partition: SolrRDDPartition =>
        log.info("Computing the partition " + partition.index + " on host name " + context.taskMetrics().hostname)

        //TODO: Add backup mechanism to StreamingResultsIterator by being able to query any replica in case the main url goes down
        val url = partition.preferredReplica.replicaUrl
        val query = partition.query
        log.info("Using the shard url " + url + " for getting partition data for split: "+split)
        val resultsIterator: ResultsIterator =
          if (exportHandler.isDefined && exportHandler.get)
            getExportHandlerBasedIterator(url, query)
          else
            new StreamingResultsIterator(
              SolrSupport.getHttpSolrClient(url),
              partition.query,
              partition.cursorMark)

        context.addTaskCompletionListener { (context) =>
          log.info(f"Fetched ${resultsIterator.getNumDocs} rows from shard $url for partition ${split.index}")
        }
        JavaConverters.asScalaIteratorConverter(resultsIterator.iterator()).asScala

      case partition: AnyRef => throw new Exception("Unknown partition type '" + partition.getClass)
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val shards = SolrSupport.buildShardList(zkHost, collection)
    val query = if (solrQuery.isEmpty) buildQuery else solrQuery.get
    // Add defaults for shards. TODO: Move this for different implementations (Streaming)
    if (!(exportHandler.isDefined && exportHandler.get))
      SolrQuerySupport.setQueryDefaultsForShards(query, uniqueKey)
    val partitions = if (splitField.isDefined)
      SolrPartitioner.getSplitPartitions(shards, query, splitField.get, splitsPerShard.get) else SolrPartitioner.getShardPartitions(shards, query)
    if (log.isDebugEnabled)
      log.debug(s"Found ${partitions.length} partitions: ${partitions.mkString(",")}")
    partitions
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val urls: Seq[String] = Seq.empty
    split match {
      case partition: SolrRDDPartition => Array(partition.preferredReplica.replicaHostName)
      case partition: AnyRef => log.warn("Unknown partition type '" + partition.getClass + "'")
    }
    urls
  }

  private def getAllAddresses(hostName: String): Array[InetAddress] = {
    try {
      return InetAddress.getAllByName(hostName)
    } catch {
      case e: Exception => log.info("Exception while resolving IP address for host name '" + hostName + "' with exception " + e)
    }
    Array.empty[InetAddress]
  }

  def query(q: String): SolrRDD = copy(query = Option(q))

  def query(solrQuery: SolrQuery): SolrRDD = copy(solrQuery = Option(solrQuery))

  def select(fl: String): SolrRDD = copy(fields = Some(fl.split(",")))

  def select(fl: Array[String]): SolrRDD = copy(fields = Some(fl))

  def rows(rows: Int): SolrRDD = copy(rows = Some(rows))

  def doSplits(): SolrRDD = copy(splitField = Some(DEFAULT_SPLIT_FIELD))

  def splitField(field: String): SolrRDD = copy(splitField = Some(field))

  def splitsPerShard(splitsPerShard: Int): SolrRDD = copy(splitsPerShard = Some(splitsPerShard))

  def useExportHandler: SolrRDD = copy(exportHandler = Some(true))

  def solrCount: BigInt = SolrQuerySupport.getNumDocsFromSolr(collection, zkHost, solrQuery)

  def buildQuery: SolrQuery = {
    var solrQuery : SolrQuery = SolrQuerySupport.toQuery(query.get)
    if (!solrQuery.getFields.eq(null) && solrQuery.getFields.length > 0) {
      solrQuery = solrQuery.setFields(fields.getOrElse(Array.empty[String]):_*)
    }
    if (!solrQuery.getRows.eq(null)) {
      solrQuery = solrQuery.setRows(rows.get)
    }

    solrQuery.set("collection", collection)
    solrQuery
  }

}

object SolrRDD {

  def randomReplicaLocation(solrShard: SolrShard): String = {
    randomReplica(solrShard).replicaUrl
  }

  def randomReplica(solrShard: SolrShard): SolrReplica = {
    solrShard.replicas(Random.nextInt(solrShard.replicas.size))
  }

  def apply(zkHost: String, collection: String, sc: SparkContext) =
    new SolrRDD(zkHost, collection, sc)

}

