package com.lucidworks.spark.rdd

import java.net.InetAddress

import com.lucidworks.spark.query.StreamingResultsIterator
import com.lucidworks.spark.util.{SolrQuerySupport, SolrSupport}
import com.lucidworks.spark.{ShardPartitioner, SolrRDDPartition, SolrShard}
import com.lucidworks.spark.util.QueryConstants._
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.SolrDocument
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters
import scala.util.Random

/**
 * TODO: Add support for filter queries and be able to pass in SolrQuery object
 */
class SolrRDD(
    val zkHost: String,
    val collection: String,
    @transient val sc: SparkContext,
    val query : Option[String] = Option(DEFAULT_QUERY),
    val fields: Option[Array[String]] = None,
    val rows: Option[Int] = Option(DEFAULT_PAGE_SIZE),
    val splitField: Option[String] = None,
    val splitsPerShard: Option[Int] = Option(DEFAULT_SPLITS_PER_SHARD),
    val solrQuery: Option[SolrQuery] = None)
  extends RDD[SolrDocument](sc, Seq.empty) with Logging{ //TODO: Do we need to pass any deps on parent RDDs for Solr?

  val uniqueKey = SolrQuerySupport.getUniqueKey(zkHost, collection)

  protected def copy(
    query: Option[String] = query,
    fields: Option[Array[String]] = fields,
    rows: Option[Int] = rows,
    splitField: Option[String] = splitField,
    splitsPerShard: Option[Int] = splitsPerShard,
    solrQuery: Option[SolrQuery] = solrQuery): SolrRDD = {
    new SolrRDD(zkHost, collection, sc, query, fields, rows, splitField, splitsPerShard, solrQuery)
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[SolrDocument] = {
    val rddPartition = split.asInstanceOf[SolrRDDPartition]
    val taskHostName = context.taskMetrics().hostname
    log.info("Computing the partition " + rddPartition.index + "' on host name " + taskHostName)

    // TODO: Define primary, backups as a definition for getting data locality instead of relying on taskHostName

    // Use taskHostName to see if any of the Solr replicas are available on this machine
    var replicaUrl: Option[String] = None
    val addresses: Array[InetAddress] = getAllAddresses(taskHostName)
    log.debug("InetAddresses of host name for partition '" + split.index + "' are " + addresses.mkString(" "))
    rddPartition.solrShard.replicas.foreach(f => {
      log.debug("Replica addresses for partition '" + "' are " + f.locations.mkString(" "))
      if (addresses.intersect(f.locations).length > 0) {
        log.info("Found a replica on the same node as executor for partition '" + split.index + " '. Location " + addresses.intersect(f.locations).mkString(" "))
        replicaUrl = Some(f.replicaLocation)
      }
    })

    //TODO: Add backup mechanism to StreamingResultsIterator by being able to query any replica in case the main url goes down
    val shardUrl = replicaUrl.getOrElse(SolrRDD.randomReplicaLocation(rddPartition.solrShard))
    log.info("Using the shard url " + shardUrl + " for getting partition data")
    val streamingIterator = new StreamingResultsIterator(
      SolrSupport.getHttpSolrClient(shardUrl),
      rddPartition.query,
      rddPartition.cursorMark)

    context.addTaskCompletionListener { (context) =>
      log.info(f"Fetched ${streamingIterator.getNumDocs} rows from shard $shardUrl for partition ${split.index}")
    }
    JavaConverters.asScalaIteratorConverter(streamingIterator.iterator()).asScala
  }

  override protected def getPartitions: Array[Partition] = {
    val shards = SolrSupport.buildShardList(zkHost, collection)
    val query = if (solrQuery.isEmpty) buildQuery else solrQuery.get
    if (splitField.isDefined)
      ShardPartitioner.getSplitPartitions(shards, query, splitField.get, splitsPerShard.get)
    else
      ShardPartitioner.getShardPartitions(shards, query)
  }

  //TODO: Implement this and return the list of replicas. How to co-ordinate the shard url between this and compute method
    override def getPreferredLocations(split: Partition): Seq[String] = {
    val urls: Seq[String] = Seq.empty
    split.asInstanceOf[SolrRDDPartition].solrShard.replicas.foreach(f => urls + f.replicaHostName)
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

  def query(q: String): SolrRDD = {
    copy(query = Option(q))
  }

  def query(solrQuery: SolrQuery): SolrRDD = {
    copy(solrQuery = Option(solrQuery))
  }

  def select(fl: String): SolrRDD = {
    copy(fields = Some(fl.split(",")))
  }

  def select(fl: Array[String]): SolrRDD = {
    copy(fields = Some(fl))
  }

  def rows(rows: Int): SolrRDD = {
    copy(rows = Some(rows))
  }

  def splitField(field: String): SolrRDD = {
    copy(splitField = Some(field))
  }

  def splitsPerShard(splitsPerShard: Int): SolrRDD = {
    copy(splitsPerShard = Some(splitsPerShard))
  }

  def buildQuery: SolrQuery = {
    var solrQuery : SolrQuery = SolrQuerySupport.toQuery(query.get)
    //TODO: Remove null and replace with Option and None
    if (!solrQuery.getFields.eq(null) && solrQuery.getFields.length > 0)
      solrQuery = solrQuery.setFields(fields.getOrElse(Array.empty[String]):_*)
    if (!solrQuery.getRows.eq(null))
      solrQuery = solrQuery.setRows(rows.get)

    solrQuery.set("collection", collection)
    solrQuery.set("distrib", "false")
    solrQuery.setStart(0)

    if (solrQuery.getSortField == null || solrQuery.getSortField.isEmpty)
      solrQuery = solrQuery.addSort(SolrQuery.SortClause.asc(uniqueKey))

    solrQuery
  }

}

object SolrRDD {

  def randomReplicaLocation(solrShard: SolrShard): String = {
    solrShard.replicas(Random.nextInt(solrShard.replicas.size)).replicaLocation
  }
}

