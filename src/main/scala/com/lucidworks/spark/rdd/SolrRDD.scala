package com.lucidworks.spark.rdd

import com.lucidworks.spark._
import com.lucidworks.spark.util.QueryConstants._
import com.lucidworks.spark.util.{CacheCloudSolrClient, CacheHttpSolrClient, SolrQuerySupport}
import org.apache.solr.client.solrj.SolrQuery
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerEvent}

import scala.reflect.ClassTag
import scala.util.Random

abstract class SolrRDD[T: ClassTag](
    val zkHost: String,
    val collection: String,
    @transient private val sc: SparkContext,
    requestHandler: Option[String] = None,
    query : Option[String] = None,
    fields: Option[Array[String]] = None,
    rows: Option[Int] = None,
    splitField: Option[String] = None,
    splitsPerShard: Option[Int] = None,
    solrQuery: Option[SolrQuery] = None,
    uKey: Option[String] = None)
  extends RDD[T](sc, Seq.empty)
  with LazyLogging {

  sparkContext.addSparkListener(new SparkFirehoseListener() {
    override def onEvent(event: SparkListenerEvent): Unit = event match {
        case e: SparkListenerApplicationEnd =>
          logger.debug(s"Invalidating cloud client and http client caches for event ${e}")
          CacheCloudSolrClient.cache.invalidateAll()
          CacheHttpSolrClient.cache.invalidateAll()
        case _ =>
    }
  })

  val uniqueKey: String = if (uKey.isDefined) uKey.get else SolrQuerySupport.getUniqueKey(zkHost, collection.split(",")(0))

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split match {
      case partition: SelectSolrRDDPartition => Array(partition.preferredReplica.getHostAndPort())
      case partition: ExportHandlerPartition => Array(partition.preferredReplica.getHostAndPort())
      case _: AnyRef => Seq.empty
    }
  }

  def buildQuery: SolrQuery

  def query(q: String): SolrRDD[T]

  def query(solrQuery: SolrQuery): SolrRDD[T]

  def select(fl: String): SolrRDD[T]

  def select(fl: Array[String]): SolrRDD[T]

  def rows(rows: Int): SolrRDD[T]

  def doSplits(): SolrRDD[T]

  def splitField(field: String): SolrRDD[T]

  def splitsPerShard(splitsPerShard: Int): SolrRDD[T]

  def requestHandler(requestHandler: String): SolrRDD[T]

  def solrCount: BigInt = SolrQuerySupport.getNumDocsFromSolr(collection, zkHost, solrQuery)

  def getReplicaToQuery(partition: SolrRDDPartition, attempt_no: Int): String = {
    val preferredReplicaUrl = partition.preferredReplica.replicaUrl
    if (attempt_no == 0)
      preferredReplicaUrl
    else {
      logger.info(s"Task attempt no. ${attempt_no}. Checking if replica ${preferredReplicaUrl} is healthy")
      // can't do much if there is only one replica for this shard
      if (partition.solrShard.replicas.length == 1)
        return preferredReplicaUrl

      // Switch to another replica as the task has failed
      val newReplicaToQuery = SolrRDD.randomReplica(partition.solrShard, partition.preferredReplica)
      logger.info(s"Switching from $preferredReplicaUrl to ${newReplicaToQuery.replicaUrl}")
      partition.preferredReplica = newReplicaToQuery
      newReplicaToQuery.replicaUrl
    }
  }

  def calculateSplitsPerShard(solrQuery: SolrQuery, shardSize: Int, replicaSize: Int, docsPerTask: Int = 10000, maxRows: Option[Int] = None): Int = {
    val minSplitSize = 2 * replicaSize
    if (maxRows.isDefined) return minSplitSize
    val noOfDocs = SolrQuerySupport.getNumDocsFromSolr(collection, zkHost, Some(solrQuery))
    val splits = noOfDocs / (docsPerTask * shardSize)
    val splitsPerShard = if (splits > minSplitSize) {
      splits.toInt
    } else {
      minSplitSize
    }
    logger.debug(s"Suggested split size: ${splitsPerShard} for collection size: ${noOfDocs} using query: ${solrQuery}")
    splitsPerShard
  }
}

object SolrRDD {

  def randomReplicaLocation(solrShard: SolrShard): String = {
    randomReplica(solrShard).replicaUrl
  }

  def randomReplica(solrShard: SolrShard): SolrReplica = {
    solrShard.replicas(Random.nextInt(solrShard.replicas.size))
  }

  def randomReplica(solrShard: SolrShard, replicaToExclude: SolrReplica): SolrReplica = {
    val filteredReplicas = solrShard.replicas.filter(p => p.equals(replicaToExclude))
    solrShard.replicas(Random.nextInt(filteredReplicas.size))
  }

  def apply(
      zkHost: String,
      collection: String,
      @transient sc: SparkContext,
      requestHandler: Option[String] = None,
      query : Option[String] = Option(DEFAULT_QUERY),
      fields: Option[Array[String]] = None,
      rows: Option[Int] = Option(DEFAULT_PAGE_SIZE),
      splitField: Option[String] = None,
      splitsPerShard: Option[Int] = None,
      solrQuery: Option[SolrQuery] = None,
      uKey: Option[String] = None,
      maxRows: Option[Int] = None,
      accumulator: Option[SparkSolrAccumulator] = None): SolrRDD[_] = {
    if (requestHandler.isDefined) {
      if (requiresStreamingRDD(requestHandler.get)) {
        // streaming doesn't support maxRows
        new StreamingSolrRDD(zkHost, collection, sc, requestHandler, query, fields, rows, splitField, splitsPerShard, solrQuery, uKey, accumulator)
      } else {
        new SelectSolrRDD(zkHost, collection, sc, requestHandler, query, fields, rows, splitField, splitsPerShard, solrQuery, uKey, maxRows, accumulator)
      }
    } else {
      new SelectSolrRDD(zkHost, collection, sc, Some(DEFAULT_REQUEST_HANDLER), query, fields, rows, splitField, splitsPerShard, solrQuery, uKey, maxRows, accumulator)
    }
  }

  def requiresStreamingRDD(rq: String): Boolean = {
    rq == QT_EXPORT || rq == QT_STREAM || rq == QT_SQL
  }

}

