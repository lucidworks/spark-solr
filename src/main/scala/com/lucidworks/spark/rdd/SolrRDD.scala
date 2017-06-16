package com.lucidworks.spark.rdd

import com.lucidworks.spark._
import com.lucidworks.spark.util.QueryConstants._
import com.lucidworks.spark.util.{SolrQuerySupport, SolrSupport}
import com.typesafe.scalalogging.LazyLogging
import org.apache.solr.client.solrj.SolrQuery
import org.apache.spark._
import org.apache.spark.rdd.RDD

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

  val uniqueKey: String = if (uKey.isDefined) uKey.get else SolrQuerySupport.getUniqueKey(zkHost, collection.split(",")(0))

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split match {
      case partition: SelectSolrRDDPartition => Array(partition.preferredReplica.replicaHostName)
      case partition: ExportHandlerPartition => Array(partition.preferredReplica.replicaHostName)
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
    val preferredReplica = partition.preferredReplica.replicaUrl
    if (attempt_no == 0)
      preferredReplica
    else {
      logger.info(s"Task attempt no. ${attempt_no}. Checking if replica ${preferredReplica} is healthy")
      // can't do much if there is only one replica for this shard
      if (partition.solrShard.replicas.length == 1)
        return preferredReplica

      // Query preferred replica to check if the response is successful. If not, try with a different replica
      try {
        partition match {
          case _: SelectSolrRDDPartition =>
            SolrQuerySupport.querySolr(SolrSupport.getCachedHttpSolrClient(preferredReplica, zkHost), partition.query.getCopy, 0, "*")
          case _: ExportHandlerPartition =>
            SolrQuerySupport.validateExportHandlerQuery(SolrSupport.getCachedHttpSolrClient(preferredReplica, zkHost), partition.query.getCopy)
        }
        preferredReplica
      } catch {
        case e: Exception =>
          logger.error(s"Error querying replica ${preferredReplica} while checking if it's healthy. Exception: $e")
          val newReplicaToQuery = SolrRDD.randomReplica(partition.solrShard, partition.preferredReplica)
          logger.info(s"Switching from $preferredReplica to ${newReplicaToQuery.replicaUrl}")
          partition.preferredReplica = newReplicaToQuery
          newReplicaToQuery.replicaUrl
      }
    }
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
      maxRows: Option[Int] = None): SolrRDD[_] = {
    if (requestHandler.isDefined) {
      if (requiresStreamingRDD(requestHandler.get)) {
        // streaming doesn't support maxRows
        new StreamingSolrRDD(zkHost, collection, sc, requestHandler, query, fields, rows, splitField, splitsPerShard, solrQuery, uKey)
      } else {
        new SelectSolrRDD(zkHost, collection, sc, requestHandler, query, fields, rows, splitField, splitsPerShard, solrQuery, uKey, maxRows)
      }
    } else {
      new SelectSolrRDD(zkHost, collection, sc, Some(DEFAULT_REQUEST_HANDLER), query, fields, rows, splitField, splitsPerShard, solrQuery, uKey, maxRows)
    }
  }

  def requiresStreamingRDD(rq: String): Boolean = {
    rq == QT_EXPORT || rq == QT_STREAM || rq == QT_SQL
  }
}

