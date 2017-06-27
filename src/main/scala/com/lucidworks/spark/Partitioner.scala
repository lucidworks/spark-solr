package com.lucidworks.spark

import java.net.InetAddress

import com.lucidworks.spark.rdd.SolrRDD
import com.lucidworks.spark.util.SolrSupport
import org.apache.solr.client.solrj.SolrQuery
import org.apache.spark.Partition

import scala.collection.mutable.ArrayBuffer

// Is there a need to override {@code Partitioner.scala} and define our own partition id's
object SolrPartitioner {

  def getShardPartitions(shards: List[SolrShard], query: SolrQuery) : Array[Partition] = {
    shards.zipWithIndex.map{ case (shard, i) =>
      // Chose any of the replicas as the active shard to query
      SelectSolrRDDPartition(i, "*", shard, query, SolrRDD.randomReplica(shard))}.toArray
  }

  def getSplitPartitions(
      shards: List[SolrShard],
      query: SolrQuery,
      splitFieldName: String,
      splitsPerShard: Int): Array[Partition] = {
    var splitPartitions = ArrayBuffer.empty[SelectSolrRDDPartition]
    var counter = 0
    shards.foreach(shard => {
      val splits = SolrSupport.getShardSplits(query, shard, splitFieldName, splitsPerShard)
      splits.foreach(split => {
        splitPartitions += SelectSolrRDDPartition(counter, "*", shard, split.query, split.replica)
        counter = counter + 1
      })
    })
    splitPartitions.toArray
  }

  // Workaround for SOLR-10490. TODO: Remove once fixed
  def getExportHandlerPartitions(
      shards: List[SolrShard],
      query: SolrQuery): Array[Partition] = {
    shards.zipWithIndex.map{ case (shard, i) =>
      // Chose any of the replicas as the active shard to query
      ExportHandlerPartition(i, shard, query, SolrRDD.randomReplica(shard), 0, 0)}.toArray
  }

  // Workaround for SOLR-10490. TODO: Remove once fixed
  def getExportHandlerPartitions(
      shards: List[SolrShard],
      query: SolrQuery,
      splitFieldName: String,
      splitsPerShard: Int): Array[Partition] = {
    val splitPartitions = ArrayBuffer.empty[ExportHandlerPartition]
    var counter = 0
    shards.foreach(shard => {
      // Form a continuous iterator list so that we can pick different replicas for different partitions in round-robin mode
      val splits = SolrSupport.getExportHandlerSplits(query, shard, splitFieldName, splitsPerShard)
      splits.foreach(split => {
        splitPartitions += ExportHandlerPartition(counter, shard, split.query, split.replica, split.numWorkers, split.workerId)
        counter = counter+1
      })
    })
    splitPartitions.toArray
  }

}

case class SolrShard(shardName: String, replicas: List[SolrReplica])

case class SolrReplica(
    replicaNumber: Int,
    replicaName: String,
    replicaUrl: String,
    replicaHostName: String,
    locations: Array[InetAddress]) {
  def getHostAndPort(): String = {replicaHostName.substring(0, replicaHostName.indexOf('_'))}
  override def toString(): String = {
    return s"SolrReplica(${replicaNumber}) ${replicaName}: url=${replicaUrl}, hostName=${replicaHostName}, locations="+locations.mkString(",")
  }
}
