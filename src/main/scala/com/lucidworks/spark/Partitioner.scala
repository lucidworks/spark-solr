package com.lucidworks.spark

import java.net.InetAddress

import com.lucidworks.spark.rdd.SolrRDD
import com.lucidworks.spark.util.SolrSupport
import org.apache.solr.client.solrj.SolrQuery
import org.apache.spark.Partition

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// Is there a need to override {@code Partitioner.scala} and define our own partition id's
object SolrPartitioner {

  def getShardPartitions(shards: List[SolrShard], query: SolrQuery) : Array[Partition] = {
    shards.zipWithIndex.map{ case (shard, i) =>
      // Chose any of the replicas as the active shard to query
      new ShardRDDPartition(i, "*", shard, query, SolrRDD.randomReplica(shard))}.toArray
  }

  def getSplitPartitions(
      shards: List[SolrShard],
      query: SolrQuery,
      splitFieldName: String,
      splitsPerShard: Int): Array[Partition] = {
    var splitPartitions = ArrayBuffer.empty[SplitRDDPartition]
    var counter = 0
    shards.foreach(shard => {
      // Form a continuous iterator list so that we can pick different replicas for different partitions in round-robin mode
      val replicaContinuousIterator: Iterator[SolrReplica] = Iterator.continually(shard.replicas).flatten
      val splits = SolrSupport.splitShards(query, shard, splitFieldName, splitsPerShard)
      splits.foreach(split => {
        splitPartitions += SplitRDDPartition(counter, "*", shard, split.getSplitQuery, replicaContinuousIterator.next())
        counter = counter + 1
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
  override def toString(): String = {
    return s"SolrReplica(${replicaNumber}) ${replicaName}: url=${replicaUrl}, hostName=${replicaHostName}, locations="+locations.mkString(",")
  }
}
