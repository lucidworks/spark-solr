package com.lucidworks.spark

import java.net.InetAddress

import com.lucidworks.spark.util.SolrSupportScala
import org.apache.solr.client.solrj.SolrQuery
import org.apache.spark.Partition

import scala.collection.mutable.ArrayBuffer

trait SolrRDDPartition extends Partition {
  def cursorMark: String
  def solrShard: SolrShard
  def query: SolrQuery
}

case class ShardRDDPartition(
  index: Int,
  cursorMark: String,
  solrShard: SolrShard,
  query: SolrQuery) extends SolrRDDPartition

case class SplitRDDPartition(
  index: Int,
  cursorMark: String,
  solrShard: SolrShard,
  query: SolrQuery) extends SolrRDDPartition

// Is there a need to override {@code Partitioner.scala} and define our own partition id's
object ShardPartitioner {

  def getShardPartitions(shards: List[SolrShard], query: SolrQuery) : Array[Partition] = {
    shards.zipWithIndex.map{case (shard, i) => new ShardRDDPartition(i, "*", shard, query)}.toArray
  }

  def getSplitPartitions(shards: List[SolrShard],
                         query: SolrQuery,
                         splitFieldName: String,
                         splitsPerShard: Int): Array[Partition] = {
    var splitPartitions = ArrayBuffer.empty[SplitRDDPartition]
    var counter = 0
    shards.foreach(shard => {
      val splits = SolrSupportScala.splitShards(query, shard, splitFieldName, splitsPerShard)
      splits.foreach(split => {
        splitPartitions += SplitRDDPartition(counter, "*", shard, split.getQuery)
        counter = counter + 1
      })
    })
    splitPartitions.toArray
  }
}

case class SolrShard(
  shardName: String,
  replicas: List[SolrReplica])

case class SolrReplica(
  replicaNumber: Int,
  replicaName: String,
  replicaLocation: String,
  replicaHostName: String,
  locations: Array[InetAddress])
