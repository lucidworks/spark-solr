package com.lucidworks.spark

import org.apache.solr.client.solrj.SolrQuery
import org.apache.spark.{Partition, Logging, Partitioner}

class ShardPartitioner(
  solrQuery: SolrQuery,
  shards: List[SolrShard])
  extends Partitioner with Logging {
  override def numPartitions: Int = shards.length

  override def getPartition(key: Any): Int = {
    key match {
      case partition: SolrRDDPartition =>
        partition.solrShard.shardNumber
      case _ =>
        //     throw new SparkException("Unknown partition '" + key + "' of type '" + key.asInstanceOf[AnyRef].getClass) + "'")
        0
    }
  }

  def getPartitions: Array[Partition] = {
    shards.map(f => getRDDPartition(f)).toArray
  }

  private def getRDDPartition(shard: SolrShard): Partition = {
    new SolrRDDPartition("*", shard, solrQuery)
  }
}

case class SolrShard(
  shardNumber: Int,
  shardName: String,
  replicas: List[SolrReplica])

case class SolrReplica(
  replicaNumber: Int,
  replicaName: String,
  replicaLocation: String,
  replicaHostName: String)
