package com.lucidworks.spark

import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.params.SolrParams
import org.apache.spark.Partition

trait SolrRDDPartition extends Partition {
  def cursorMark: String
  def solrShard: SolrShard
  def query: SolrQuery
  def preferredReplica: SolrReplica // Preferred replica to query
}

case class CloudStreamPartition(
    index: Int,
    zkhost:String,
    collection:String,
    params: SolrParams)
  extends Partition

case class ShardRDDPartition(
    index: Int,
    cursorMark: String,
    solrShard: SolrShard,
    query: SolrQuery,
    preferredReplica: SolrReplica)
  extends SolrRDDPartition

case class SplitRDDPartition(
    index: Int,
    cursorMark: String,
    solrShard: SolrShard,
    query: SolrQuery,
    preferredReplica: SolrReplica)
  extends SolrRDDPartition

case class ExportHandlerPartition(
    index: Int,
    solrShard: SolrShard,
    query: SolrQuery,
    preferredReplica: SolrReplica,
    numWorkers: Int,
    workerId: Int)
  extends Partition

case class SolrLimitPartition(
    index: Int = 0,
    zkhost:String,
    collection:String,
    maxRows: Int,
    query: SolrQuery)
  extends Partition
