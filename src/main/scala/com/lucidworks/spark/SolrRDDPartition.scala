package com.lucidworks.spark

import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.params.SolrParams
import org.apache.spark.Partition

trait SolrRDDPartition extends Partition {
  val solrShard: SolrShard
  val query: SolrQuery
  var preferredReplica: SolrReplica // Preferred replica to query
}

case class CloudStreamPartition(
    index: Int,
    zkhost:String,
    collection:String,
    params: SolrParams)
  extends Partition

case class SelectSolrRDDPartition(
    index: Int,
    cursorMark: String,
    solrShard: SolrShard,
    query: SolrQuery,
    var preferredReplica: SolrReplica)
  extends SolrRDDPartition

case class ExportHandlerPartition(
    index: Int,
    solrShard: SolrShard,
    query: SolrQuery,
    var preferredReplica: SolrReplica,
    numWorkers: Int,
    workerId: Int)
  extends SolrRDDPartition

case class SolrLimitPartition(
    index: Int = 0,
    zkhost:String,
    collection:String,
    maxRows: Int,
    query: SolrQuery)
  extends Partition
