package com.lucidworks.spark

import org.apache.solr.client.solrj.SolrQuery
import org.apache.spark.Partition

class SolrRDDPartition(
  val cursorMark: String,
  val solrShard: SolrShard,
  val query: SolrQuery
  ) extends Partition{
  override def index: Int = solrShard.shardNumber
}
