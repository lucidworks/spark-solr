package com.lucidworks.spark

import org.apache.solr.client.solrj.SolrQuery
import org.apache.spark.Partition

case class SolrRDDPartition(
  index: Int,
  cursorMark: String,
  solrShard: SolrShard,
  query: SolrQuery
  ) extends Partition{
}
