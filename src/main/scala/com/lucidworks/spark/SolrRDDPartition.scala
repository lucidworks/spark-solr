package com.lucidworks.spark

import org.apache.solr.client.solrj.SolrQuery
import org.apache.spark.Partition

trait SolrRDDPartition extends Partition {
  def cursorMark: String
  def solrShard: SolrShard
  def query: SolrQuery
}
