package com.lucidworks.spark.query;

import org.apache.solr.client.solrj.SolrQuery;

import java.io.Serializable;

public interface ShardSplit extends Serializable {
  String getShardUrl();
  SolrQuery getSplitQuery();
  void setNumHits(Long numHits);
  Long getNumHits();
}
