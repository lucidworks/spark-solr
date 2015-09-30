package com.lucidworks.spark.query;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public interface ShardSplit<T> extends Serializable {
  String getShardUrl();
  String getSplitFilterQuery();
  String getSplitFieldName();
  SolrQuery getQuery();
  SolrQuery getSplitQuery();
  void setNumHits(Long numHits);
  Long getNumHits();
  T getLowerInc();
  T getUpper();
  List<ShardSplit> reSplit(SolrClient solrClient, long docsPerSplit) throws IOException, SolrServerException;
}
