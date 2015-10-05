package com.lucidworks.spark.query;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;
import java.util.List;

public interface ShardSplitStrategy {

  List<ShardSplit> getSplits(String shardUrl, SolrQuery query, String splitFieldName, int numSplits)
      throws IOException, SolrServerException;

}
