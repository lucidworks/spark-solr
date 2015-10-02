package com.lucidworks.spark.query;

import org.apache.solr.client.solrj.SolrQuery;
import java.io.Serializable;

/**
 * Interface to an object that encapsulates metadata needed to query a sub-range of a shard, aka "split"
 * to increase parallelism when reading data from Solr.
 */
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
  T nextUpper(T lower, long increment);
  long getRange();
}
