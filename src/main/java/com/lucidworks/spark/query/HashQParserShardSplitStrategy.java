package com.lucidworks.spark.query;

import com.lucidworks.spark.SolrReplica;
import com.lucidworks.spark.SolrShard;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class HashQParserShardSplitStrategy implements ShardSplitStrategy, Serializable {

  protected SolrShard shard;

  public HashQParserShardSplitStrategy(SolrShard shard) {
    this.shard = shard;
  }

  @Override
  public List<ShardSplit> getSplits(String shardUrl, SolrQuery query, String splitFieldName, int numSplits) throws IOException, SolrServerException {
    query.set("partitionKeys", splitFieldName);
    List<ShardSplit> splits = new ArrayList<>(numSplits);
    scala.collection.immutable.List<SolrReplica> replicas = shard.replicas();
    final int numReplicas = replicas.size();
    for (int i=0; i < numSplits; i++) {
      String fq = "{!hash workers="+numSplits+" worker="+i+"}";
      // with hash, we can hit all replicas in the shard in parallel
      String splitShardUrl = shardUrl;
      if (numReplicas > 1) {
        splitShardUrl = (i < numReplicas) ? replicas.apply(i).replicaUrl() : replicas.apply(i % numReplicas).replicaUrl();
      }
      splits.add(new WorkerShardSplit(query, splitShardUrl, splitFieldName, fq));
    }
    return splits;
  }

  class WorkerShardSplit extends AbstractShardSplit<String> {
    WorkerShardSplit(SolrQuery query, String shardUrl, String rangeField, String fq) {
      super(query, shardUrl, rangeField, fq);
    }

    @Override
    public String nextUpper(String lower, long increment) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getRange() {
      throw new UnsupportedOperationException();
    }
  }
}
