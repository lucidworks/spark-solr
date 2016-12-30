package com.lucidworks.spark.query;

import com.lucidworks.spark.SolrReplica;
import com.lucidworks.spark.SolrShard;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import scala.Function2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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

    List<SolrReplica> sortedReplicas = new ArrayList<>(numReplicas);
    for (int r=0; r < numReplicas; r++) sortedReplicas.add(replicas.apply(r));
    sortedReplicas.sort((o1, o2) -> o1.replicaName().compareTo(o2.replicaName()));

    for (int i=0; i < numSplits; i++) {
      String fq = "{!hash workers="+numSplits+" worker="+i+"}";
      // with hash, we can hit all replicas in the shard in parallel
      SolrReplica replica;
      if (numReplicas > 1) {
        replica = (i < numReplicas) ? sortedReplicas.get(i) : sortedReplicas.get(i % numReplicas);
      } else {
        replica = sortedReplicas.get(0);
      }
      splits.add(new WorkerShardSplit(query, replica.replicaUrl(), splitFieldName, fq, replica));
    }
    return splits;
  }

  public static class WorkerShardSplit extends AbstractShardSplit<String> {

    SolrReplica myReplica;

    WorkerShardSplit(SolrQuery query, String shardUrl, String rangeField, String fq, SolrReplica myReplica) {
      super(query, shardUrl, rangeField, fq);
      this.myReplica = myReplica;
    }

    public SolrReplica getReplica() {
      return myReplica;
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
