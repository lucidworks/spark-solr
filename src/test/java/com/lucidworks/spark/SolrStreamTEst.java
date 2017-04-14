package com.lucidworks.spark;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.junit.Test;

public class SolrStreamTEst {

  @Test
  public void testSolrStream() throws Exception {
    SolrQuery query = new SolrQuery("*:*");
    query.set("fq", "{!hash workers=2 worker=0}");
    query.set("partitionKeys", "_version_");
    query.set("distrib", "false");
    String shardUrl = "http://localhost:8983/solr/test_shard1_replica1";
    SolrStream stream = new SolrStream(shardUrl, query);
    stream.open();
    Tuple tuple = stream.read();

  }
}
