package com.lucidworks.spark.solr;

import com.lucidworks.spark.BatchSizeType;
import com.lucidworks.spark.RDDProcessorTestBase;
import com.lucidworks.spark.util.EmbeddedSolrServerFactory;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Test;

public class TestEmbeddedSolrServer extends RDDProcessorTestBase {

  @Test
  public void testEmbeddedSolrServerUseNumDocsAsBatchSize() throws Exception {
    this.batchSizeType = BatchSizeType.NUM_DOCS;
    this.batchSize = 3; // 3 docs
    runEmbeddedServerTest("batchNumDocs", 10);
  }

  @Test
  public void testEmbeddedSolrServerUseNumBytesAsBatchSize() throws Exception {
    this.batchSizeType = BatchSizeType.NUM_BYTES;
    this.batchSize = 1000; // 1000 bytes
    runEmbeddedServerTest("batchNumBytes", 100);
  }

  private void runEmbeddedServerTest(String testCollection, int numDocs) throws Exception {
    EmbeddedSolrServer embeddedSolrServer = null;
    try {
      String zkHost = cluster.getZkServer().getZkAddress();
      buildCollection(zkHost, testCollection, numDocs, 1);
      embeddedSolrServer = EmbeddedSolrServerFactory.singleton.getEmbeddedSolrServer(zkHost, testCollection);
      QueryResponse queryResponse = embeddedSolrServer.query(new SolrQuery("*:*"));
      assert (queryResponse.getStatus() == 0);
    } finally {
      if (embeddedSolrServer != null) {
        embeddedSolrServer.close();
      }
      deleteCollection(testCollection);
    }
  }

  @Test
  public void testEmbeddedSolrServerCustomConfig() throws Exception {
    String testCollection = "testEmbeddedSolrServerConfig";
    EmbeddedSolrServer embeddedSolrServer = null;
    try {
      String zkHost = cluster.getZkServer().getZkAddress();
      buildCollection(zkHost, testCollection, 10, 1);
      embeddedSolrServer = EmbeddedSolrServerFactory.singleton.getEmbeddedSolrServer(zkHost, testCollection, "custom-solrconfig.xml", null);
      QueryResponse queryResponse = embeddedSolrServer.query(new SolrQuery("*:*"));
      assert(queryResponse.getStatus() == 0);
    } finally {
      if (embeddedSolrServer != null) {
        embeddedSolrServer.close();
      }
      deleteCollection(testCollection);
    }
  }
}
