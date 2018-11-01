package com.lucidworks.spark.solr;

import com.lucidworks.spark.RDDProcessorTestBase;
import com.lucidworks.spark.util.EmbeddedSolrServerFactory;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Test;

public class TestEmbeddedSolrServer extends RDDProcessorTestBase {

  @Test
  public void testEmbeddedSolrServer() throws Exception {
    String testCollection = "testEmbeddedSolrServer";
    EmbeddedSolrServer embeddedSolrServer = null;
    try {
      String zkHost = cluster.getZkServer().getZkAddress();
      buildCollection(zkHost, testCollection, 10, 1);
      embeddedSolrServer = EmbeddedSolrServerFactory.singleton.getEmbeddedSolrServer(zkHost, testCollection);
      QueryResponse queryResponse = embeddedSolrServer.query(new SolrQuery("*:*"));
      assert(queryResponse.getStatus() == 0);
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
