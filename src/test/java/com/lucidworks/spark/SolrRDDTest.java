package com.lucidworks.spark;

import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test basic functionality of the SolrRDD implementations.
 */
public class SolrRDDTest extends RDDProcessorTestBase {

  @Test
  public void testCollectionAliasSupport() throws Exception {
    String zkHost = cluster.getZkServer().getZkAddress();
    buildCollection(zkHost, "test1");
    buildCollection(zkHost, "test2");

    // create a collection alias that uses test1 and test2 under the covers
    String aliasName = "test";
    String createAliasCollectionsList = "test1,test2";
    ModifiableSolrParams modParams = new ModifiableSolrParams();
    modParams.set(CoreAdminParams.ACTION, CollectionParams.CollectionAction.CREATEALIAS.name());
    modParams.set("name", aliasName);
    modParams.set("collections", createAliasCollectionsList);
    QueryRequest request = new QueryRequest(modParams);
    request.setPath("/admin/collections");
    cloudSolrServer.request(request);

    Aliases aliases = cloudSolrServer.getZkStateReader().getAliases();
    assertEquals(createAliasCollectionsList, aliases.getCollectionAlias(aliasName));

    // ok, alias is setup ... now fire a query against it
    long expectedNumDocs = 6;
    SolrRDD solrRDD = new SolrRDD(zkHost, aliasName);
    JavaRDD<SolrDocument> resultsRDD = solrRDD.query(jsc.sc(), "*:*");
    long numFound = resultsRDD.count();
    assertTrue("expected " + expectedNumDocs + " docs in query results from alias " + aliasName + ", but got " + numFound,
      numFound == expectedNumDocs);
  }
}
