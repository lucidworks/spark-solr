package com.lucidworks.spark;

import com.lucidworks.spark.rdd.SolrRDD;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

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
    SolrRDD solrRDD = new SolrRDD(zkHost, aliasName, jsc.sc());
    JavaRDD<SolrDocument> resultsRDD = solrRDD.query("*:*");
    long numFound = resultsRDD.count();
    assertTrue("expected " + expectedNumDocs + " docs in query results from alias " + aliasName + ", but got " + numFound,
      numFound == expectedNumDocs);
  }

  @Test
  public void testQueryShards() throws Exception {
    String zkHost = cluster.getZkServer().getZkAddress();
    String testCollection = "queryShards";
    int numDocs = 2000;
    buildCollection(zkHost, testCollection, numDocs, 3);

    SolrRDD solrRDD = new SolrRDD(zkHost, testCollection, jsc.sc());
    SolrQuery testQuery = new SolrQuery();
    testQuery.setQuery("*:*");
    testQuery.setRows(57);
    testQuery.addSort(new SolrQuery.SortClause("id", SolrQuery.ORDER.asc));
    JavaRDD<SolrDocument> docs = solrRDD.queryShards(testQuery);
    List<SolrDocument> docList = docs.collect();
    assertTrue("expected "+numDocs+" from queryShards but only found "+docList.size(), docList.size() == numDocs);
    deleteCollection(testCollection);
  }

}
