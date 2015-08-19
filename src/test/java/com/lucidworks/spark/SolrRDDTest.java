package com.lucidworks.spark;

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
    SolrRDD solrRDD = new SolrRDD(zkHost, aliasName);
    JavaRDD<SolrDocument> resultsRDD = solrRDD.query(jsc.sc(), "*:*");
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

    SolrRDD solrRDD = new SolrRDD(zkHost, testCollection);
    SolrQuery testQuery = new SolrQuery();
    testQuery.setQuery("*:*");
    testQuery.setRows(57);
    testQuery.addSort(new SolrQuery.SortClause("id", SolrQuery.ORDER.asc));
    JavaRDD<SolrDocument> docs = solrRDD.queryShards(jsc, testQuery);
    List<SolrDocument> docList = docs.collect();
    assertTrue("expected "+numDocs+" from queryShards but only found "+docList.size(), docList.size() == numDocs);
    deleteCollection(testCollection);
  }

  @Test
  public void testQueryDeep() throws Exception {
    String zkHost = cluster.getZkServer().getZkAddress();
    String testCollection = "queryDeep";
    int numDocs = 2000;
    buildCollection(zkHost, testCollection, numDocs);
    SolrRDD solrRDD = new SolrRDD(zkHost, testCollection);
    SolrQuery testQuery = new SolrQuery();
    testQuery.setQuery("*:*");
    testQuery.setRows(200);
    testQuery.addSort(new SolrQuery.SortClause("field3_i", SolrQuery.ORDER.desc));
    testQuery.addSort(new SolrQuery.SortClause("id", SolrQuery.ORDER.asc));
    JavaRDD<SolrDocument> docs = solrRDD.queryDeep(jsc, testQuery);
    List<SolrDocument> docList = docs.collect();
    assertTrue("expected "+numDocs+" from queryDeep but only found "+docList.size(), docList.size() == numDocs);
    assertTrue("expected last doc with field3_i==" +(numDocs-1)+" but got "+docList.get(0),
      numDocs-1 == (Integer)(docList.get(0)).getFirstValue("field3_i"));
    deleteCollection(testCollection);
  }
}
