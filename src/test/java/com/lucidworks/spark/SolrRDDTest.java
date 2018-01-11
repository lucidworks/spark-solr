package com.lucidworks.spark;

import com.lucidworks.spark.rdd.SolrJavaRDD;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
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
    assertEquals(createAliasCollectionsList, aliases.getCollectionAliasMap().get(aliasName));

    // ok, alias is setup ... now fire a query against it
    long expectedNumDocs = 6;
    SolrJavaRDD solrRDD = SolrJavaRDD.get(zkHost, aliasName, jsc.sc());
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

    SolrJavaRDD solrRDD = SolrJavaRDD.get(zkHost, testCollection, jsc.sc());


    SolrQuery testQuery = new SolrQuery();
    testQuery.setQuery("*:*");
    testQuery.setRows(57);
    testQuery.addSort(new SolrQuery.SortClause("id", SolrQuery.ORDER.asc));
    JavaRDD<SolrDocument> docs = solrRDD.queryShards(testQuery);
    List<SolrDocument> docList = docs.collect();
    assertTrue("expected "+numDocs+" from queryShards but only found "+docList.size(), docList.size() == numDocs);

    deleteCollection(testCollection);
  }

  @Ignore //Ignore until real-time GET is implemented
  @Test
  public void testGet() throws Exception {
    String zkHost = cluster.getZkServer().getZkAddress();
    String testCollection = "queryGet";
    deleteCollection(testCollection);
    buildCollection(zkHost, testCollection, new String[0], 1);

    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "new-dummy-doc");
    doc.addField("field1_s", "value1");
    doc.addField("field2_s", "value2");
    cloudSolrServer.add(testCollection, doc, -1);

    SolrJavaRDD rdd = SolrJavaRDD.get(zkHost, testCollection, jsc.sc());
//    List<SolrDocument> docs = rdd.get(doc.getField("id").getValue().toString()).collect();
//    assert docs.size() == 1;
//    assert docs.get(0).get("id").equals(doc.getField("id").getValue());
  }

  @Test
  public void testSolrQuery() throws Exception {
    String testCollection = "testSolrQuery";

    try {
      String zkHost = cluster.getZkServer().getZkAddress();
      String[] inputDocs = new String[] {
        testCollection+"-1,foo,bar,1,[a;b],[1;2]",
        testCollection+"-2,foo,baz,2,[c;d],[3;4]",
        testCollection+"-3,bar,baz,3,[e;f],[5;6]"
      };

      buildCollection(zkHost, testCollection, inputDocs, 1);

      {
        String queryStr = "q=*:*&sort=id asc&fq=field1_s:foo";

        SolrJavaRDD solrRDD = SolrJavaRDD.get(zkHost, testCollection, jsc.sc());
        List<SolrDocument> docs = solrRDD.query(queryStr).collect();

        assert(docs.size() == 2);
        assert docs.get(0).get("id").equals(testCollection + "-1");
      }

      {
        String queryStr = "q=*:*&sort=id&fq=field3_i:[2 TO 3]";

        SolrJavaRDD solrRDD = SolrJavaRDD.get(zkHost, testCollection, jsc.sc());

        List<SolrDocument> docs = solrRDD.queryNoSplits(queryStr).collect();

        assert docs.size() == 2;
        assert docs.get(0).get("id").equals(testCollection + "-2");
      }

    } finally {
      deleteCollection(testCollection);
    }

  }


}
