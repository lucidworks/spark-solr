package com.lucidworks.spark;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test basic functionality of the SolrRDD implementations.
 */
public class SolrRDDTest extends TestSolrCloudClusterSupport implements Serializable {

  protected transient JavaSparkContext jsc;

  @Before
  public void setupSparkStreamingContext() {
    SparkConf conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set("spark.default.parallelism", "1");
    jsc = new JavaSparkContext(conf);
  }

  @After
  public void stopSparkContext() {
    jsc.stop();
  }

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
  
  protected void buildCollection(String zkHost, String collection) throws Exception {
    String confName = "testConfig";
    File confDir = new File("src/test/resources/conf");
    int numShards = 2;
    int replicationFactor = 1;
    createCollection(collection, numShards, replicationFactor, 2, confName, confDir);

    // index some docs into both collections
    int numDocsIndexed = indexDocs(zkHost, collection);
    Thread.sleep(1000L);

    // verify docs got indexed ... relies on soft auto-commits firing frequently
    SolrRDD solrRDD = new SolrRDD(zkHost, collection);
    JavaRDD<SolrDocument> resultsRDD = solrRDD.query(jsc.sc(), "*:*");
    long numFound = resultsRDD.count();
    assertTrue("expected "+numDocsIndexed+" docs in query results from "+collection+", but got "+numFound,
      numFound == (long)numDocsIndexed);
  }

  protected int indexDocs(String zkHost, String collection) {
    String[] inputDocs = new String[] { collection+"-1,foo,bar", collection+"-2,foo,baz", collection+"-3,bar,baz" };
    JavaRDD<String> input = jsc.parallelize(Arrays.asList(inputDocs), 1);
    JavaRDD<SolrInputDocument> docs = input.map(new Function<String, SolrInputDocument>() {
      public SolrInputDocument call(String row) throws Exception {
        String[] fields = row.split(",");
        SolrInputDocument doc = new SolrInputDocument();
        doc.setField("id", fields[0]);
        doc.setField("field1", fields[1]);
        doc.setField("field2", fields[2]);
        return doc;
      }
    });
    SolrSupport.indexDocs(zkHost, collection, 1, docs);
    return inputDocs.length;
  }
}
