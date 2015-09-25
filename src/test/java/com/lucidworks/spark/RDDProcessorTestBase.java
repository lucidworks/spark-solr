package com.lucidworks.spark;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;

/**
 * Base class for testing RDDProcessor implementations.
 */
public class RDDProcessorTestBase extends TestSolrCloudClusterSupport implements Serializable {

  protected static transient JavaSparkContext jsc;

  @BeforeClass
  public static void setupJavaSparkContext() {
    SparkConf conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set("spark.default.parallelism", "1");
    jsc = new JavaSparkContext(conf);
  }

  @AfterClass
  public static void stopSparkContext() {
    jsc.stop();
  }

  protected void buildCollection(String zkHost, String collection) throws Exception {
    String[] inputDocs = new String[] {
      collection+"-1,foo,bar,1,[a;b],[1;2]",
      collection+"-2,foo,baz,2,[c;d],[3;4]",
      collection+"-3,bar,baz,3,[e;f],[5;6]"
    };
    buildCollection(zkHost, collection, inputDocs, 2);
  }

  protected void buildCollection(String zkHost, String collection, int numDocs) throws Exception {
    buildCollection(zkHost, collection, numDocs, 2);
  }

  protected void buildCollection(String zkHost, String collection, int numDocs, int numShards) throws Exception {
    String[] inputDocs = new String[numDocs];
    for (int n=0; n < numDocs; n++)
      inputDocs[n] = collection+"-"+n+",foo"+n+",bar"+n+","+n+",[a;b],[1;2]";
    buildCollection(zkHost, collection, inputDocs, numShards);
  }

  protected void buildCollection(String zkHost, String collection, String[] inputDocs, int numShards) throws Exception {
    String confName = "testConfig";
    File confDir = new File("src/test/resources/conf");
    int replicationFactor = 1;
    createCollection(collection, numShards, replicationFactor, numShards /* maxShardsPerNode */, confName, confDir);

    // index some docs into the new collection
    if (inputDocs != null) {
      int numDocsIndexed = indexDocs(zkHost, collection, inputDocs);
      Thread.sleep(1000L);
      // verify docs got indexed ... relies on soft auto-commits firing frequently
      SolrRDD solrRDD = new SolrRDD(zkHost, collection);
      JavaRDD<SolrDocument> resultsRDD = solrRDD.query(jsc.sc(), "*:*");
      long numFound = resultsRDD.count();
      assertTrue("expected " + numDocsIndexed + " docs in query results from " + collection + ", but got " + numFound,
          numFound == (long) numDocsIndexed);
    }
  }

  protected int indexDocs(String zkHost, String collection, String[] inputDocs) {
    JavaRDD<String> input = jsc.parallelize(Arrays.asList(inputDocs), 1);
    JavaRDD<SolrInputDocument> docs = input.map(new Function<String, SolrInputDocument>() {
      public SolrInputDocument call(String row) throws Exception {
        String[] fields = row.split(",");
        if (fields.length < 6)
          throw new IllegalArgumentException("Each test input doc should have 6 fields! invalid doc: "+row);

        SolrInputDocument doc = new SolrInputDocument();
        doc.setField("id", fields[0]);
        doc.setField("field1_s", fields[1]);
        doc.setField("field2_s", fields[2]);
        doc.setField("field3_i", Integer.parseInt(fields[3]));

        String[] list = fields[4].substring(1,fields[4].length()-1).split(";");
        for (int i=0; i < list.length; i++)
          doc.addField("field4_ss", list[i]);

        list = fields[5].substring(1,fields[5].length()-1).split(";");
        for (int i=0; i < list.length; i++)
          doc.addField("field5_ii", Integer.parseInt(list[i]));

        return doc;
      }
    });
    SolrSupport.indexDocs(zkHost, collection, 1, docs);
    return inputDocs.length;
  }
}
