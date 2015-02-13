package com.lucidworks.spark.example.streaming.oneusagov;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.lucidworks.spark.SolrRDD;
import com.lucidworks.spark.SparkApp;
import com.lucidworks.spark.StreamProcessorTestBase;
import com.lucidworks.spark.streaming.MessageStreamReceiver;
import com.lucidworks.spark.streaming.file.LocalFileMessageStream;
import org.apache.commons.cli.CommandLine;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Indexes data pulled from the 1.usa.gov feed.
 */
@Ignore
public class OneUsaGovStreamProcessorTest extends StreamProcessorTestBase {

  @Test
  public void testOneUsaGovStream() throws Exception {
    String confName = "testConfig";
    File confDir = new File("src/test/resources/conf");
    String testCollection = "oneusagovreq";
    int numShards = 1;
    int replicationFactor = 1;

    createCollection(testCollection, numShards, replicationFactor, confName, confDir);

    // Load the test data from a local file
    final File testDataFile = new File("src/test/resources/test-data/oneusagov/oneusagov_sample.txt");
    assertTrue(testDataFile.isFile());
    Set<OneUsaGovRequest> requests = loadTestData(testDataFile);

    // mock the HTTP stream receiver with a stream from a local test file
    OneUsaGovStreamProcessor streamProcessor = new OneUsaGovStreamProcessor() {
      @Override
      protected JavaDStream<String> openStream(JavaStreamingContext jssc, CommandLine cli) {
        LocalFileMessageStream messageStream =
          new LocalFileMessageStream(testDataFile.getAbsolutePath());
        return jssc.receiverStream(new MessageStreamReceiver(messageStream));
      }
    };

    // process takes a CommandLine ... which kind of sucks for unit testing
    String zkHost = cluster.getZkServer().getZkAddress();
    String[] args = new String[] { "-zkHost", zkHost, "-collection", testCollection };
    CommandLine cli =
      SparkApp.processCommandLineArgs(
        SparkApp.joinCommonAndProcessorOptions(streamProcessor.getOptions()), args);

    streamProcessor.plan(jssc, cli);

    // Actually start processing the stream here ...
    // Note that the SparkApp actually starts the stream processing for the processor impl
    // which makes unit testing processors easier
    jssc.start();

    // let the docs flow through the streaming job
    Thread.sleep(2000);

    // verify docs got indexed ... relies on soft auto-commits firing frequently
    SolrRDD solrRDD = new SolrRDD(zkHost, testCollection);
    JavaRDD<SolrDocument> resultsRDD =
      solrRDD.query(jssc.sparkContext(), new SolrQuery("*:*"), false);

    long numFound = resultsRDD.count();

    assertTrue("expected "+requests.size()+" docs in query results, but got "+numFound,
      numFound == requests.size());

    List<SolrDocument> results = resultsRDD.collect();
    for (OneUsaGovRequest req : requests) {
      String reqId = req.globalBitlyHash; // this relies on the test data not having the same shortened URL
      boolean foundIt = false;
      for (SolrDocument doc : results) {
        if (reqId.equals(doc.getFirstValue("globalBitlyHash_s"))) {
          foundIt = true;
          assertEq(reqId, "userAgent", req.userAgent, doc);
          assertEq(reqId, "countryCode", req.countryCode, doc);
          assertEq(reqId, "encodingUserBitlyHash", req.encodingUserBitlyHash, doc);
          assertEq(reqId, "encodingUserLogin", req.encodingUserLogin, doc);
          assertEq(reqId, "shortUrlCName", req.shortUrlCName, doc);
          assertEq(reqId, "referringUrl", req.referringUrl, doc);
          assertEq(reqId, "longUrl", req.longUrl, doc);
          assertEq(reqId, "geoRegion", req.geoRegion, doc);
          assertEq(reqId, "timezone", req.timezone, doc);
          assertEq(reqId, "geoCityName", req.geoCityName, doc);
          assertEq(reqId, "acceptLanguage", req.acceptLanguage, doc);

          Long timestamp = (Long)doc.getFirstValue("timestamp_l");
          assertNotNull("No timestamp_l field found in Solr doc for "+reqId, timestamp);
          assertTrue(req.timestamp == timestamp.longValue());

          break;
        }
      }
      if (!foundIt)
        fail("document with globalBitlyHash_s == '"+reqId+"' not found in Solr!");
    }
  }

  private void assertEq(String id, String fieldName, String expected, SolrDocument doc) {
    Object actual = doc.getFirstValue(fieldName+"_s");
    assertEquals("Expected ="+expected+" but got '"+actual+"' for req with ID: "+id, expected, actual);
  }

  private Set<OneUsaGovRequest> loadTestData(File testDataFile) throws Exception {
    Set<OneUsaGovRequest> requests = new HashSet<OneUsaGovRequest>();
    BufferedReader br = null;
    String line = null;
    try {
      br = new BufferedReader(new InputStreamReader(new FileInputStream(testDataFile), StandardCharsets.UTF_8));
      while ((line = br.readLine()) != null) {
        String json = line.trim();
        if (json.length() > 0) {
          requests.add(OneUsaGovRequest.parse(json));
        }
      }
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (Exception ignore){}
      }
    }
    return requests;
  }
}
