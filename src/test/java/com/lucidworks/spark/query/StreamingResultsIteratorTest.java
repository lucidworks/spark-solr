package com.lucidworks.spark.query;

import com.lucidworks.spark.RDDProcessorTestBase;
import com.lucidworks.spark.util.SolrQuerySupport;
import com.lucidworks.spark.util.SolrRelationUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;
import com.lucidworks.spark.util.QueryResultsIterator;
import scala.Some$;

public class StreamingResultsIteratorTest extends RDDProcessorTestBase {

  //@Ignore
  @Test
  public void testCursorsWithUnstableIdSort() throws Exception {
    final CloudSolrClient cloudSolrClient = cloudSolrServer; // from base

    String zkHost = cluster.getZkServer().getZkAddress();
    String testCollection = "testStreamingResultsIterator";
    buildCollection(zkHost, testCollection, null, 1);
    cloudSolrClient.setDefaultCollection(testCollection);

    final Random random = new Random(5150);
    final int numDocs = 100;
    final List<String> ids = new ArrayList<String>(numDocs);
    for (int i=0; i < numDocs; i++)
      ids.add(String.valueOf(i));
    Collections.shuffle(ids);

    // need two threads for this: 1) to send docs to Solr with randomized keys
    Thread sendDocsThread = new Thread() {
      @Override
      public void run() {
        for (int i=0; i < numDocs; i++)  {
          SolrInputDocument doc = new SolrInputDocument();
          doc.setField("id", ids.get(i));
          try {
            cloudSolrClient.add(doc, 50);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }

          long sleepMs = random.nextInt(10) * 10L;
          try {
            Thread.sleep(sleepMs);
          } catch (Exception exc) { exc.printStackTrace(); }

          if (i % 10 == 0)
            System.out.println("sendDocsThread has sent "+(i+1)+" docs so far ...");
        }

        System.out.println("sendDocsThread finished sending "+numDocs+" docs");
      }
    };

    SolrQuery solrQuery = new SolrQuery("*:*");
    solrQuery.setFields("id");
    solrQuery.setRows(5);
    solrQuery.setSort(new SolrQuery.SortClause("id", "asc"));
    solrQuery.set("collection", testCollection);

    sendDocsThread.start();
    Thread.sleep(2000);

    //StreamingResultsIterator sri = new StreamingResultsIterator(cloudSolrClient, solrQuery, "*");
    QueryResultsIterator sri = new QueryResultsIterator(cloudSolrClient, solrQuery, "*") ;
    int numDocsFound = 0;
    boolean hasNext = false;
    do {
      Thread.sleep(500);
      hasNext = sri.hasNext();
    } while (hasNext == false);

    while (sri.hasNext()) {
      SolrDocument next = sri.next();
      assertNotNull(next);
      ++numDocsFound;

      // sleep a little to let the underlying results change
      long sleepMs = random.nextInt(10) * 5L;
      try {
        Thread.sleep(sleepMs);
      } catch (Exception exc) { exc.printStackTrace(); }
    }

    try {
      sendDocsThread.interrupt();
    } catch (Exception ignore) {}

    //assertTrue("Iterator didn't return all docs! Num found: "+numDocsFound, numDocs == numDocsFound);
  }
}
