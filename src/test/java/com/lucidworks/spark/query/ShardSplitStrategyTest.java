package com.lucidworks.spark.query;

import com.lucidworks.spark.RDDProcessorTestBase;
import com.lucidworks.spark.SolrRDD;
import com.lucidworks.spark.SolrSupport;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ShardSplitStrategyTest extends RDDProcessorTestBase {

  public static Logger log = Logger.getLogger(ShardSplitStrategyTest.class);

  protected String createTestWord(Random rand) {
    String word = "";
    for (int i=0; i < 3; i++) {
      int randCharIndex = rand.nextInt(StringFieldShardSplitStrategy.alpha.length);
      word += StringFieldShardSplitStrategy.alpha[randCharIndex];
    }
    return word;
  }

  @Test
  public void testSplitStrategy() throws Exception {

    String zkHost = cluster.getZkServer().getZkAddress();

    String collection = "testStringSplit";

    Random rand = new Random(5150);

    String[] inputDocs = new String[2000];
    for (int d=0; d < inputDocs.length; d++) {
      String word = createTestWord(rand);
      inputDocs[d] = "d"+d+","+word+",bar,1,[a;b],[1;2]";
      inputDocs[++d] = "d"+d+","+word.substring(0,2)+",bar,1,[a;b],[1;2]";
    }
    buildCollection(zkHost, collection, inputDocs, 1);

    // verify the _version_ field is sane
    SolrQuery q = new SolrQuery("*:*");
    q.set("collection", collection);
    q.set("distrib", false);
    q.setRows(1); // top 1
    q.addSort("_version_", SolrQuery.ORDER.asc);
    q.setGetFieldStatistics("_version_");
    QueryResponse qr = cloudSolrServer.query(q);
    SolrDocument doc = qr.getResults().get(0);
    Long minFromSort = (Long)doc.getFirstValue("_version_");
    Long minFromStats = ((Double)qr.getFieldStatsInfo().get("_version_").getMin()).longValue();
    Long maxFromStats = ((Double)qr.getFieldStatsInfo().get("_version_").getMax()).longValue();
    Long numFoundFromStats = qr.getFieldStatsInfo().get("_version_").getCount();

    q.removeSort("_version_");
    q.addSort("_version_", SolrQuery.ORDER.desc);
    qr = cloudSolrServer.query(q);
    doc = qr.getResults().get(0);
    Long maxFromSort = (Long)doc.getFirstValue("_version_");

    q = new SolrQuery("*:*");
    q.addFilterQuery("_version_:[" + minFromStats + " TO " + maxFromStats + "]");
    q.set("collection", collection);
    q.set("distrib", false);
    qr = cloudSolrServer.query(q);
    Long numFoundFromQuery = qr.getResults().getNumFound();

    assertTrue(numFoundFromStats == inputDocs.length);
    assertTrue(numFoundFromStats.longValue() == numFoundFromQuery.longValue());
    assertTrue(minFromSort.longValue() == minFromStats.longValue());
    assertTrue(maxFromSort.longValue() == maxFromStats.longValue());

    SolrRDD solrRDD = new SolrRDD(zkHost, collection);
    List<String> shardList = solrRDD.buildShardList(cloudSolrServer);

    SolrQuery solrQuery = new SolrQuery("*:*");
    solrQuery.addFilterQuery("id:[* TO *]");
    solrQuery.addFilterQuery("field2_s:bar");
    String shardUrl = shardList.get(0);

    // try various split sizes
    for (int i=1; i <= 9; i++) {
      // split on _version_ field
      int desiredSplits = i*3;
      verifySplits(solrRDD, inputDocs.length, shardUrl, new NumberFieldShardSplitStrategy(), "_version_", desiredSplits, solrQuery);

      // split on string field
      verifySplits(solrRDD, inputDocs.length, shardUrl, new StringFieldShardSplitStrategy(), "field1_s", desiredSplits, solrQuery);
    }
  }

  protected void verifySplits(SolrRDD solrRDD,
                              int expNumDocs,
                              String shardUrl,
                              ShardSplitStrategy splitStrategy,
                              String splitField,
                              int desiredSplits,
                              SolrQuery solrQuery)
      throws Exception
  {
    List<ShardSplit> splits = splitStrategy.getSplits(shardUrl, solrQuery, splitField, desiredSplits);

    log.info("Created " + splits.size() + " splits using " + splitStrategy.getClass().getSimpleName());
    for (ShardSplit split : splits) {
      log.info(split);
    }

    // number of splits is not exact ~ allow +/- 1 from desired for this test
    //assertTrue("Expected ~" + desiredSplits + " splits, but found " + splits.size(),
    //    (desiredSplits - 2) <= splits.size() && splits.size() <= (desiredSplits + 2));

    solrQuery.setRows(expNumDocs);
    Map<String,String> docIdSet = new HashMap<String,String>();
    int numDocs = 0;
    for (ShardSplit ss : splits) {
      SolrQuery splitQuery = ss.getSplitQuery();
      String splitFq = ss.getSplitFilterQuery();
      String[] fqs = splitQuery.getFilterQueries();
      assertTrue(fqs.length == 3);
      for (String fq : fqs) {
        if (fq.startsWith("id")) {
          assertEquals("id:[* TO *]", fq);
        } else if (fq.startsWith("field2_s")) {
          assertEquals("field2_s:bar", fq);
        } else if (fq.startsWith(splitField)) {
          assertEquals(splitFq, fq);
        } else {
          fail("Unexpected filter query in split query: "+fq+"; query="+splitQuery);
        }
      }

      QueryResponse qr = solrRDD.querySolr(SolrSupport.getHttpSolrClient(ss.getShardUrl()), splitQuery, 0, null);
      SolrDocumentList docList = qr.getResults();
      numDocs += docList.getNumFound();
      for (SolrDocument doc : docList) {
        String docId = (String)doc.getFirstValue("id");
        docIdSet.put(docId, String.valueOf(doc.getFirstValue(splitField)));
      }
    }

    for (int d=0; d < expNumDocs; d++) {
      String expId = "d"+d;
      if (!docIdSet.containsKey(expId)) {
        fail("Doc with id: " + expId + " not found using "+splitStrategy.getClass().getSimpleName());
      }
    }
    assertTrue("Expected "+expNumDocs+" but splits only found "+numDocs, expNumDocs == numDocs);

  }
}
