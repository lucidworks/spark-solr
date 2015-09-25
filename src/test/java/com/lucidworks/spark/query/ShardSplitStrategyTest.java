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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ShardSplitStrategyTest extends RDDProcessorTestBase {

  public static Logger log = Logger.getLogger(ShardSplitStrategyTest.class);

  protected String createTestWord(Random rand) {
    String word = "";
    for (int i=0; i < 3; i++) {
      char charAt =
          StringFieldShardSplitStrategy.alphaChars.charAt(rand.nextInt(StringFieldShardSplitStrategy.alphaChars.length()));
      word += charAt;
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

    SolrRDD solrRDD = new SolrRDD(zkHost, collection);
    List<String> shardList = solrRDD.buildShardList(cloudSolrServer);

    SolrQuery solrQuery = new SolrQuery("*:*");
    String shardUrl = shardList.get(0);

    // try various split sizes
    for (int i=1; i <= 6; i++) {
      // split on _version_ field
      int desiredSplits = i*5;
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
    assertTrue("Expected ~" + desiredSplits + " splits, but found " + splits.size(),
        (desiredSplits - 1) <= splits.size() && splits.size() <= (desiredSplits + 1));

    solrQuery.setRows(expNumDocs);
    Map<String,String> docIdSet = new HashMap<String,String>();
    int numDocs = 0;
    for (ShardSplit ss : splits) {
      QueryResponse qr = solrRDD.querySolr(SolrSupport.getHttpSolrClient(ss.getShardUrl()), ss.getSplitQuery(), 0, null);
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
        fail("Doc with id: " + expId + " not found!");
      }
    }
    assertTrue("Expected "+expNumDocs+" but splits only found "+numDocs, expNumDocs == numDocs);

  }
}
