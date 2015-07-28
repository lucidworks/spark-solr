package com.lucidworks.spark.example.query;

import com.lucidworks.spark.SparkApp;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.fail;

public class SolrQueryProcessorTest {

  @Ignore
  @Test
  public void testQueryProcessor() {
    String[] args = new String[] {
      "query-solr", "-zkHost", "localhost:9983",
      "-collection", "gettingstarted", "-query", "*:*",
      "-master", "local[2]"
    };

    try {
      SparkApp.main(args);
    } catch (Exception exc) {
      exc.printStackTrace();
      fail("QueryProcessor failed due to: "+exc);
    }
  }
}
