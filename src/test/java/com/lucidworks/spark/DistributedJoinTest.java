package com.lucidworks.spark;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.fail;

public class DistributedJoinTest {
  @Ignore
  @Test
  public void testJoin() {
    String[] args = new String[] {
      "join-solr", "-zkHost", "localhost:9983",
      "-collection", "gettingstarted", "-query", "*:*",
      "-master", "local[4]", "-v"
    };

    try {
      SparkApp.main(args);
    } catch (Exception exc) {
      exc.printStackTrace();
      fail("DistributedSolrJoin failed due to: "+exc);
    }
  }
}
