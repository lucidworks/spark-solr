package com.lucidworks.spark.example.hadoop;

import com.lucidworks.spark.SparkApp;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.fail;

public class Logs2SolrRDDProcessorTest {

  @Ignore
  @Test
  public void testRDDProcessor() {
    String[] args = new String[] {
      "logs2solr", "-zkHost", "localhost:9983",
      "-collection", "gettingstarted",
      "-hdfsPath", "hdfs://localhost:9000/user/timpotter/gc_logs",
      "-master", "local[2]", "-v"
    };

    try {
      SparkApp.main(args);
    } catch (Exception exc) {
      exc.printStackTrace();
      fail(getClass().getSimpleName()+" failed due to: "+exc);
    }
  }
}
