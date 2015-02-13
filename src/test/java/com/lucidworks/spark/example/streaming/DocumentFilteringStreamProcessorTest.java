package com.lucidworks.spark.example.streaming;

import com.lucidworks.spark.SparkApp;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.fail;

public class DocumentFilteringStreamProcessorTest  {

  @Ignore
  @Test
  public void testIndexing() throws Exception {
    String[] args = new String[] {
      "docfilter", "-zkHost", "localhost:9983",
      "-collection", "gettingstarted",
      "-master", "local[2]", "-v"
    };

    try {
      SparkApp.main(args);
    } catch (Exception exc) {
      exc.printStackTrace();
      fail("QueryProcessor failed due to: "+exc);
    }
  }
}
