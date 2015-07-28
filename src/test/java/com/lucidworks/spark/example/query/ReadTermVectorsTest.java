package com.lucidworks.spark.example.query;

import com.lucidworks.spark.SparkApp;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.fail;

public class ReadTermVectorsTest {

  @Ignore
  @Test
  public void testQueryProcessor() {
    String[] args = new String[] {
      "term-vectors", "-zkHost", "localhost:9983",
      "-collection", "gettingstarted", "-query", "*:*",
      "-field", "name",
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
