package com.lucidworks.spark.example.query;

import com.lucidworks.spark.SparkApp;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.fail;

public class WordCountTest {

  @Ignore
  @Test
  public void testQueryProcessor() {
    String[] args = new String[] {
      "com.lucidworks.spark.example.query.WordCount", "-zkHost", "localhost:9983",
      "-collection", "gettingstarted", "-query", "*:*",
      "-master", "local[2]"
    };

    try {
      SparkApp.main(args);
    } catch (Exception exc) {
      exc.printStackTrace();
      fail("WordCount failed due to: "+exc);
    }
  }
}
