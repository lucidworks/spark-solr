package com.lucidworks.spark;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.Before;

/**
 * Base class for tests that need a SolrCloud cluster and a JavaStreamingContext.
 */
public abstract class StreamProcessorTestBase extends TestSolrCloudClusterSupport implements Serializable {

  protected transient JavaStreamingContext jssc;

  @Before
  public void setupSparkStreamingContext() {
    SparkConf conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set("spark.default.parallelism", "1");
    jssc = new JavaStreamingContext(conf, new Duration(500));
  }

  @After
  public void stopSparkStreamingContext() {
    jssc.stop(true, true);
  }

}
