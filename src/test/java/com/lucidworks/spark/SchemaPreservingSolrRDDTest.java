package com.lucidworks.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.linalg.Vectors;

import static org.junit.Assert.assertTrue;

/**
 * Tests for the schemaPreservingSolrRDD implementation
 */
public class SchemaPreservingSolrRDDTest extends RDDProcessorTestBase {
  protected transient SQLContext sqlContext;

  @Test
  public void testNestedDataFrames() throws Exception {
    SQLContext sqlContext = new SQLContext(jsc);
    String confName = "testConfig";
    File confDir = new File("src/test/resources/conf");
    int numShards = 2;
    int replicationFactor = 1;
    deleteCollection("testNested");
    createCollection("testNested", numShards, replicationFactor, 2, confName, confDir);
    List<StructField> fields = new ArrayList<StructField>();
    List<StructField> fields1 = new ArrayList<StructField>();
    List<StructField> fields2 = new ArrayList<StructField>();
    fields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("testing_s", DataTypes.StringType, true));
    fields1.add(DataTypes.createStructField("test1_s", DataTypes.StringType, true));
    fields1.add(DataTypes.createStructField("test2_s", DataTypes.StringType, true));
    fields2.add(DataTypes.createStructField("test11_s", DataTypes.StringType, true));
    fields2.add(DataTypes.createStructField("test12_s", DataTypes.StringType, true));
    fields2.add(DataTypes.createStructField("test13_s", DataTypes.StringType, true));
    fields1.add(DataTypes.createStructField("testtype_s", DataTypes.createStructType(fields2) , true));
    fields.add(DataTypes.createStructField("test_s", DataTypes.createStructType(fields1), true));
    StructType schema = DataTypes.createStructType(fields);
    Row dm = RowFactory.create("7", "test", RowFactory.create("test1", "test2", RowFactory.create("test11", "test12", "test13")));
    List<Row> list = new ArrayList<Row>();
    list.add(dm);
    JavaRDD<Row> rdd = jsc.parallelize(list);
    DataFrame df = sqlContext.createDataFrame(rdd, schema);
    HashMap<String, String> options = new HashMap<String, String>();
    String zkHost = cluster.getZkServer().getZkAddress();
    options = new HashMap<String, String>();
    options.put("zkhost", zkHost);
    options.put("collection", "testNested");
    options.put("preserveschema", "Y");
    df.write().format("solr").options(options).mode(SaveMode.Overwrite).save();
    Thread.sleep(1000);
    DataFrame df2 = sqlContext.read().format("solr").options(options).load();
    df2 = sqlContext.createDataFrame(df2.javaRDD(),df2.schema());
    df.show();
    df2.show();
    deleteCollection("testNested");
  }

  @Test
  public void loadLRParquetIntoSolr() throws Exception {
    SQLContext sqlContext = new SQLContext(jsc);
    String confName = "testConfig";
    File confDir = new File("src/test/resources/conf");
    int numShards = 2;
    int replicationFactor = 1;
    deleteCollection("TestLR");
    Thread.sleep(1000);
    createCollection("TestLR", numShards, replicationFactor, 2, confName, confDir);
    String zkHost = cluster.getZkServer().getZkAddress();
    DataFrame dfLR = sqlContext.load("data/LRParquet" + jsc.version() + "/data/");
    HashMap<String, String> options = new HashMap<String, String>();
    options = new HashMap<String, String>();
    options.put("zkhost", zkHost);
    options.put("collection", "TestLR");
    options.put("preserveschema", "Y");
    dfLR.write().format("solr").options(options).mode(SaveMode.Overwrite).save();
    dfLR.show();
    dfLR.printSchema();
    Thread.sleep(5000);
    DataFrame dfLR2 = sqlContext.read().format("solr").options(options).load();
    dfLR2.show();
    dfLR2.printSchema();
    assertCount(dfLR.javaRDD().count(), dfLR.intersect(dfLR2).javaRDD().count(), "compare dataframe count");
    deleteCollection("TestLR");
    Thread.sleep(1000);
  }

  @Test
  public void loadNBParquetIntoSolr() throws Exception {
    SQLContext sqlContext = new SQLContext(jsc);
    String confName = "testConfig";
    File confDir = new File("src/test/resources/conf");
    int numShards = 2;
    int replicationFactor = 1;
    deleteCollection("TestNB");
    Thread.sleep(1000);
    createCollection("TestNB", numShards, replicationFactor, 2, confName, confDir);
    String zkHost = cluster.getZkServer().getZkAddress();
    DataFrame dfNB = null;
    dfNB = sqlContext.load("data/NBParquet" + jsc.version() + "/data/");
    HashMap<String, String> options = new HashMap<String, String>();
    options = new HashMap<String, String>();
    options.put("zkhost", zkHost);
    options.put("preserveschema", "Y");
    options.put("collection", "TestNB");
    dfNB.write().format("solr").options(options).mode(SaveMode.Overwrite).save();
    dfNB.show();
    Thread.sleep(5000);
    DataFrame dfNB2 = sqlContext.read().format("solr").options(options).load();
    dfNB2.show();
    dfNB.printSchema();
    dfNB2.printSchema();
    assertCount(dfNB.javaRDD().count(), dfNB.intersect(dfNB2).javaRDD().count(), "compare dataframe count");
    deleteCollection("TestNB");
    Thread.sleep(1000);
  }

  protected void assertCount(long expected, long actual, String expr) {
    assertTrue("expected count == "+expected+" but got "+actual+" for "+expr, expected == actual);
  }
}

