package com.lucidworks.spark;


import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.*;
import org.junit.Test;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.sql.*;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.*;

import static org.junit.Assert.*;


/**
 * Tests for the SolrRelation implementation.
 */
public class SolrRelationTest extends RDDProcessorTestBase {

  protected transient SQLContext sqlContext;

  @Test
  public void testFilterSupport() throws Exception {

    SQLContext sqlContext = new SQLContext(jsc);

    String[] testData = new String[] {
      "1,a,x,1000,[a;x],[1000]",
      "2,b,y,2000,[b;y],[2000]",
      "3,c,z,3000,[c;z],[3000]",
      "4,a,x,4000,[a;x],[4000]"
    };

    String zkHost = cluster.getZkServer().getZkAddress();
    String testCollection = "testFilterSupport";
    deleteCollection(testCollection);
    deleteCollection("testFilterSupport2");
    buildCollection(zkHost, testCollection, testData, 2);

    Map<String, String> options = new HashMap<String, String>();
    options.put("zkhost", zkHost);
    options.put("collection", testCollection);

    DataFrame df = sqlContext.read().format("solr").options(options).load();

    validateSchema(df);
    //df.show();

    long count = df.count();
    assertCount(testData.length, count, "*:*");

    Row[] rows = df.collect();
    for (int r=0; r < rows.length; r++) {
      Row row = rows[r];
      List val = row.getList(row.fieldIndex("field4_ss"));
      assertNotNull(val);

      List list = new ArrayList();
      list.addAll(val); // clone since we need to sort the entries for testing only
      Collections.sort(list);
      assertTrue(list.size() == 2);
      assertEquals(list.get(0), row.getString(row.fieldIndex("field1_s")));
      assertEquals(list.get(1), row.getString(row.fieldIndex("field2_s")));
    }

    count = df.filter(df.col("field1_s").equalTo("a")).count();
    assertCount(2, count, "field1_s == a");

    count = df.filter(df.col("field3_i").gt(3000)).count();
    assertCount(1, count, "field3_i > 3000");

    count = df.filter(df.col("field3_i").geq(3000)).count();
    assertCount(2, count, "field3_i >= 3000");

    count = df.filter(df.col("field3_i").lt(2000)).count();
    assertCount(1, count, "field3_i < 2000");

    count = df.filter(df.col("field3_i").leq(1000)).count();
    assertCount(1, count, "field3_i <= 1000");

    count = df.filter(df.col("field3_i").gt(2000).and(df.col("field2_s").equalTo("z"))).count();
    assertCount(1, count, "field3_i > 2000 AND field2_s == z");

    count = df.filter(df.col("field3_i").lt(2000).or(df.col("field1_s").equalTo("a"))).count();
    assertCount(2, count, "field3_i < 2000 OR field1_s == a");

    count = df.filter(df.col("field1_s").isNotNull()).count();
    assertCount(4, count, "field1_s IS NOT NULL");

    count = df.filter(df.col("field1_s").isNull()).count();
    assertCount(0, count, "field1_s IS NULL");

    // write to another collection to test writes
    String confName = "testConfig";
    File confDir = new File("src/test/resources/conf");
    int numShards = 2;
    int replicationFactor = 1;
    createCollection("testFilterSupport2", numShards, replicationFactor, 2, confName, confDir);

    options = new HashMap<String, String>();
    options.put("zkhost", zkHost);
    options.put("collection", "testFilterSupport2");

    df.write().format("solr").options(options).mode(SaveMode.Overwrite).save();
    Thread.sleep(1000);

    DataFrame df2 = sqlContext.read().format("solr").options(options).load();
    df2.show();

    deleteCollection(testCollection);
    deleteCollection("testFilterSupport2");
  }

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
    fields1.add(DataTypes.createStructField("testtype_s", DataTypes.createStructType(fields2), true));
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
    df2.registerTempTable("DFTEST");
    sqlContext.sql("SELECT test_s.testtype_s FROM DFTEST").show();
    deleteCollection("testNested");
  }

  public void createMLModelLRParquet() throws Exception {
    List<LabeledPoint> list = new ArrayList<LabeledPoint>();
    LabeledPoint zero = new LabeledPoint(0.0, Vectors.dense(1.0, 0.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0));
    LabeledPoint one = new LabeledPoint(1.0, Vectors.dense(8.0,7.0,6.0,4.0,5.0,6.0,1.0,2.0,3.0));
    list.add(zero);
    list.add(one);
    JavaRDD<LabeledPoint> data = jsc.parallelize(list);
    final LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
              .setNumClasses(2)
              .run(data.rdd());
    model.save(jsc.sc(), "LRParquet");
  }

  public void createMLModelNBParquet() throws Exception {
    List<LabeledPoint> list = new ArrayList<LabeledPoint>();
    LabeledPoint zero = new LabeledPoint(0.0, Vectors.dense(1.0, 0.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0));
    LabeledPoint one = new LabeledPoint(1.0, Vectors.dense(8.0,7.0,6.0,4.0,5.0,6.0,1.0,2.0,3.0));
    list.add(zero);
    list.add(one);
    JavaRDD<LabeledPoint> data = jsc.parallelize(list);
    final NaiveBayesModel model = NaiveBayes.train(data.rdd(), 1.0);
    model.save(jsc.sc(), "NBParquet");
  }

  @Test
  public void loadLRParquetIntoSolr() throws Exception {
    createMLModelLRParquet();
    SQLContext sqlContext = new SQLContext(jsc);
    String confName = "testConfig";
    File confDir = new File("src/test/resources/conf");
    int numShards = 2;
    int replicationFactor = 1;
    deleteCollection("TestLR");
    Thread.sleep(1000);
    createCollection("TestLR", numShards, replicationFactor, 2, confName, confDir);
    String zkHost = cluster.getZkServer().getZkAddress();
    DataFrame dfLR = sqlContext.load("LRParquet/data/");
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
    assertCount(dfLR.count(), dfLR.intersect(dfLR2).count(), "compare dataframe count");
    deleteCollection("TestLR");
    Thread.sleep(1000);
    File lRModel = new File("LRParquet").getAbsoluteFile();
    FileUtils.forceDelete(lRModel);
  }

  @Test
  public void loadNBParquetIntoSolr() throws Exception {
    createMLModelNBParquet();
    SQLContext sqlContext = new SQLContext(jsc);
    String confName = "testConfig";
    File confDir = new File("src/test/resources/conf");
    int numShards = 2;
    int replicationFactor = 1;
    deleteCollection("TestNB");
    Thread.sleep(1000);
    createCollection("TestNB", numShards, replicationFactor, 2, confName, confDir);
    String zkHost = cluster.getZkServer().getZkAddress();
    DataFrame dfNB = sqlContext.load("NBParquet/data/");
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
    assertCount(dfNB.count(), dfNB.intersect(dfNB2).count(), "compare dataframe count");
    deleteCollection("TestNB");
    Thread.sleep(1000);
    File nBModel = new File("NBParquet").getAbsoluteFile();
    FileUtils.forceDelete(nBModel);
  }

  protected void assertCount(long expected, long actual, String expr) {
    assertTrue("expected count == " + expected + " but got " + actual + " for " + expr, expected == actual);
  }

  protected void validateSchema(DataFrame df) {
    df.printSchema();
    StructType schema = df.schema();
    assertNotNull(schema);
    String[] expectedSchemaFields = new String[]{"id","field1_s","field2_s","field3_i","field4_ss","field5_ii"};
    Map<String,StructField> schemaFields = new HashMap<String, StructField>();
    for (StructField sf : schema.fields())
      schemaFields.put(sf.name(), sf);

    for (String fieldName : expectedSchemaFields) {
      StructField field = schemaFields.get(fieldName);
      if (field == null)
        fail("Expected schema field '" + fieldName + "' not found! Schema is: " + schema.prettyJson());
      DataType type = field.dataType();
      if (fieldName.equals("id") || fieldName.endsWith("_s")) {
        assertEquals("Field '" + fieldName + "' should be a string but has type '" + type + "' instead!", "string", type.typeName());
      } else if (fieldName.endsWith("_i")) {
        assertEquals("Field '" + fieldName + "' should be an integer but has type '" + type + "' instead!", "integer", type.typeName());
      } else if (fieldName.endsWith("_ss")) {
        assertEquals("Field '"+fieldName+"' should be an array but has '"+type+"' instead!", "array", type.typeName());
        ArrayType arrayType = (ArrayType)type;
        assertEquals("Field '"+fieldName+"' should have a string element type but has '"+arrayType.elementType()+
          "' instead!", "string", arrayType.elementType().typeName());
      } else if (fieldName.endsWith("_ii")) {
        assertEquals("Field '"+fieldName+"' should be an array but has '"+type+"' instead!", "array", type.typeName());
        ArrayType arrayType = (ArrayType)type;
        assertEquals("Field '"+fieldName+"' should have an integer element type but has '"+arrayType.elementType()+
          "' instead!", "integer", arrayType.elementType().typeName());
      }
    }
  }
}
