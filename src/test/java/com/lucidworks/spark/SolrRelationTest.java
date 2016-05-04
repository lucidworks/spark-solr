package com.lucidworks.spark;

import com.lucidworks.spark.util.Constants;
import com.lucidworks.spark.util.SolrSupport;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.*;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.spark.sql.functions;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.*;

import static com.lucidworks.spark.util.ConfigurationConstants.*;
import static org.junit.Assert.*;

/**
 * Tests for the SolrRelation implementation.
 */
public class SolrRelationTest extends RDDProcessorTestBase {

  @Test
  public void testSampleIndex() throws Exception {
    String testCollection = "testSampleIndex";
    try {
      SQLContext sqlContext = new SQLContext(jsc);

      deleteCollection(testCollection);
      int numShards = 3;
      int numDocs = 100;
      String zkHost = cluster.getZkServer().getZkAddress();
      buildCollection(zkHost, testCollection, numDocs, numShards);

      Map<String, String> options = new HashMap<String, String>();
      options.put(SOLR_ZK_HOST_PARAM(), zkHost);
      options.put(SOLR_COLLECTION_PARAM(), testCollection);
      options.put(SAMPLE_SEED(), "5150");
      options.put(SAMPLE_PCT(), "0.1");
      DataFrame fromSolr = sqlContext.read().format(Constants.SOLR_FORMAT()).options(options).load();
      long count = fromSolr.count();

      System.out.println("\n\n"+count+"\n\n");
      assertTrue(count >= 8 && count <= 12); // not exact because of shard imbalance

      deleteCollection(testCollection);
    } finally {
      deleteCollection(testCollection);
    }
  }

  @Test
  public void testIndexOneusagovDataFrame() throws Exception {
    String testCollection = "testIndexOneusagovDataFrame";
    try {
      SQLContext sqlContext = new SQLContext(jsc);

      // load test data from json file to index into Solr
      DataFrame eventsDF = sqlContext.read().json("src/test/resources/test-data/oneusagov/oneusagov_sample.json");
      eventsDF = eventsDF.withColumnRenamed("_id", "id");

      sqlContext.udf().register("secs2ts", new UDF1<Long, Timestamp>() {
        public Timestamp call(final Long secs) throws Exception {
          return (secs != null) ? new Timestamp(secs * 1000) : null;
        }
      }, DataTypes.TimestampType);
      eventsDF = eventsDF.withColumn("ts", functions.callUDF("secs2ts", eventsDF.col("t"))).drop("t");

      eventsDF.printSchema();

      deleteCollection(testCollection);
      String confName = "testConfig";
      File confDir = new File("src/test/resources/conf");
      int numShards = 1;
      int replicationFactor = 1;
      createCollection(testCollection, numShards, replicationFactor, numShards /* maxShardsPerNode */, confName, confDir);
      validateDataFrameStoreLoad(sqlContext, testCollection, eventsDF);
    } finally {
      deleteCollection(testCollection);
    }
  }

  //@Ignore
  @Test
  public void testFlattenMultivalued() throws Exception {
    String testCollection = "testFlattenMultivalued";
    try {
      SQLContext sqlContext = new SQLContext(jsc);

      deleteCollection(testCollection);
      String confName = "testConfig";
      File confDir = new File("src/test/resources/conf");
      int numShards = 1;
      int replicationFactor = 1;
      createCollection(testCollection, numShards, replicationFactor, numShards /* maxShardsPerNode */, confName, confDir);

      Date now = new Date();

      SolrInputDocument doc = new SolrInputDocument();
      doc.setField("id", "flatten-1");
      byte[] rawContentBytes = "this is the value of the _raw_content_ field".getBytes(StandardCharsets.UTF_8);
      doc.setField("_raw_content_", DatatypeConverter.printBase64Binary(rawContentBytes));
      byte[] imagesBytes = "this is the value of the images field".getBytes(StandardCharsets.UTF_8);
      doc.addField("images", DatatypeConverter.printBase64Binary(imagesBytes));
      doc.addField("ts_tdts", now);
      cloudSolrServer.add(testCollection, doc);
      cloudSolrServer.commit(testCollection);

      String zkHost = cluster.getZkServer().getZkAddress();
      Map<String, String> options = new HashMap<String, String>();
      options.put(SOLR_ZK_HOST_PARAM(), zkHost);
      options.put(SOLR_COLLECTION_PARAM(), testCollection);
      options.put(FLATTEN_MULTIVALUED(), "true");
      options.put(SOLR_FIELD_PARAM(), "id, _raw_content_, images, ts_tdts");

      SolrQuery q = new SolrQuery("*:*");
      q.setRows(100);
      q.addSort("id", SolrQuery.ORDER.asc);
      dumpSolrCollection(testCollection, q);

      // now read the data back from Solr and validate that it was saved correctly and that all data type handling is correct
      DataFrame fromSolr = sqlContext.read().format(Constants.SOLR_FORMAT()).options(options).load();
      fromSolr.printSchema();

      List<Row> rows = fromSolr.collectAsList();
      assertTrue(rows.size() == 1);
      Row first = rows.get(0);
      assertEquals(doc.getFieldValue("id"), first.get(first.fieldIndex("id")));

      // compare the bytes in the images field, which proves the multivalued field was flattened correctly
      byte[] images = (byte[])first.get(first.fieldIndex("images"));
      String imagesFromSolr = new String(images, StandardCharsets.UTF_8);
      assertEquals(new String(imagesBytes, StandardCharsets.UTF_8), imagesFromSolr);

      assertEquals(now, first.get(first.fieldIndex("ts_tdts")));
    } finally {
      deleteCollection(testCollection);
    }
  }

  //@Ignore
  @Test
  public void testEventsDataFrame() throws Exception {
    String testCollection = "testEventsDataFrame";
    try {
      SQLContext sqlContext = new SQLContext(jsc);

      // load test data from json file to index into Solr
      DataFrame eventsDF = sqlContext.read().json("src/test/resources/test-data/events.json");
      eventsDF = eventsDF.select("id", "count_l", "doc_id_s", "flag_s", "session_id_s", "type_s", "tz_timestamp_txt", "user_id_s", "`params.title_s`");

      deleteCollection(testCollection);
      String confName = "testConfig";
      File confDir = new File("src/test/resources/conf");
      int numShards = 1;
      int replicationFactor = 1;
      createCollection(testCollection, numShards, replicationFactor, numShards /* maxShardsPerNode */, confName, confDir);
      validateDataFrameStoreLoad(sqlContext, testCollection, eventsDF);
    } finally {
      deleteCollection(testCollection);
    }
  }

  //@Ignore
  @Test
  public void testAggDataFrame() throws Exception {
    String testCollection = "testAggDataFrame";
    try {
      SQLContext sqlContext = new SQLContext(jsc);

      // load test data from json file to index into Solr
      DataFrame aggDF = sqlContext.read().json("src/test/resources/test-data/em_sample.json");
      aggDF = aggDF.select("id","aggr_count_l","aggr_id_s","aggr_job_id_s","aggr_type_s",
        "co_occurring_docIds_counts_ls","co_occurring_docIds_ss","entity_id_s","entity_type_s",
        "flag_s","grouping_key_s","in_session_ids_counts_ls","in_session_ids_ss","in_user_id_s",
        "in_user_id_s_counts_ls","in_user_ids_counts_ls","in_user_ids_ss","out_clicks_counts_ls",
        "out_clicks_ss","out_session_ids_counts_ls","out_session_ids_ss");

      aggDF.printSchema();

      deleteCollection(testCollection);
      String confName = "testConfig";
      File confDir = new File("src/test/resources/conf");
      int numShards = 1;
      int replicationFactor = 1;
      createCollection(testCollection, numShards, replicationFactor, numShards /* maxShardsPerNode */, confName, confDir);
      validateDataFrameStoreLoad(sqlContext, testCollection, aggDF);
    } finally {
      deleteCollection(testCollection);
    }
  }

  //@Ignore
  @Test
  public void testMVDateHandling() throws Exception {
    SQLContext sqlContext = new SQLContext(jsc);
    String testCollection = "testMVDateHandling";

    int numShards = 1;
    String zkHost = cluster.getZkServer().getZkAddress();

    String[] testData = new String[] {
            "1,a,x,1000,[a;x],[1000],[2016-01-02T03:04:05.006Z,2016-02-02T03:04:05.006Z]",
            "2,b,y,2000,[b;y],[2000],[2016-01-02T03:04:05.006Z,2016-02-02T03:04:05.006Z]",
            "3,c,z,3000,[c;z],[3000],[2016-01-02T03:04:05.006Z,2016-02-02T03:04:05.006Z]",
            "4,a,x,4000,[a;x],[4000],[2016-01-02T03:04:05.006Z,2016-02-02T03:04:05.006Z]"
    };
    buildCollection(zkHost, testCollection, testData, numShards);

    SolrQuery q = new SolrQuery("*:*");
    q.setRows(100);
    q.addSort("id", SolrQuery.ORDER.asc);
    dumpSolrCollection(testCollection, q);

    Map<String, String> options = new HashMap<String, String>();
    options.put(SOLR_ZK_HOST_PARAM(), zkHost);
    options.put(SOLR_COLLECTION_PARAM(), testCollection);
    options.put(FLATTEN_MULTIVALUED(), "false");

    // now read the data back from Solr and validate that it was saved correctly and that all data type handling is correct
    DataFrame fromSolr = sqlContext.read().format(Constants.SOLR_FORMAT()).options(options).load();
    fromSolr = fromSolr.sort("id");
    fromSolr.printSchema();

    Row[] docsFromSolr = fromSolr.collect();
    assertTrue(docsFromSolr.length == 4);
  }

  //@Ignore
  @Test
  public void testFilterSupport() throws Exception {
    String testCollection = "testFilterSupport";
    String testCollection2 = "testFilterSupport2";
    try {
      SQLContext sqlContext = new SQLContext(jsc);

      String[] testData = new String[] {
        "1,a,x,1000,[a;x],[1000]",
        "2,b,y,2000,[b;y],[2000]",
        "3,c,z,3000,[c;z],[3000]",
        "4,a,x,4000,[a;x],[4000]"
      };

      String zkHost = cluster.getZkServer().getZkAddress();
      buildCollection(zkHost, testCollection, testData, 2);

      Map<String, String> options = new HashMap<String, String>();
      options.put(SOLR_ZK_HOST_PARAM(), zkHost);
      options.put(SOLR_COLLECTION_PARAM(), testCollection);
      options.put(FLATTEN_MULTIVALUED(), "false");

      DataFrame df = sqlContext.read().format("solr").options(options).load();
      df.show();
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

      count = df.filter(df.col("field1_s").equalTo("a")).count();
      assertCount(2, count, "field1_s <=> a");

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
      createCollection(testCollection2, numShards, replicationFactor, 2, confName, confDir);

      options = new HashMap<String, String>();
      options.put(SOLR_ZK_HOST_PARAM(), zkHost);
      options.put(SOLR_COLLECTION_PARAM(), testCollection2);
      options.put(FLATTEN_MULTIVALUED(), "false");

      df.write().format("solr").options(options).mode(SaveMode.Overwrite).save();
      Thread.sleep(1000);

      DataFrame df2 = sqlContext.read().format("solr").options(options).load();
      df2.show();
    } finally {
      deleteCollection(testCollection);
      deleteCollection(testCollection2);
    }
  }

  protected static String array2cdl(String[] arr) {
    // this is really horrible
    String str = Arrays.asList(arr).toString();
    return str.substring(1, str.length() - 1).replaceAll(" ", "");
  }


  protected static Row[] validateDataFrameStoreLoad(SQLContext sqlContext, String testCollection, DataFrame sourceData) throws Exception {
    String idFieldName = "id";

    sourceData = sourceData.sort(idFieldName);
    sourceData.printSchema();
    Row[] testData = sourceData.collect();
    String[] cols = sourceData.columns();

    String zkHost = cluster.getZkServer().getZkAddress();
    Map<String, String> options = new HashMap<String, String>();
    options.put(SOLR_ZK_HOST_PARAM(), zkHost);
    options.put(SOLR_COLLECTION_PARAM(), testCollection);
    sourceData.write().format(Constants.SOLR_FORMAT()).options(options).mode(SaveMode.Overwrite).save();

    // Explicit commit to make sure all docs are visible
    CloudSolrClient solrCloudClient = SolrSupport.getCachedCloudClient(zkHost);
    solrCloudClient.commit(testCollection, true, true);

    SolrQuery q = new SolrQuery("*:*");
    q.setRows(100);
    q.addSort(idFieldName, SolrQuery.ORDER.asc);
    dumpSolrCollection(testCollection, q);

    // now read the data back from Solr and validate that it was saved correctly and that all data type handling is correct
    options.put(SOLR_FIELD_PARAM(), array2cdl(cols));
    options.put(FLATTEN_MULTIVALUED(), "false");

    System.out.println("\n\n>> reading data from Solr using options: "+options+"\n\n");

    DataFrame fromSolr = sqlContext.read().format(Constants.SOLR_FORMAT()).options(options).load();
    fromSolr = fromSolr.sort(idFieldName);
    fromSolr.printSchema();

    Row[] docsFromSolr = fromSolr.collect();
    Set<String> solrCols = new TreeSet<>();
    solrCols.addAll(Arrays.asList(fromSolr.columns()));
    for (String col : cols) {
      if (!solrCols.contains(col)) {
        assertTrue("expected "+col+" in Solr DataFrame, but only found: "+solrCols+", source cols: "+Arrays.asList(cols), solrCols.contains(col));
      }
    }

    long actualEvents = docsFromSolr.length;
    assertTrue("Expected " + testData.length + " docs from Solr, but found: " + actualEvents, actualEvents == testData.length);
    for (int e=0; e < testData.length; e++) {
      Row exp = testData[e];
      Row doc = docsFromSolr[e];
      for (String col : cols) {
        Object expVal = exp.get(exp.fieldIndex(col));
        Object actVal = doc.get(doc.fieldIndex(col));
        assertEquals("Value mismatch for col "+col+" at row "+e, expVal, actVal);
      }
    }

    return docsFromSolr;
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
        assertEquals("Field '" + fieldName + "' should be an integer but has type '" + type + "' instead!", "long", type.typeName());
      } else if (fieldName.endsWith("_ss")) {
        assertEquals("Field '"+fieldName+"' should be an array but has '"+type+"' instead!", "array", type.typeName());
        ArrayType arrayType = (ArrayType)type;
        assertEquals("Field '"+fieldName+"' should have a string element type but has '"+arrayType.elementType()+
          "' instead!", "string", arrayType.elementType().typeName());
      } else if (fieldName.endsWith("_ii")) {
        assertEquals("Field '"+fieldName+"' should be an array but has '"+type+"' instead!", "array", type.typeName());
        ArrayType arrayType = (ArrayType)type;
        assertEquals("Field '"+fieldName+"' should have an integer element type but has '"+arrayType.elementType()+
          "' instead!", "long", arrayType.elementType().typeName());
      }
    }
  }
}
