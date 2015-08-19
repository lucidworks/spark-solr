package com.lucidworks.spark;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.*;
import org.junit.Test;

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

  protected void assertCount(long expected, long actual, String expr) {
    assertTrue("expected count == "+expected+" but got "+actual+" for "+expr, expected == actual);
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
