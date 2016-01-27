package com.lucidworks.spark;

import com.lucidworks.spark.util.EventsimUtil;
import com.lucidworks.spark.util.SolrSchemaUtil;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.lucidworks.spark.util.ConfigurationConstants.SOLR_COLLECTION_PARAM;
import static com.lucidworks.spark.util.ConfigurationConstants.SOLR_QUERY_PARAM;
import static com.lucidworks.spark.util.ConfigurationConstants.SOLR_ZK_HOST_PARAM;

public class SolrSqlTest extends RDDProcessorTestBase{


  /**
   * 1. Create a collection
   * 2. Modify the schema to enable docValues for some fields
   * 3. Reload the collection
   * 4. Index sample dataset
   * 5. Do a series of SQL queries and make sure they return valid results
   * 6.
   * @throws Exception
   */
  //@Ignore
  @Test
  public void testSQLQueries() throws Exception {
    String testCollectionName = "testSQLQueries-eventsim";
    String zkHost = cluster.getZkServer().getZkAddress();

    deleteCollection(testCollectionName);
    buildCollection(zkHost, testCollectionName, null, 2);
    EventsimUtil.defineSchemaForEventSim(zkHost, testCollectionName);
    EventsimUtil.loadEventSimDataSet(zkHost, testCollectionName);

    SQLContext sqlContext = new SQLContext(jsc.sc());
    HashMap<String, String> options = new HashMap<>();

    options.put(SOLR_ZK_HOST_PARAM, zkHost);
    options.put(SOLR_COLLECTION_PARAM, testCollectionName);
    options.put(SOLR_QUERY_PARAM, "*:*");

    {
      DataFrame eventsim = sqlContext.read().format("solr").options(options).load();
      eventsim.registerTempTable("eventsim");

      DataFrame records = sqlContext.sql("SELECT * FROM eventsim");
      StructType schema = records.schema();

      long numRecords = records.count();
      assert numRecords == 1000;

      String[] fieldNames = schema.fieldNames();
      // list of fields are present in src/test/resources/eventsim/fields_schema.json
      assert fieldNames.length == 18 + 1 + 1; // extra fields are id and _version_

      // assert the schema for specific fields 'timestamp', 'length', 'status', 'registration'
      StructType baseSchema = SolrSchemaUtil.getBaseSchema(zkHost, testCollectionName, false);
      StructField timestamp = schema.apply("timestamp");
      assert timestamp.dataType().typeName().equals(DataTypes.TimestampType.typeName());
    }

    // Filter for some fields
    {
      DataFrame eventsim = sqlContext.read().format("solr").options(options).load();
      eventsim.registerTempTable("eventsim");

      DataFrame records = sqlContext.sql("SELECT `userId`, `timestamp` from eventsim WHERE `gender` = 'M'");
      assert records.count() == 567;
//      Row[] rows = records.collect();
    }

  }

  @Test(expected=IllegalArgumentException.class)
  public void testInvalidOptions() {
    SQLContext sqlContext = new SQLContext(jsc.sc());
    sqlContext.read().format("solr").load();
  }

  @Test(expected=IllegalArgumentException.class)
  public void testInvalidCollectionOption() {
    SQLContext sqlContext = new SQLContext(jsc.sc());

    Map<String, String> options = Collections.singletonMap("zkHost", cluster.getZkServer().getZkAddress());
    sqlContext.read().format("solr").options(options).load();
  }
}
