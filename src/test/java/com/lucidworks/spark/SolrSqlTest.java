package com.lucidworks.spark;

import com.lucidworks.spark.util.EventsimUtil;
import junit.framework.Assert;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.lucidworks.spark.util.ConfigurationConstants.*;

public class SolrSqlTest extends RDDProcessorTestBase{


  /**
   * 1. Create a collection
   * 2. Modify the schema to enable docValues for some fields
   * 3. Index sample dataset
   * 4. Do a series of SQL queries and make sure they return valid results
   * @throws Exception
   */
  //@Ignore
  @Test
  public void testSQLQueries() throws Exception {
    String testCollectionName = "testSQLQueries";
    try {

      String zkHost = cluster.getZkServer().getZkAddress();

      SQLContext sqlContext = new SQLContext(jsc.sc());
      HashMap<String, String> options = new HashMap<>();

      deleteCollection(testCollectionName);
      buildCollection(zkHost, testCollectionName, null, 2);
      EventsimUtil.defineSchemaForEventSim(zkHost, testCollectionName);
      EventsimUtil.loadEventSimDataSet(zkHost, testCollectionName, sqlContext);

      options.put(SOLR_ZK_HOST_PARAM(), zkHost);
      options.put(SOLR_COLLECTION_PARAM(), testCollectionName);
      options.put(SOLR_QUERY_PARAM(), "*:*");

      {
        DataFrame eventsim = sqlContext.read().format("solr").options(options).option(SOLR_DOC_VALUES(), "true").load();
        eventsim.registerTempTable("eventsim");

        DataFrame records = sqlContext.sql("SELECT * FROM eventsim");
        StructType schema = records.schema();
        List<Row> rows = records.collectAsList();
        assert records.count() == 1000;

        String[] fieldNames = schema.fieldNames();
        // list of fields that are present in src/test/resources/eventsim/fields_schema.json
        assert fieldNames.length == 19 + 1 + 1; // extra fields are id and _version_

        Assert.assertEquals(schema.apply("ts").dataType().typeName(), DataTypes.TimestampType.typeName());
        Assert.assertEquals(schema.apply("sessionId").dataType().typeName(), DataTypes.LongType.typeName());
        Assert.assertEquals(schema.apply("length").dataType().typeName(), DataTypes.DoubleType.typeName());
        Assert.assertEquals(schema.apply("song").dataType().typeName(), DataTypes.StringType.typeName());

        assert rows.get(0).length() == 21;
      }

      // Filter using SQL syntax and escape field names
      {
        DataFrame eventsim = sqlContext.read().format("solr").options(options).load();
        eventsim.registerTempTable("eventsim");

        DataFrame records = sqlContext.sql("SELECT `userId`, `ts` from eventsim WHERE `gender` = 'M'");
        assert records.count() == 567;
      }

      // Configure the sql query to do splits using an int type field. TODO: Assert the number of partitions based on the field values
      {
        options.put(SOLR_SPLIT_FIELD_PARAM(), "sessionId");
        options.put(SOLR_SPLITS_PER_SHARD_PARAM(), "10");
        options.put(SOLR_DOC_VALUES(), "false");
        DataFrame eventsim = sqlContext.read().format("solr").options(options).load();

        List<Row> rows = eventsim.collectAsList();
        assert rows.size() == 1000;
      }
    } finally {
      deleteCollection(testCollectionName);
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
