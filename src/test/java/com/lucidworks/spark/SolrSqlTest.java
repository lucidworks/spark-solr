package com.lucidworks.spark;

import com.lucidworks.spark.util.EventsimUtil;
import junit.framework.Assert;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.*;

import static com.lucidworks.spark.util.ConfigurationConstants.*;

public class SolrSqlTest extends RDDProcessorTestBase {


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

      HashMap<String, String> options = new HashMap<>();

      deleteCollection(testCollectionName);
      buildCollection(zkHost, testCollectionName, null, 2);
      EventsimUtil.loadEventSimDataSet(zkHost, testCollectionName, sparkSession);

      options.put(SOLR_ZK_HOST_PARAM(), zkHost);
      options.put(SOLR_COLLECTION_PARAM(), testCollectionName);
      options.put(SOLR_QUERY_PARAM(), "*:*");

      {
        Dataset eventsim = sparkSession.read().format("solr").options(options).option(SOLR_DOC_VALUES(), "true").load();
        eventsim.registerTempTable("eventsim");

        Dataset records = sparkSession.sql("SELECT * FROM eventsim");
        StructType schema = records.schema();
        List<Object> rows = records.collectAsList();
        assert records.count() == 1000;

        String[] fieldNames = schema.fieldNames();
        // list of fields that are indexed from {@code EventsimUtil#loadEventSimDataSet}
        Assert.assertEquals(21, fieldNames.length);  // 18 fields from the file + id + _root_ + artist_txt
        //assert fieldNames.length == 20;

        Assert.assertEquals(schema.apply("ts").dataType().typeName(), DataTypes.TimestampType.typeName());
        Assert.assertEquals(schema.apply("sessionId").dataType().typeName(), DataTypes.LongType.typeName());
        Assert.assertEquals(schema.apply("length").dataType().typeName(), DataTypes.DoubleType.typeName());
        Assert.assertEquals(schema.apply("song").dataType().typeName(), DataTypes.StringType.typeName());

        Assert.assertEquals(21, ((Row)rows.get(0)).length());
      }

      // Filter using SQL syntax and escape field names
      {
        Dataset eventsim = sparkSession.read().format("solr").options(options).load();
        eventsim.registerTempTable("eventsim");

        Dataset records = sparkSession.sql("SELECT `userId`, `ts` from eventsim WHERE `gender` = 'M'");
        assert records.count() == 567;
      }

      // Configure the sql query to do splits using an int type field. TODO: Assert the number of partitions based on the field values
      {
        options.put(SOLR_SPLIT_FIELD_PARAM(), "sessionId");
        options.put(SOLR_SPLITS_PER_SHARD_PARAM(), "10");
        options.put(SOLR_DOC_VALUES(), "false");
        Dataset eventsim = sparkSession.read().format("solr").options(options).load();

        List<Object> rows = eventsim.collectAsList();
        assert rows.size() == 1000;
      }
    } finally {
      deleteCollection(testCollectionName);
    }
  }

  @Test(expected=IllegalArgumentException.class)
  public void testInvalidOptions() {
    sparkSession.read().format("solr").load();
  }

  @Test(expected=IllegalArgumentException.class)
  public void testInvalidCollectionOption() {

    Map<String, String> options = Collections.singletonMap("zkHost", cluster.getZkServer().getZkAddress());
    sparkSession.read().format("solr").options(options).load();
  }

}
