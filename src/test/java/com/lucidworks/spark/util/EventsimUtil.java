package com.lucidworks.spark.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Collections;
import java.util.Map;

public class EventsimUtil {
  static final Logger log = Logger.getLogger(EventsimUtil.class);
  private static ObjectMapper objectMapper = new ObjectMapper();

  /**
   * Load the eventsim json dataset and write it using Solr writer
   * @throws Exception
   */
  public static void loadEventSimDataSet(String zkHost, String collectionName, SparkSession sparkSession) throws Exception {
    String datasetPath = "src/test/resources/eventsim/sample_eventsim_1000.json";
    Dataset df = sparkSession.read().json(datasetPath);
    // Modify the unix timestamp to ISO format for Solr
    log.info("Indexing eventsim documents from file " + datasetPath);

    df.registerTempTable("jdbcDF");
    sparkSession.udf().register("ts2iso", new UDF1<Long, Timestamp>() {
      public Timestamp call(Long ts) {
        return asDate(ts);
      }
    }, DataTypes.TimestampType);

    // Registering an UDF and re-using it via DataFrames is not available through Java right now.
    Dataset newDF = sparkSession.sql("SELECT userAgent, userId, artist, auth, firstName, gender, itemInSession, lastName, " +
      "length, level, location, method, page, sessionId, song,  " +
      "ts2iso(registration) AS registration, ts2iso(ts) AS ts, status from jdbcDF");

    HashMap<String, String> options = new HashMap<String, String>();
    options.put("zkhost", zkHost);
    options.put("collection", collectionName);
    options.put(ConfigurationConstants.GENERATE_UNIQUE_KEY(), "true");

    newDF = newDF.withColumn("artist_txt", df.col("artist"));
    newDF.write().format("solr").options(options).mode(org.apache.spark.sql.SaveMode.Overwrite).save();

    CloudSolrClient cloudSolrClient = SolrSupport.getCachedCloudClient(zkHost);
    cloudSolrClient.commit(collectionName, true, true);

    long docsInSolr = SolrQuerySupport.getNumDocsFromSolr(collectionName, zkHost, scala.Option.apply((SolrQuery) null));
    if (!(docsInSolr == 1000)) {
      throw new Exception("All eventsim documents did not get indexed. Expected '1000'. Actual docs in Solr '" + docsInSolr + "'");
    }
  }

  public static void defineTextFields(CloudSolrClient solrCloud, String collection) throws Exception {
    Map<String, Object> fieldParams = new HashMap<>();
    fieldParams.put("name", "artist_txt");
    fieldParams.put("indexed", "true");
    fieldParams.put("stored", "true");
    fieldParams.put("multiValued", "false");
    fieldParams.put("type", "text_en");
    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    solrParams.add("updateTimeoutSecs", "30");
    SchemaRequest.AddField addField = new SchemaRequest.AddField(fieldParams);
    SchemaRequest.MultiUpdate addFieldsMultiUpdate = new SchemaRequest.MultiUpdate(Collections.singletonList(addField), solrParams);

    // Add the fields using SolrClient
    SchemaResponse.UpdateResponse response = addFieldsMultiUpdate.process(solrCloud, collection);
    if (response.getStatus() > 400) {
      throw new SolrException(SolrException.ErrorCode.getErrorCode(response.getStatus()), "Error indexing fields to the Schema");
    }
    log.info("Added new field 'artist_txt' to Solr schema for collection" + collection);
  }

  private static Timestamp asDate(Object tsObj) {
    if (tsObj != null) {
      long tsLong = (tsObj instanceof Number) ? ((Number)tsObj).longValue() : Long.parseLong(tsObj.toString());
      return new Timestamp(tsLong);
    }
    return null;
  }
}
