package com.lucidworks.spark.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.status.api.v1.NotFoundException;
import scala.collection.JavaConversions;
import scala.collection.immutable.Set$;

import java.io.File;
import java.sql.Timestamp;
import java.util.*;

import static com.lucidworks.spark.util.SolrQuerySupport.getFieldTypes;

public class EventsimUtil {
  static final Logger log = Logger.getLogger(EventsimUtil.class);
  private static ObjectMapper objectMapper = new ObjectMapper();

  /**
   * Load the eventsim json dataset and post it through HttpClient
   * @throws Exception
   */
  public static void loadEventSimDataSet(String zkHost, String collectionName, SQLContext sqlContext) throws Exception {
    String datasetPath = "src/test/resources/eventsim/sample_eventsim_1000.json";
    Dataset df = sqlContext.read().json(datasetPath);
    // Modify the unix timestamp to ISO format for Solr
    log.info("Indexing eventsim documents from file " + datasetPath);

    df.registerTempTable("jdbcDF");
    sqlContext.udf().register("ts2iso", new UDF1<Long, Timestamp>() {
      public Timestamp call(Long ts) {
        return asDate(ts);
      }
    }, DataTypes.TimestampType);

    // Registering an UDF and re-using it via DataFrames is not available through Java right now.
    Dataset newDF = sqlContext.sql("SELECT userAgent, userId, artist, auth, firstName, gender, itemInSession, lastName, " +
      "length, level, location, method, page, sessionId, song,  " +
      "ts2iso(registration) AS registration, ts2iso(ts) AS ts, status from jdbcDF");

    HashMap<String, String> options = new HashMap<String, String>();
    options.put("zkhost", zkHost);
    options.put("collection", collectionName);
    options.put(ConfigurationConstants.GENERATE_UNIQUE_KEY(), "true");

    newDF.write().format("solr").options(options).mode(org.apache.spark.sql.SaveMode.Overwrite).save();

    CloudSolrClient cloudSolrClient = SolrSupport.getCachedCloudClient(zkHost);
    cloudSolrClient.commit(collectionName, true, true);

    long docsInSolr = SolrQuerySupport.getNumDocsFromSolr(collectionName, zkHost, scala.Option.apply((SolrQuery) null));
    if (!(docsInSolr == 1000)) {
      throw new Exception("All eventsim documents did not get indexed. Expected '1000'. Actual docs in Solr '" + docsInSolr + "'");
    }
  }

  private static Timestamp asDate(Object tsObj) {
    if (tsObj != null) {
      long tsLong = (tsObj instanceof Number) ? ((Number)tsObj).longValue() : Long.parseLong(tsObj.toString());
      return new Timestamp(tsLong);
    }
    return null;
  }
}
