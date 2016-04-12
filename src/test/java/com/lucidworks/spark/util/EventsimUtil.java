package com.lucidworks.spark.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.status.api.v1.NotFoundException;
import scala.collection.JavaConversions;
import scala.collection.immutable.Set$;

import java.io.File;
import java.sql.Timestamp;
import java.util.*;
import java.util.Date;

import static com.lucidworks.spark.util.SolrQuerySupport.getFieldTypes;

public class EventsimUtil {
  static final Logger log = Logger.getLogger(EventsimUtil.class);
  private static ObjectMapper objectMapper = new ObjectMapper();

  /**
   * Define the schema for Eventsim dataset
   * @param zkHost
   * @param collectionName
   * @throws Exception
   */
  public static void defineSchemaForEventSim(String zkHost, String collectionName) throws Exception {
    String schemaPath = "src/test/resources/eventsim/fields_schema.json";
    log.info("Reading schema file: " + schemaPath);
    File schemaFile = new File(schemaPath);
    if (!schemaFile.exists())
      throw new NotFoundException("Could not find the schema file at path " + schemaPath);

    CloudSolrClient solrClient = SolrSupport.getCachedCloudClient(zkHost);
    solrClient.setDefaultCollection(collectionName);
    List<Map<String, Object>> fieldDefinitions = new ObjectMapper().readValue(schemaFile, new TypeReference<List<Map<String, Object>>>() {
    });
    JavaConversions.asScalaSet(new HashSet<>());
    Map<String, SolrFieldMeta> fields = JavaConversions.asJavaMap(getFieldTypes(
      Set$.MODULE$.<String>empty(),
      SolrSupport.getSolrBaseUrl(zkHost),
      collectionName));
    Set<String> existingFields = fields.keySet();

    // Add the fields to Solr schema
    for (Map<String, Object> fd: fieldDefinitions) {
      String name = (String)fd.get("name");
      if (!existingFields.contains(name)) {
        // Add the field to Solr
        SchemaRequest.AddField addFieldRequest = new SchemaRequest.AddField(fd);
        SchemaResponse.UpdateResponse updateResponse = addFieldRequest.process(solrClient);

        if (updateResponse.getStatus() != 0)
          throw new Exception("Incorrect status response from Solr. Errors are: " + updateResponse.getResponse().get("errors"));
        if (updateResponse.getResponse().asMap(5).containsKey("errors"))
          throw new Exception("Errors from schema request: " + updateResponse.getResponse().get("errors").toString());
        log.info("Added field definition: " + fd.toString());
      } else {
        log.info("Field '" + name + "' already exists");
      }
    }
  }

  /**
   * Load the eventsim json dataset and post it through HttpClient
   * @throws Exception
   */
  public static void loadEventSimDataSet(String zkHost, String collectionName, SQLContext sqlContext) throws Exception {
    String datasetPath = "src/test/resources/eventsim/sample_eventsim_1000.json";
    DataFrame df = sqlContext.read().json(datasetPath);
    // Modify the unix timestamp to ISO format for Solr
    log.info("Indexing eventsim documents from file " + datasetPath);

    df.registerTempTable("jdbcDF");
    sqlContext.udf().register("ts2ISO", new UDF1<Long, Timestamp>() {
      public Timestamp call(Long ts) {
        return asDate(ts);
      }
    }, DataTypes.TimestampType);

    // Registering an UDF and re-using it via DataFrames is not available through Java right now.
    DataFrame newDF = sqlContext.sql("SELECT userAgent, userId, artist, auth, firstName, gender, itemInSession, lastName, " +
      "length, level, location, method, page, sessionId, song,  " +
      "ts2ISO(registration) AS registration, ts2ISO(ts) AS ts, status from jdbcDF");

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
