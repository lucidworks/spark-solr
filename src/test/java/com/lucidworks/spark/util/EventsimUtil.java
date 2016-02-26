package com.lucidworks.spark.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.status.api.v1.NotFoundException;
import scala.collection.JavaConversions;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

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
    Map<String, SolrFieldMeta> fields = JavaConversions.asJavaMap(getFieldTypes(JavaConversions.asScalaSet(new HashSet<>()).toSet(), SolrSupport.getSolrBaseUrl(zkHost), collectionName));
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
      }
    }
  }

  /**
   * Load the eventsim json dataset and post it through HttpClient
   * @throws Exception
   */
  public static void loadEventSimDataSet(String zkHost, String collectionName) throws Exception {
    String datasetPath = "src/test/resources/eventsim/sample_eventsim_1000.json";
    File eventsimFile = new File(datasetPath);
    if (!eventsimFile.exists())
      throw new FileNotFoundException("File not found at path '" + datasetPath + "'");

    // Convert the eventsim data to valid SolrDocument
    List<SolrInputDocument> docs = Files.lines(eventsimFile.toPath())
                                    .map(EventsimUtil::convertToSolrDocument)
                                    .collect(Collectors.toList());

    CloudSolrClient solrClient = SolrSupport.getCachedCloudClient(zkHost);
    solrClient.setDefaultCollection(collectionName);
    solrClient.add(docs);
    solrClient.commit();

 }

  private static SolrInputDocument convertToSolrDocument(String line) {
    SolrInputDocument doc = new SolrInputDocument();
    try {
      Map<String, Object> event = objectMapper.readValue(line, new TypeReference<Map<String, Object>>() {
      });
      // Parse timestamps to ISO format
      if (event.containsKey("ts")) {
        doc.setField("timestamp", asDate(event.get("ts")));
      }
      if (event.containsKey("registration")) {
        doc.setField("registration", asDate(event.get("registration")));
      }

      // Add all other fields to Solr
      for (String k: event.keySet()) {
        if (!k.equals("ts") && !k.equals("registration")) {
          doc.setField(k, event.get(k));
        }
      }

      doc.setField("id", UUID.randomUUID().toString());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return doc;
  }

  private static Date asDate(Object tsObj) {
    long tsLong = (tsObj instanceof Number) ? ((Number)tsObj).longValue() : Long.parseLong(tsObj.toString());
    return new Date(tsLong);
  }
}
