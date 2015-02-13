package com.lucidworks.spark.example.streaming.oneusagov;

import com.lucidworks.spark.SolrSupport;
import com.lucidworks.spark.SparkApp;
import com.lucidworks.spark.streaming.MessageStreamReceiver;
import com.lucidworks.spark.streaming.netty.HttpClientMessageStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.net.URI;
import java.net.URL;
import java.util.UUID;

/**
 * Read events from the 1usa.gov url shortening service, enrich, and index in Solr.
 */
public class OneUsaGovStreamProcessor extends SparkApp.StreamProcessor {

  public static Logger log = Logger.getLogger(OneUsaGovStreamProcessor.class);

  public static String DEFAULT_ONE_USA_GOV_ENDPOINT = "http://developer.usa.gov/1usagov";

  public String getName() { return "oneusagov"; }

  /**
   * Read a stream of JSON docs from the 1.usa.gov HTTP feed.
   */
  protected JavaDStream<String> openStream(JavaStreamingContext jssc, CommandLine cli) {
    URI uri = null;
    try {
      uri = new URL(cli.getOptionValue("url", DEFAULT_ONE_USA_GOV_ENDPOINT)).toURI();
    } catch (Exception exc) {
      throw new RuntimeException(exc);
    }
    return jssc.receiverStream(new MessageStreamReceiver(new HttpClientMessageStream(uri)));
  }

  /**
   * Reads data from the OneUsaGov feed and indexes in Solr.
   */
  public void plan(JavaStreamingContext jssc, CommandLine cli) throws Exception {

    // Read JSON objects from the message stream (HTTP URL) and parse them into OneUsaGovRequest objects
    JavaDStream<OneUsaGovRequest> requests = openStream(jssc, cli).map(
      new Function<String, OneUsaGovRequest>() {
        public OneUsaGovRequest call(String json) {
          OneUsaGovRequest req = null;
          try {
            req = OneUsaGovRequest.parse(json);
          } catch (Exception exc) {
            log.error("Failed to parse [" + json + "] due to: " + exc);
          }
          return req;
        }
      }
    );

    // Map OneUsaGovRequest objects into SolrInputDocument objects using basic Java reflection and dynamic fields
    JavaDStream<SolrInputDocument> docs = requests.map(
      new Function<OneUsaGovRequest,SolrInputDocument>() {
        public SolrInputDocument call(OneUsaGovRequest req) {
          // SolrSupport provides a very basic way to map properties from an object to a SolrInputDocument
          // more complex mappings should be done manually
          return SolrSupport.autoMapToSolrInputDoc(UUID.randomUUID().toString(), req, null);
        }
      }
    );

    // when ready, send the docs into a SolrCloud cluster
    SolrSupport.indexDStreamOfDocs(zkHost, collection, batchSize, docs);
  }

  public Option[] getOptions() {
    return new Option[]{
      OptionBuilder
              .withArgName("URL")
              .hasArg()
              .isRequired(false)
              .withDescription("One USA Gov endpoint; default is: "+DEFAULT_ONE_USA_GOV_ENDPOINT)
              .create("url")
    };
  }
}
