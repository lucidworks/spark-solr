package com.lucidworks.spark;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;

/**
 * Simple example of indexing tweets into Solr using Spark streaming; be sure to update the
 * twitter4j.properties file on the classpath with your Twitter API credentials.
 */
public class TwitterToSolrStreamProcessor extends SparkApp.StreamProcessor {

  public static Logger log = Logger.getLogger(TwitterToSolrStreamProcessor.class);

  public String getName() { return "twitter-to-solr"; }

  /**
   * Sends a stream of tweets to Solr.
   */
  public int process(JavaStreamingContext jssc, CommandLine cli) throws Exception {
    String filtersArg = cli.getOptionValue("filters");
    String[] filters = (filtersArg != null) ? filtersArg.split(",") : new String[0];

    final boolean verbose = cli.hasOption("verbose");

    // start receiving a stream of tweets ...
    JavaReceiverInputDStream<Status> tweets =
      TwitterUtils.createStream(jssc, null, filters);

    // map incoming tweets into PipelineDocument objects for indexing in Solr
    JavaDStream<SolrInputDocument> docs = tweets.map(
      new Function<Status,SolrInputDocument>() {

        /**
         * Convert a twitter4j Status object into a SolrJ SolrInputDocument
         */
        public SolrInputDocument call(Status status) {

          if (verbose)
            log.info("Received tweet: "+status.getId()+": "+status.getText().replaceAll("\\s+", " "));

          SolrInputDocument doc = new SolrInputDocument();
          doc.setField("id", String.format("t-%d", status.getId()));
          doc.setField("provider_s", "twitter");
          doc.setField("tweet_s", status.getText());
          doc.setField("author_s", status.getUser().getScreenName());
          doc.setField("created_at_tdt", status.getCreatedAt());
          doc.setField("type_s", status.isRetweet() ? "retweet" : "post");
          return doc;
        }
      }
    );

    // TODO: You can do other transformations / enrichments on the data before sending to Solr

    // when ready, send the docs into a SolrCloud cluster
    String zkHost = cli.getOptionValue("zkHost", cli.getOptionValue("zkHost", "localhost:9983"));
    String collection = cli.getOptionValue("collection", cli.getOptionValue("collection", "collection1"));
    SolrSupport.indexDStreamOfDocs(zkHost, collection, 100, docs);

    jssc.start();              // Start the computation
    jssc.awaitTermination();   // Wait for the computation to terminate

    return 0;
  }

  public Option[] getOptions() {
    return new Option[]{
      OptionBuilder
              .withArgName("LIST")
              .hasArg()
              .isRequired(false)
              .withDescription("List of Twitter keywords to filter on, separated by commas")
              .create("filters")
    };
  }
}
