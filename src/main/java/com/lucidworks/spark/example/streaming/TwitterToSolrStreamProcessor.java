package com.lucidworks.spark.example.streaming;

import com.lucidworks.spark.SparkApp;
import com.lucidworks.spark.util.SolrSupport;
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
  @Override
  public void setup(JavaStreamingContext jssc, CommandLine cli) throws Exception {
    String filtersArg = cli.getOptionValue("tweetFilters");
    String[] filters = (filtersArg != null) ? filtersArg.split(",") : new String[0];

    // start receiving a stream of tweets ...
    JavaReceiverInputDStream<Status> tweets =
      TwitterUtils.createStream(jssc, null, filters);

    String fusionUrl = cli.getOptionValue("fusion");
    if (fusionUrl != null) {
      // just send JSON directly to Fusion
      SolrSupport.sendDStreamOfDocsToFusion(fusionUrl, cli.getOptionValue("fusionCredentials"), tweets.dstream(), batchSize);
    } else {
      // map incoming tweets into PipelineDocument objects for indexing in Solr
      JavaDStream<SolrInputDocument> docs = tweets.map(
          new Function<Status,SolrInputDocument>() {

            /**
             * Convert a twitter4j Status object into a SolrJ SolrInputDocument
             */
            public SolrInputDocument call(Status status) {

              if (log.isDebugEnabled()) {
                log.debug("Received tweet: " + status.getId() + ": " + status.getText().replaceAll("\\s+", " "));
              }

              // simple mapping from primitives to dynamic Solr fields using reflection
              SolrInputDocument doc =
                  SolrSupport.autoMapToSolrInputDoc("tweet-"+status.getId(), status, null);
              doc.setField("provider_s", "twitter");
              doc.setField("author_s", status.getUser().getScreenName());
              doc.setField("type_s", status.isRetweet() ? "echo" : "post");

              if (log.isDebugEnabled())
                log.debug("Transformed document: " + doc.toString());
              return doc;
            }
          }
      );

      // when ready, send the docs into a SolrCloud cluster
      SolrSupport.indexDStreamOfDocs(zkHost, collection, batchSize, docs.dstream());
    }
  }

  public Option[] getOptions() {
    return new Option[]{
      OptionBuilder
              .withArgName("LIST")
              .hasArg()
              .isRequired(false)
              .withDescription("List of Twitter keywords to filter on, separated by commas")
              .create("tweetFilters"),
        OptionBuilder
            .withArgName("URL(s)")
            .hasArg()
            .isRequired(false)
            .withDescription("Fusion endpoint")
            .create("fusion"),
        OptionBuilder
            .withArgName("user:password:realm")
            .hasArg()
            .isRequired(false)
            .withDescription("Fusion credentials user:password:realm")
            .create("fusionCredentials")
    };
  }
}
