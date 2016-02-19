package com.lucidworks.spark.example.streaming;

import com.lucidworks.spark.util.SolrSupport;
import com.lucidworks.spark.SparkApp;
import com.lucidworks.spark.filter.DocFilterContext;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;

import java.util.ArrayList;
import java.util.List;

/**
 * Example showing how to match documents against a set of known queries; useful
 * for doing things like alerts, etc.
 */
public class DocumentFilteringStreamProcessor extends SparkApp.StreamProcessor {

  public static Logger log = Logger.getLogger(DocumentFilteringStreamProcessor.class);

  /**
   * A DocFilterContext is responsible for loading queries from some external system and
   * then doing something with each doc that is matched to a query.
   */
  class ExampleDocFilterContextImpl implements DocFilterContext {

    public void init(JavaStreamingContext jssc, CommandLine cli) {
      // nothing to init for this basic impl
    }

    public String getDocIdFieldName() { return "id"; }

    public List<SolrQuery> getQueries() {
      List<SolrQuery> queryList = new ArrayList<SolrQuery>();

      // a real impl would pull queries from an external system, such as Solr or a DB or a file
      SolrQuery q1 = new SolrQuery("type_s:post");
      q1.setParam("_qid_", "POSTS"); // identify the query when tagging matching docs
      queryList.add(q1);

      SolrQuery q2 = new SolrQuery("type_s:echo");
      q2.setParam("_qid_", "ECHOS");
      queryList.add(q2);

      return queryList;
    }

    public void onMatch(SolrQuery query, SolrInputDocument inputDoc) {
      String[] qids = query.getParams("_qid_");
      if (qids == null || qids.length < 1) return; // not one of ours

      if (log.isDebugEnabled())
        log.debug("document [" + inputDoc.getFieldValue("id") + "] matches query: " + qids[0]);

      // just index the matching query for later analysis
      inputDoc.addField("_qid_ss", qids[0]);
    }
  }

  public String getName() { return "docfilter"; }

  @Override
  public void setup(JavaStreamingContext jssc, CommandLine cli) throws Exception {

    // load the DocFilterContext implementation, which knows how to load queries
    DocFilterContext docFilterContext = loadDocFilterContext(jssc, cli);
    final String idFieldName = docFilterContext.getDocIdFieldName();

    // start receiving a stream of tweets ...
    String filtersArg = cli.getOptionValue("tweetFilters");
    String[] filters = (filtersArg != null) ? filtersArg.split(",") : new String[0];
    JavaReceiverInputDStream<Status> tweets = TwitterUtils.createStream(jssc, null, filters);

    // map incoming tweets into SolrInputDocument objects for indexing in Solr
    JavaDStream<SolrInputDocument> docs = tweets.map(
      new Function<Status,SolrInputDocument>() {
        public SolrInputDocument call(Status status) {
          SolrInputDocument doc =
            SolrSupport.autoMapToSolrInputDoc(idFieldName, "tweet-"+status.getId(), status, null);
          doc.setField("provider_s", "twitter");
          doc.setField("author_s", status.getUser().getScreenName());
          doc.setField("type_s", status.isRetweet() ? "echo" : "post");
          return doc;
        }
      }
    );

    // run each doc through a list of filters pulled from our DocFilterContext
    String filterCollection = cli.getOptionValue("filterCollection", collection);
    DStream<SolrInputDocument> enriched =
      SolrSupport.filterDocuments(docFilterContext, zkHost, filterCollection, docs.dstream());

    // now index the enriched docs into Solr (or do whatever after the matching process runs)
    SolrSupport.indexDStreamOfDocs(zkHost, collection, batchSize, enriched);
  }

  protected DocFilterContext loadDocFilterContext(JavaStreamingContext jssc, CommandLine cli)
    throws Exception
  {
    DocFilterContext ctxt = null;
    String docFilterContextImplClass = cli.getOptionValue("docFilterContextImplClass");
    if (docFilterContextImplClass != null) {
      Class<DocFilterContext> implClass =
        (Class<DocFilterContext>)getClass().getClassLoader().loadClass(docFilterContextImplClass);
      ctxt = implClass.newInstance();
    } else {
      ctxt = new ExampleDocFilterContextImpl();
    }
    ctxt.init(jssc, cli);
    return ctxt;
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
        .withArgName("NAME")
        .hasArg()
        .isRequired(false)
        .withDescription("Collection to pull configuration files to create an " +
          "EmbeddedSolrServer for document matching; defaults to the value of the collection option.")
        .create("filterCollection"),
      OptionBuilder
        .withArgName("CLASS")
        .hasArg()
        .isRequired(false)
        .withDescription("Name of the DocFilterContext implementation class; defaults to an internal example impl: "+
          ExampleDocFilterContextImpl.class.getName())
        .create("docFilterContextImplClass")
    };
  }
}
