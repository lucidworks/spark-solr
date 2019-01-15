package com.lucidworks.spark.query;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

public class StreamingExpressionResultIterator extends TupleStreamIterator {

  private static final Logger log = Logger.getLogger(StreamingExpressionResultIterator.class);

  protected String zkHost;
  protected String collection;
  protected String qt;
  protected CloudSolrClient cloudSolrClient;
  protected HttpSolrClient httpSolrClient;
  protected SolrClientCache solrClientCache;

  private final Random random = new Random(5150L);

  public StreamingExpressionResultIterator(CloudSolrClient cloudSolrClient, HttpSolrClient httpSolrClient, String collection, SolrParams solrParams) {
    super(solrParams);
    this.cloudSolrClient = cloudSolrClient;
    this.httpSolrClient = httpSolrClient;
    this.collection = collection;

    qt = solrParams.get(CommonParams.QT);
    if (qt == null) qt = "/stream";
  }

  protected TupleStream openStream() {
    TupleStream stream;

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.QT, qt);

    String aggregationMode = solrParams.get("aggregationMode");

    log.info("aggregationMode=" + aggregationMode + ", solrParams: " + solrParams);
    if (aggregationMode != null) {
      params.set("aggregationMode", aggregationMode);
    } else {
      params.set("aggregationMode", "facet"); // use facet by default as it is faster
    }
    
    if ("/sql".equals(qt)) {
      String sql = solrParams.get("sql").replaceAll("\\s+", " ");
      log.info("Executing SQL statement " + sql + " against collection " + collection);
      params.set("stmt", sql);
    } else {
      String expr = solrParams.get("expr").replaceAll("\\s+", " ");
      log.info("Executing streaming expression " + expr + " against collection " + collection);
      params.set("expr", expr);
    }

    try {
      String url = (new ZkCoreNodeProps(getRandomReplica())).getCoreUrl();
      log.info("Sending "+qt+" request to replica "+url+" of "+collection+" with params: "+params);
      long startMs = System.currentTimeMillis();
      stream = new SolrStream(url, params);
      stream.setStreamContext(getStreamContext());
      stream.open();
      long diffMs = (System.currentTimeMillis() - startMs);
      log.debug("Open stream to "+url+" took "+diffMs+" (ms)");
    } catch (Exception e) {
      log.error("Failed to execute request ["+solrParams+"] due to: "+e, e);
      if (e instanceof RuntimeException) {
        throw (RuntimeException)e;
      } else {
        throw new RuntimeException(e);
      }
    }
    return stream;
  }

  // We have to set the streaming context so that we can pass our own cloud client with authentication
  protected StreamContext getStreamContext() {
    StreamContext context = new StreamContext();
    solrClientCache = new SparkSolrClientCache(cloudSolrClient, httpSolrClient);
    context.setSolrClientCache(solrClientCache);
    return context;
  }

  protected Replica getRandomReplica() {
    ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();
    Collection<Slice> slices = zkStateReader.getClusterState().getCollection(collection.split(",")[0]).getActiveSlices();
    if (slices == null || slices.size() == 0)
      throw new IllegalStateException("No active shards found "+collection);

    List<Replica> shuffler = new ArrayList<>();
    for (Slice slice : slices) {
      shuffler.addAll(slice.getReplicas());
    }
    return shuffler.get(random.nextInt(shuffler.size()));
  }
}
