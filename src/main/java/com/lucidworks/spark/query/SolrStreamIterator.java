package com.lucidworks.spark.query;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.common.params.CommonParams;

import java.io.IOException;

/**
 * An iterator over a stream of query results from one Solr Core. It is a
 * wrapper over the SolrStream to adapt it to an iterator interface.
 * <p>
 * This iterator is not thread safe. It is intended to be used within the
 * context of a single thread.
 */
public class SolrStreamIterator extends TupleStreamIterator {

  private static final Logger log = Logger.getLogger(SolrStreamIterator.class);

  protected SolrQuery solrQuery;
  protected String shardUrl;
  protected int numWorkers;
  protected int workerId;
  protected SolrClientCache solrClientCache;
  protected HttpSolrClient httpSolrClient;
  protected CloudSolrClient cloudSolrClient;

  // Remove the whole code around StreamContext, numWorkers, workerId once SOLR-10490 is fixed.
  // It should just work if an 'fq' passed in the params with HashQ filter
  public SolrStreamIterator(String shardUrl, CloudSolrClient cloudSolrClient, HttpSolrClient httpSolrClient, SolrQuery solrQuery, int numWorkers, int workerId) {
    super(solrQuery);

    this.shardUrl = shardUrl;
    this.cloudSolrClient = cloudSolrClient;
    this.httpSolrClient = httpSolrClient;
    this.solrQuery = solrQuery;
    this.numWorkers = numWorkers;
    this.workerId = workerId;

    if (solrQuery.getRequestHandler() == null) {
      solrQuery = solrQuery.setRequestHandler("/export");
    }
    solrQuery.setRows(null);
    solrQuery.set(CommonParams.WT, CommonParams.JAVABIN);
    //SolrQuerySupport.validateExportHandlerQuery(solrServer, solrQuery);
  }

  protected TupleStream openStream() {
    SolrStream stream;
    try {
      stream = new SolrStream(shardUrl, solrQuery);
      stream.setStreamContext(getStreamContext());
      stream.open();
    } catch (IOException e1) {
      throw new RuntimeException(e1);
    }
    return stream;
  }

  // We have to set the streaming context so that we can pass our own cloud client with authentication
  protected StreamContext getStreamContext() {
    StreamContext context = new StreamContext();
    solrClientCache = new SparkSolrClientCache(cloudSolrClient, httpSolrClient);
    context.setSolrClientCache(solrClientCache);
    context.numWorkers = numWorkers;
    context.workerID = workerId;
    return context;
  }

  protected void afterStreamClosed() throws Exception {
    // No need to close http or cloudClient because they are re-used from cache
  }
}
