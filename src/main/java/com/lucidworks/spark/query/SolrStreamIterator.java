package com.lucidworks.spark.query;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
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

  protected SolrClient solrServer;
  protected SolrQuery solrQuery;
  protected String shardUrl;
  protected int numWorkers;
  protected int workerId;
  protected SolrClientCache solrClientCache;

  // Remove the whole code around StreamContext, numWorkers, workerId once SOLR-10490 is fixed.
  // It should just work if an 'fq' passed in the params with HashQ filter
  public SolrStreamIterator(String shardUrl, SolrClient solrServer, SolrQuery solrQuery, int numWorkers, int workerId) {
    super(solrQuery);

    this.shardUrl = shardUrl;
    this.solrServer = solrServer;
    this.solrQuery = mergeFq(solrQuery);
    this.numWorkers = numWorkers;
    this.workerId = workerId;

    if (solrQuery.getRequestHandler() == null) {
      solrQuery = solrQuery.setRequestHandler("/export");
    }
    solrQuery.setRows(null);
    solrQuery.set(CommonParams.WT, CommonParams.JAVABIN);
    //SolrQuerySupport.validateExportHandlerQuery(solrServer, solrQuery);
  }

  public SolrStreamIterator(String shardUrl, SolrClient solrServer, SolrQuery solrQuery) {
    this(shardUrl, solrServer, solrQuery, 0, 0);
  }

  protected TupleStream openStream() {
    SolrStream stream;
    try {
      stream = new SolrStream(shardUrl, solrQuery);
      if (numWorkers > 0)
        stream.setStreamContext(getStreamContext());
      stream.open();
    } catch (IOException e1) {
      throw new RuntimeException(e1);
    }
    return stream;
  }

  // TODO: Remove once SOLR-10490 is fixed
  protected StreamContext getStreamContext() {
    StreamContext context = new StreamContext();
    solrClientCache = new SolrClientCache();
    context.setSolrClientCache(solrClientCache);
    context.numWorkers = numWorkers;
    context.workerID = workerId;
    return context;
  }

  protected void afterStreamClosed() throws Exception {
    if (!(solrServer instanceof CloudSolrClient)) {
      IOUtils.closeQuietly(solrServer);
    }
    if (solrClientCache != null) {
      solrClientCache.close();
    }
  }

  protected SolrQuery mergeFq(SolrQuery solrQuery) {
    String[] values = solrQuery.getFilterQueries();
    if (values != null && values.length > 1) {
      String fqResult = "";
      for (int i = 0; i < values.length; i++) {
        if (i != values.length - 1) {
          fqResult += "(" + values[i] + ")" + " AND ";
        } else {
          fqResult += "(" + values[i] + ")";
        }
      }
      log.info("Merged multiple FQ params in to a single param. Result: '" + fqResult + "'");
      solrQuery.set(CommonParams.FQ, fqResult);
    }
    return solrQuery;
  }
}
