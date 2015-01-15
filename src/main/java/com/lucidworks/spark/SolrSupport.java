package com.lucidworks.spark;

import java.net.ConnectException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.ConnectTimeoutException;
import org.apache.commons.httpclient.NoHttpResponseException;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaDStream;

import org.apache.spark.api.java.function.*;

public class SolrSupport {

  public static Logger log = Logger.getLogger(SolrSupport.class);

  private static Map<String,SolrServer> solrServers = new HashMap<String, SolrServer>();

  public static SolrServer getSolrServer(String key) {
    SolrServer solr = null;
    synchronized (solrServers) {
      solr = solrServers.get(key);
      if (solr == null) {
        solr = new CloudSolrServer(key);
        solrServers.put(key, solr);
      }
    }
    return solr;
  }

  /**
   * Helper function for indexing a DStream of SolrInputDocuments to Solr.
   */
  public static void indexDStreamOfDocs(final String zkHost, final String collection, final int batchSize, JavaDStream<SolrInputDocument> docs) {
    docs.foreachRDD(
      new Function<JavaRDD<SolrInputDocument>, Void>() {
        public Void call(JavaRDD<SolrInputDocument> solrInputDocumentJavaRDD) throws Exception {
          solrInputDocumentJavaRDD.foreachPartition(
            new VoidFunction<Iterator<SolrInputDocument>>() {
              public void call(Iterator<SolrInputDocument> solrInputDocumentIterator) throws Exception {
                final SolrServer solrServer = getSolrServer(zkHost);
                List<SolrInputDocument> batch = new ArrayList<SolrInputDocument>();
                while (solrInputDocumentIterator.hasNext()) {
                  batch.add(solrInputDocumentIterator.next());

                  if (batch.size() >= batchSize)
                    sendBatchToSolr(solrServer, collection, batch);
                }

                if (!batch.isEmpty())
                  sendBatchToSolr(solrServer, collection, batch);
              }
            }
          );
          return null;
        }
      }
    );
  }

  public static void sendBatchToSolr(SolrServer solrServer, String collection, List<SolrInputDocument> batch) {
    UpdateRequest req = new UpdateRequest();
    req.setParam("collection", collection);
    req.add(batch);
    try {
      solrServer.request(req);
    } catch (Exception e) {
      if (shouldRetry(e)) {
        log.error("Send batch to collection "+collection+" failed due to "+e+"; will retry ...");
        try {
          Thread.sleep(2000);
        } catch (InterruptedException ie) {
          Thread.interrupted();
        }

        try {
          solrServer.request(req);
        } catch (Exception e1) {
          log.error("Retry send batch to collection "+collection+" failed due to: "+e1, e1);
          if (e1 instanceof RuntimeException) {
            throw (RuntimeException)e1;
          } else {
            throw new RuntimeException(e1);
          }
        }
      } else {
        log.error("Send batch to collection "+collection+" failed due to: "+e, e);
        if (e instanceof RuntimeException) {
          throw (RuntimeException)e;
        } else {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private static boolean shouldRetry(Exception exc) {
    Throwable rootCause = SolrException.getRootCause(exc);
    return (rootCause instanceof ConnectException ||
            rootCause instanceof ConnectTimeoutException ||
            rootCause instanceof NoHttpResponseException ||
            rootCause instanceof SocketException);

  }
}
