package com.lucidworks.spark.util;

import java.io.Serializable;
import org.apache.solr.client.solrj.SolrClient;

/**
 * EmbeddedSolrServer was removed in Solr 9. This factory is retained for compile compatibility
 * but will throw UnsupportedOperationException at runtime.
 */
public class EmbeddedSolrServerFactory implements Serializable {

  public static final EmbeddedSolrServerFactory singleton = new EmbeddedSolrServerFactory();

  public synchronized SolrClient getEmbeddedSolrServer(String zkHost, String collection) {
    throw new UnsupportedOperationException(
      "EmbeddedSolrServer was removed in Solr 9 and is no longer supported.");
  }

  public synchronized SolrClient getEmbeddedSolrServer(String zkHost, String collection, String solrConfigXml, String solrXml) {
    throw new UnsupportedOperationException(
      "EmbeddedSolrServer was removed in Solr 9 and is no longer supported.");
  }
}
