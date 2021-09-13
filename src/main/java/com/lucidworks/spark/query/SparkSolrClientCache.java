package com.lucidworks.spark.query;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;

/**
 * Overriding so that we can pass our own cloud client for StreamingContext
 * zkhost param is not used since we have an existing cloud instance for the ZK
 */
public class SparkSolrClientCache extends SolrClientCache {

  private final CloudSolrClient solrClient;
  private final HttpSolrClient httpSolrClient;

  public SparkSolrClientCache(CloudSolrClient solrClient, HttpSolrClient httpSolrClient) {
    this.solrClient = solrClient;
    this.httpSolrClient = httpSolrClient;
  }

  public synchronized CloudSolrClient getCloudSolrClient(String zkHost) {
    return solrClient;
  }

  public synchronized HttpSolrClient getHttpSolrClient(String host) {
    if (host != null && host.endsWith("/")) {
      host = host.substring(0, host.length() - 1);
    }
    httpSolrClient.setBaseURL(host);
    return httpSolrClient;
  }

}
