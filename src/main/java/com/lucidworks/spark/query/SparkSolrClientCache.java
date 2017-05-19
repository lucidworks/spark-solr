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

  public SparkSolrClientCache(CloudSolrClient solrClient) {
    this.solrClient = solrClient;
  }

  public synchronized CloudSolrClient getCloudSolrClient(String zkHost) {
    return solrClient;
  }

  public synchronized HttpSolrClient getHttpSolrClient(String host) {
    return new HttpSolrClient.Builder()
        .withBaseSolrUrl(host)
        .withHttpClient(solrClient.getHttpClient())
        .build();
  }

}
