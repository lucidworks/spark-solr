package com.lucidworks.spark.query;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Overriding so that we can pass our own cloud client for StreamingContext.
 * zkhost param is not used since we have an existing cloud instance for the ZK.
 *
 * Http2SolrClient is immutable (no setBaseURL), so we cache one per base URL.
 */
public class SparkSolrClientCache extends SolrClientCache {

  private final CloudSolrClient solrClient;
  private final Map<String, Http2SolrClient> httpClients = new ConcurrentHashMap<>();

  public SparkSolrClientCache(CloudSolrClient solrClient) {
    this.solrClient = solrClient;
  }

  public synchronized CloudSolrClient getCloudSolrClient(String zkHost) {
    return solrClient;
  }

  public synchronized SolrClient getHttpSolrClient(String baseUrl) {
    if (baseUrl != null && baseUrl.endsWith("/")) {
      baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
    }
    final String url = baseUrl;
    return httpClients.computeIfAbsent(url, u -> new Http2SolrClient.Builder(u).build());
  }

  @Override
  public void close() {
    for (Http2SolrClient client : httpClients.values()) {
      try {
        client.close();
      } catch (Exception ignore) {
      }
    }
    httpClients.clear();
    // Don't close the shared CloudSolrClient — it's managed by the cache
  }
}
