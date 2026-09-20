package com.lucidworks.spark.query;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Overriding so that we can pass our own cloud client for StreamingContext
 * zkhost param is not used since we have an existing cloud instance for the ZK
 *
 * NOTE: In Solr 8, {@code HttpSolrClient} exposed a mutable {@code setBaseURL(String)}, so a
 * single shared instance could be redirected to whichever shard host a stream needed to talk to.
 * Solr 10's {@code HttpJettySolrClient} makes its base URL immutable (set once at construction),
 * so that trick no longer works. Instead we lazily build (and cache) one client per distinct
 * host that's requested.
 */
public class SparkSolrClientCache extends SolrClientCache {

  private final CloudSolrClient solrClient;
  private final Map<String, HttpJettySolrClient> httpSolrClients = new ConcurrentHashMap<>();

  public SparkSolrClientCache(CloudSolrClient solrClient, HttpJettySolrClient httpSolrClientTemplate) {
    this.solrClient = solrClient;
    // httpSolrClientTemplate previously carried a dummy base URL that would later get mutated
    // to the real per-shard host; that base URL is unused now since we build fresh per-host
    // clients on demand below.
  }

  public synchronized CloudSolrClient getCloudSolrClient(String zkHost) {
    return solrClient;
  }

  public synchronized SolrClient getHttpSolrClient(String host) {
    String baseUrl = host;
    if (baseUrl != null && baseUrl.endsWith("/")) {
      baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
    }
    return httpSolrClients.computeIfAbsent(baseUrl,
        u -> new HttpJettySolrClient.Builder().withBaseSolrUrl(u).build());
  }

  @Override
  public synchronized void close() {
    for (HttpJettySolrClient client : httpSolrClients.values()) {
      client.close();
    }
    httpSolrClients.clear();
    super.close();
  }
}
