package com.lucidworks.spark.util;

import org.apache.solr.client.solrj.impl.Http2SolrClient;

public abstract class FusionAuthHttpClient {

  private final String zkHost;

  public FusionAuthHttpClient(String zkHost) {
    this.zkHost = zkHost;
  }

  public String getZkHost() {
    return zkHost;
  }

  public abstract Http2SolrClient.Builder getHttpClientBuilder() throws Exception;
}
