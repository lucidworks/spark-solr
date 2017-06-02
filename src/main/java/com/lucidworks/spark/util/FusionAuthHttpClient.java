package com.lucidworks.spark.util;

import org.apache.solr.client.solrj.impl.HttpSolrClient;

public abstract class FusionAuthHttpClient {

  private final String zkHost;

  public FusionAuthHttpClient(String zkHost) {
    this.zkHost = zkHost;
  }

  public String getZkHost() {
    return zkHost;
  }

  public abstract HttpSolrClient.Builder getHttpClientBuilder() throws Exception;
}
