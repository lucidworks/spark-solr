package com.lucidworks.spark.util;

import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;

public abstract class FusionAuthHttpClient {

  private final String zkHost;

  public FusionAuthHttpClient(String zkHost) {
    this.zkHost = zkHost;
  }

  public String getZkHost() {
    return zkHost;
  }

  public abstract HttpJettySolrClient.Builder getHttpClientBuilder() throws Exception;
}
