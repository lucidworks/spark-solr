package com.lucidworks.spark.util;

import org.apache.solr.client.solrj.impl.HttpClientConfigurer;

public class FusionHttpClientConfigurer extends HttpClientConfigurer {

  private final String zkHost;

  public FusionHttpClientConfigurer(String zkHost) {
    this.zkHost = zkHost;
  }

  public String getZkHost() {
    return zkHost;
  }
}
