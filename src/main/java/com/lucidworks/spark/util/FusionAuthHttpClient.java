package com.lucidworks.spark.util;

import org.apache.http.client.HttpClient;

public abstract class FusionAuthHttpClient {

  private final String zkHost;

  public FusionAuthHttpClient(String zkHost) {
    this.zkHost = zkHost;
  }

  public String getZkHost() {
    return zkHost;
  }

  public abstract HttpClient buildHttpClient() throws Exception;
}
