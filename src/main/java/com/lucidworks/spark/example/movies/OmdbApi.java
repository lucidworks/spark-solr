package com.lucidworks.spark.example.movies;

import com.lucidworks.spark.util.SolrJsonSupport;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.http.client.methods.HttpGet;
import org.codehaus.jackson.map.ObjectMapper;

import java.net.URLEncoder;
import java.util.Map;

public class OmdbApi {

  private static final RateLimiter rateLimiter = RateLimiter.create(10);
  private static HttpClient httpClient = null;
  private static ObjectMapper objectMapper = new ObjectMapper();

  private static HttpClient getHttpClient() {
    synchronized (OmdbApi.class) {
      if (httpClient == null) {
        httpClient = SolrJsonSupport.getHttpClient();
      }
    }
    return httpClient;
  }

  public static Map<String,Object> byTitle(String title, String year) throws Exception {
    HttpClient http = getHttpClient();
    String getUri = "http://www.omdbapi.com/?t="+URLEncoder.encode(title)+"&y="+URLEncoder.encode(year)+"&plot=full&r=json";
    HttpGet get = new HttpGet(getUri);
    rateLimiter.acquire();
    System.out.println("Sending GET request to: "+get.toString());
    HttpResponse resp = http.execute(get);
    String rawJson = SolrJsonSupport.getHttpResponseAsString(resp.getEntity());
    return objectMapper.readValue(rawJson, Map.class);
  }
}
