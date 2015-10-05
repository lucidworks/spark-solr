package com.lucidworks.spark.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;

public class SolrJsonSupport {

  /**
   * Utility function for sending HTTP GET request to Solr with built-in retry support.
   */
  public static Map<String,Object> getJson(HttpClient httpClient, String getUrl, int attempts) throws Exception {
    Map<String,Object> json = null;
    if (attempts >= 1) {
      try {
        json = getJson(httpClient, getUrl);
      } catch (Exception exc) {
        if (--attempts > 0 && checkCommunicationError(exc)) {
          //log.warn("Request to "+getUrl+" failed due to: "+exc.getMessage()+
          //    ", sleeping for 5 seconds before re-trying the request ...");
          try {
            Thread.sleep(2000);
          } catch (InterruptedException ie) { Thread.interrupted(); }
          
          // retry using recursion with one-less attempt available
          json = getJson(httpClient, getUrl, attempts);
        } else {
          // no more attempts or error is not retry-able
          throw exc;
        }
      }
    }
    
    return json;
  }
  
  /**
   * Utility function for sending HTTP GET request to Solr and then doing some
   * validation of the response.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static Map<String,Object> getJson(HttpClient httpClient, String getUrl) throws Exception {
    Map<String,Object> json = null;

    // ensure we're requesting JSON back from Solr
    URL url = new URL(getUrl);
    String queryString = url.getQuery();
    if (queryString != null) {
      if (queryString.indexOf("wt=json") == -1) {
        getUrl += "&wt=json";
      }
    } else {
      getUrl += "?wt=json";      
    }
       
    // Prepare a request object
    HttpGet httpget = new HttpGet(getUrl);
    
    // Execute the request
    HttpResponse response = httpClient.execute(httpget);
    
    // Get hold of the response entity
    HttpEntity entity = response.getEntity();
    int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode != 200) {
      StringBuilder body = new StringBuilder();
      if (entity != null) {
        InputStream instream = entity.getContent();
        String line;
        try {
          BufferedReader reader = 
              new BufferedReader(new InputStreamReader(instream, "UTF-8"));
          while ((line = reader.readLine()) != null) {
            body.append(line);
          }
        } catch (Exception ignore) {
          // squelch it - just trying to compose an error message here
        } finally {
          instream.close();
        }
      }
      throw new SolrException(SolrException.ErrorCode.getErrorCode(statusCode),
        "GET request [" + getUrl + "] failed due to: " + response.getStatusLine() + ": " + body);
    }
    
    // If the response does not enclose an entity, there is no need
    // to worry about connection release
    if (entity != null) {
      InputStreamReader isr = null;
      try {
        isr = new InputStreamReader(entity.getContent(), "UTF-8");
        Object resp = 
            ObjectBuilder.getVal(new JSONParser(isr));
        if (resp != null && resp instanceof Map) {
          json = (Map<String,Object>)resp;
        } else {
          throw new SolrServerException("Expected JSON object in response from "+
              getUrl+" but received "+ resp);
        }
      } catch (RuntimeException ex) {
        // In case of an unexpected exception you may want to abort
        // the HTTP request in order to shut down the underlying
        // connection and release it back to the connection manager.
        httpget.abort();
        throw ex;
      } finally {
        // Closing the input stream will trigger connection release
        isr.close();
      }
    }
    
    // lastly check the response JSON from Solr to see if it is an error
    statusCode = -1;
    Map responseHeader = (Map)json.get("responseHeader");
    if (responseHeader != null) {
      Long status = (Long)responseHeader.get("status");
      if (status != null)
        statusCode = status.intValue();
    }
    
    if (statusCode == -1)
      throw new SolrServerException("Unable to determine outcome of GET request to: "+
        getUrl+"! Response: "+json);
    
    if (statusCode != 0) {      
      String errMsg = null;
      Map error = (Map) json.get("error");
      if (error != null) {
        errMsg = (String)error.get("msg");
      }
      
      if (errMsg == null) errMsg = String.valueOf(json);
      throw new SolrException(SolrException.ErrorCode.getErrorCode(statusCode),
        "Request to "+getUrl+" failed due to: "+errMsg);
    }

    return json;
  }


  /**
   * Helper function for reading a String value from a JSON Object tree.
   */
  public static boolean asBool(String jsonPath, Map<String,Object> json) {
    boolean b = false;
    Object obj = atPath(jsonPath, json);
    if (obj != null) {
      b = (obj instanceof Boolean) ? ((Boolean)obj).booleanValue() : "true".equals(obj.toString());
    } // it's ok if it is null
    return b;
  }

  /**
   * Helper function for reading a String value from a JSON Object tree. 
   */
  public static String asString(String jsonPath, Map<String,Object> json) {
    String str = null;
    Object obj = atPath(jsonPath, json);
    if (obj != null) {
      if (obj instanceof String) {
        str = (String)obj;
      } else {
        // no ok if it's not null and of a different type
        throw new IllegalStateException("Expected a String at path "+
           jsonPath+" but found "+obj+" instead! "+json);
      }
    } // it's ok if it is null
    return str;
  }

  /**
   * Helper function for reading a Long value from a JSON Object tree.
   */
  public static Long asLong(String jsonPath, Map<String,Object> json) {
    Long num = null;
    Object obj = atPath(jsonPath, json);
    if (obj != null) {
      if (obj instanceof Long) {
        num = (Long)obj;
      } else {
        // no ok if it's not null and of a different type
        throw new IllegalStateException("Expected a Long at path "+
          jsonPath+" but found "+obj+" instead! "+json);
      }
    } // it's ok if it is null
    return num;
  }

  /**
   * Helper function for reading a List of Strings from a JSON Object tree.
   */
  @SuppressWarnings("unchecked")
  public static List<String> asList(String jsonPath, Map<String,Object> json) {
    List<String> list = null;
    Object obj = atPath(jsonPath, json);
    if (obj != null) {
      if (obj instanceof List) {
        list = (List<String>)obj;
      } else {
        // no ok if it's not null and of a different type
        throw new IllegalStateException("Expected a List at path "+
          jsonPath+" but found "+obj+" instead! "+json);
      }
    } // it's ok if it is null
    return  list;
  }

  /**
   * Helper function for reading a Map from a JSON Object tree.
   */
  @SuppressWarnings("unchecked")
  public static Map<String,Object> asMap(String jsonPath, Map<String,Object> json) {
    Map<String,Object> map = null;
    Object obj = atPath(jsonPath, json);
    if (obj != null) {
      if (obj instanceof Map) {
        map = (Map<String,Object>)obj;
      } else {
        // no ok if it's not null and of a different type
        throw new IllegalStateException("Expected a Map at path "+
          jsonPath+" but found "+obj+" instead! "+json);
      }
    } // it's ok if it is null
    return map;
  }

  /**
   * Helper function for reading an Object of unknown type from a JSON Object tree.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static Object atPath(String jsonPath, Map<String,Object> json) {
    if ("/".equals(jsonPath))
      return json;

    if (!jsonPath.startsWith("/"))
      throw new IllegalArgumentException("Invalid JSON path: "+
        jsonPath+"! Must start with a /");

    Map<String,Object> parent = json;
    Object result = null;
    String[] path = jsonPath.split("/");
    for (int p=1; p < path.length; p++) {
      Object child = parent.get(path[p]);
      if (child == null)
        break;

      if (p == path.length-1) {
        // success - found the node at the desired path
        result = child;
      } else {
        if (child instanceof Map) {
          // keep walking the path down to the desired node
          parent = (Map)child;
        } else {
          // early termination - hit a leaf before the requested node
          break;
        }
      }
    }
    return result;
  }

  /**
   * Determine if a request to Solr failed due to a communication error,
   * which is generally retry-able.
   */
  public static boolean checkCommunicationError(Exception exc) {
    Throwable rootCause = SolrException.getRootCause(exc);
    boolean wasCommError =
      (rootCause instanceof ConnectException ||
        rootCause instanceof ConnectTimeoutException ||
        rootCause instanceof NoHttpResponseException ||
        rootCause instanceof SocketException);
    return wasCommError;
  }

  public static HttpClient getHttpClient() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, 128);
    params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, 32);
    params.set(HttpClientUtil.PROP_FOLLOW_REDIRECTS, false);
    return HttpClientUtil.createClient(params);
  }

  @SuppressWarnings("deprecation")
  public static void closeHttpClient(HttpClient httpClient) {
    if (httpClient != null) {
      try {
        httpClient.getConnectionManager().shutdown();
      } catch (Exception exc) {
        // safe to ignore, we're just shutting things down
      }
    }
  }

}
