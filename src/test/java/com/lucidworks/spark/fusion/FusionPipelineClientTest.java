package com.lucidworks.spark.fusion;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.junit.Rule;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.Assert.assertEquals;

public class FusionPipelineClientTest {
  private static final String defaultWireMockRulePort = "8089";
  private static final String defaultFusionPort = "8764";
  private static final String defaultFusionServerHttpString = "http://";
  private static final String defaultHost = "localhost";
  private static final String defaultCollection = "test";
  private static final String defaultFusionIndexingPipeline = "conn_solr";
  private static final String defaultFusionProxyBaseUrl = "/api/apollo";
  private static final String defaultFusionIndexingPipelineUrlExtension = "/index-pipelines";
  private static final String defaultFusionSessionApi = "/api/session?realmName=";
  private static final String defaultFusionUser = "admin";
  private static final String defaultFusionPass = "password123";
  private static final String defaultFusionRealm = "native";
  private static final String defaultFusionIndexingPipelineUrlTerminatingString = "/index";
  private static final String defaultFusionSolrProxyUrlExtension = "/solr";

  private static String fusionServerHttpString;
  private static String fusionHost;
  private static String fusionApiPort;
  private static String wireMockRulePort = defaultWireMockRulePort;
  private static String fusionCollection;
  private static String fusionCollectionForUrl;
  private static String fusionIndexingPipeline;
  private static String fusionProxyBaseUrl;
  private static String fusionIndexingPipelineUrlExtension;
  private static String fusionSessionApi;
  private static String fusionUser;
  private static String fusionPass;
  private static String fusionRealm;
  private static String fusionSolrProxyUrlExtension;
  private static Boolean useWireMockRule = true;

  private static final Log log = LogFactory.getLog(FusionPipelineClient.class);

  static {

    ClassLoader cl = ClassLoader.getSystemClassLoader();
    try (InputStream in = cl.getResourceAsStream("wire-mock-props.xml")) {
      Properties prop = new Properties();
      prop.loadFromXML(in);

      useWireMockRule = "true".equalsIgnoreCase(String.valueOf(prop.getProperty("useWireMockRule", "true")));
      if (useWireMockRule) {
        // Set host and port when using WireMockRules.
        fusionHost = prop.getProperty("wireMockRuleHost", defaultHost) + ":";
        fusionApiPort = prop.getProperty("wireMockRulePort", defaultWireMockRulePort);
        wireMockRulePort = fusionApiPort;
      } else {
        // Set host and port when connecting to Fusion.
        fusionHost = prop.getProperty("fusionHost", defaultHost) + ":";
        fusionApiPort = prop.getProperty("fusionApiPort", defaultFusionPort);
      }

      // Set http string (probably always either http:// or https://).
      fusionServerHttpString = prop.getProperty("fusionServerHttpString", defaultFusionServerHttpString);

      // Set collection.
      fusionCollection = prop.getProperty("fusionCollection", defaultCollection);
      fusionCollectionForUrl = "/" + fusionCollection;

      // Set the fusion indexing pipeline.
      fusionIndexingPipeline = "/" +  prop.getProperty("fusionIndexingPipeline", defaultFusionIndexingPipeline);

      // Set the fusion proxy base URL.
      fusionProxyBaseUrl = prop.getProperty("fusionProxyBaseUrl", defaultFusionProxyBaseUrl);

      // Set the fusion indexing pipeline URL extension.
      fusionIndexingPipelineUrlExtension = prop.getProperty("fusionIndexingPipelineUrlExtension", defaultFusionIndexingPipelineUrlExtension);

      // Set the fusion session API.
      fusionSessionApi = prop.getProperty("fusionSessionApi", defaultFusionSessionApi);

      // Set the fusion user.
      fusionUser = prop.getProperty("fusionUser", defaultFusionUser);

      // Set the fusion password.
      fusionPass = prop.getProperty("fusionPass", defaultFusionPass);

      // Set the fusion realm.
      fusionRealm = prop.getProperty("fusionRealm", defaultFusionRealm);

      // Set the fusion solr proxy URL extension.
      fusionSolrProxyUrlExtension = prop.getProperty("fusionSolrProxyUrlExtension", defaultFusionSolrProxyUrlExtension);
    } catch (IOException e) {
      log.error("Error reading properties.xml file due to exception:[" + e.toString() + "]");
      throw new RuntimeException();
    }
  }

  private static final String fusionCollectionApiStrValForUrl = "/collections";

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(Integer.parseInt(wireMockRulePort)); // No-args constructor defaults to port 8080

  @Test
  public void testHappyPath() throws Exception {

    String fusionHostAndPort = "http://" + fusionHost + fusionApiPort;
    String fusionPipelineUrlWithoutHostAndPort = fusionProxyBaseUrl + fusionIndexingPipelineUrlExtension +
           fusionIndexingPipeline + fusionCollectionApiStrValForUrl + fusionCollectionForUrl +
           defaultFusionIndexingPipelineUrlTerminatingString;
    String fusionUrl = fusionHostAndPort + fusionPipelineUrlWithoutHostAndPort;
    String fusionSolrProxyWithoutHostAndPort = fusionSolrProxyUrlExtension + fusionCollectionForUrl;

    log.info("testHappyPath running with: fusionSolrProxyWithoutHostAndPort=" + fusionSolrProxyWithoutHostAndPort +
      " fusionPipelineUrlWithoutHostAndPort=" + fusionPipelineUrlWithoutHostAndPort +
      " wireMockRulePort=" + wireMockRulePort + " useWireMockRule=" + useWireMockRule);

    String badPath = "/api/apollo/index-pipelines/scottsCollection-default/collections/badCollection/index";
    String unauthPath = "/api/apollo/index-pipelines/scottsCollection-default/collections/unauthCollection/index";
    if (useWireMockRule) {
      // mock out the Pipeline API
      //  stubFor(post(urlEqualTo("/api/apollo/index-pipelines")).willReturn(aResponse().withStatus(200)));
      stubFor(post(urlEqualTo(fusionPipelineUrlWithoutHostAndPort)).willReturn(aResponse().withStatus(200)));

      stubFor(get(urlEqualTo(fusionProxyBaseUrl + fusionIndexingPipelineUrlExtension)).willReturn(aResponse().withStatus(200).withBody("hello")));

      // a bad node in the mix ... to test FusionPipelineClient error handling
      stubFor(post(urlEqualTo(badPath)).willReturn(aResponse().withStatus(500)));

      // another bad node in the mix which produces un-authorized errors... to test FusionPipelineClient error handling
      stubFor(post(urlEqualTo(unauthPath)).willReturn(aResponse().withStatus(401)));

      // mock out the Session API
      stubFor(post(urlEqualTo("/api/session?realmName=" + fusionRealm)).willReturn(aResponse().withStatus(200)));
    }

    String fusionEndpoints = useWireMockRule ?
      fusionUrl+
        ",http://localhost:"+wireMockRulePort+"/not_and_endpoint/api" +
        ",http://localhost:"+wireMockRulePort+badPath+
        ",http://localhost:"+wireMockRulePort+unauthPath : fusionUrl;

    String[] urls = fusionEndpoints.split(",");
    URL url = new URL(urls[0]);
    final String pipelinePath = url.getPath();

    final FusionPipelineClient pipelineClient =
      new FusionPipelineClient(fusionEndpoints, fusionUser, fusionPass, fusionRealm);

    int numThreads = 3;
    ExecutorService pool = Executors.newFixedThreadPool(numThreads);
    for (int t=0; t < numThreads; t++) {
      pool.submit(new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          for (int i = 0; i < 10; i++) {
            try {
              pipelineClient.postBatchToPipeline(pipelinePath,buildDocs(1));
            } catch (Exception exc) {
              log.error("\n\nFailed to postBatch due to: " + exc+"\n\n");
              throw new RuntimeException(exc);
            }
          }
          return null;
        }
      });
    }

    /*
    log.info("Sleeping for 10 minutes to let sessions expire");
    Thread.sleep(602*1000);
    log.info("Done sleeping for 10 minutes ... resending docs");

    for (int t=0; t < numThreads; t++) {
      pool.submit(new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          for (int i = 0; i < 10; i++) {
            try {
              pipelineClient.postBatchToPipeline(buildDocs(1));
            } catch (Exception exc) {
              log.error("\n\nFailed to postBatch due to: " + exc+"\n\n");
              throw new RuntimeException(exc);
            }
          }
          return null;
        }
      });
    }
    */

    pool.shutdown(); // Disable new tasks from being submitted
    try {
      // Wait a while for existing tasks to terminate
      if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
        pool.shutdownNow(); // Cancel currently executing tasks
        // Wait a while for tasks to respond to being cancelled
        if (!pool.awaitTermination(30, TimeUnit.SECONDS))
          System.err.println("Pool did not terminate");
      }
    } catch (InterruptedException ie) {
      // (Re-)Cancel if current thread also interrupted
      pool.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }

    HttpGet getRequest = new HttpGet(fusionHostAndPort+fusionProxyBaseUrl+fusionIndexingPipelineUrlExtension);
    HttpEntity entity = pipelineClient.sendRequestToFusion(getRequest);
    String body = pipelineClient.extractResponseBodyText(entity);
    EntityUtils.consumeQuietly(entity);
    assertEquals("hello", body);

    Thread.sleep(6000);
  }

  protected List<Map<String,Object>> buildDocs(int numDocs) {
    List<Map<String,Object>> docs = new ArrayList<Map<String,Object>>(numDocs);
    for (int n=0; n < numDocs; n++) {
      Map<String,Object> doc = new HashMap<String, Object>();
      doc.put("id", "doc"+n);
      doc.put("str_s", "str "+n);
      docs.add(doc);
    }
    return docs;
  }
}
