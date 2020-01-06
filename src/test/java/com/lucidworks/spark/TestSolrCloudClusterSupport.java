package com.lucidworks.spark;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.cloud.*;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.noggit.CharArr;
import org.noggit.JSONWriter;
import org.restlet.ext.servlet.ServerServlet;
import org.scalatest.FunSuite;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Supports tests that need a SolrCloud cluster.
 */
public class TestSolrCloudClusterSupport {

  static final Logger log = Logger.getLogger(TestSolrCloudClusterSupport.class);

  public static MiniSolrCloudCluster cluster;
  public static CloudSolrClient cloudSolrServer;
  private static File testWorkingDir;

  public CloudSolrClient getClient() {
    return cloudSolrServer;
  }

  public MiniSolrCloudCluster getCluster() {
    return cluster;
  }

  public static String readSolrXml(File solrXml) throws IOException {
    StringBuilder sb = new StringBuilder();
    String line;
    BufferedReader br = null;
    try {
      br = new BufferedReader(new InputStreamReader(new FileInputStream(solrXml), StandardCharsets.UTF_8));
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException ignore){}
      }
    }
    return sb.toString();
  }

  @BeforeClass
  public static void startCluster() throws Exception {

      System.setProperty("jetty.testMode", "true");
    File solrXml = new File("src/test/resources/solr.xml");
    String solrXmlContents = readSolrXml(solrXml);

    File targetDir = new File("target");
    if (!targetDir.isDirectory())
      fail("Project 'target' directory not found at: "+targetDir.getAbsolutePath());

    testWorkingDir = new File(targetDir, "java-solrcloud-"+System.currentTimeMillis());
    if (!testWorkingDir.isDirectory()) {
      testWorkingDir.mkdirs();
    }

    // need the schema stuff
    final SortedMap<ServletHolder,String> extraServlets = new TreeMap<ServletHolder,String>();
    final ServletHolder solrSchemaRestApi = new ServletHolder("SolrSchemaRestApi", ServerServlet.class);
    solrSchemaRestApi.setInitParameter("org.restlet.application", "org.apache.solr.rest.SolrSchemaRestApi");
    extraServlets.put(solrSchemaRestApi, "/schema/*");

    cluster = new MiniSolrCloudCluster(1, null /* hostContext */,
            testWorkingDir.toPath(), solrXmlContents, extraServlets, null);

    cloudSolrServer = new CloudSolrClient.Builder().withZkHost(cluster.getZkServer().getZkAddress()).sendUpdatesOnlyToShardLeaders().build();
    cloudSolrServer.connect();

    assertTrue(!cloudSolrServer.getZkStateReader().getClusterState().getLiveNodes().isEmpty());
  }

  @AfterClass
  public static void stopCluster() throws Exception {
    if (cloudSolrServer != null) {
      cloudSolrServer.close();
    } else {
      log.error("No CloudSolrClient available for this test! " +
              "Typically this means there was a failure to start the MiniSolrCloudCluster, check logs for more details.");
    }
    if (cluster != null) {
      cluster.shutdown();
    } else {
      log.error("No MiniSolrCloudCluster available for this test! " +
              "Typically this means there was a failure to start the MiniSolrCloudCluster, check logs for more details.");
    }

    if (testWorkingDir != null && testWorkingDir.isDirectory()) {
      FileUtils.deleteDirectory(testWorkingDir);
    }
  }

  protected void setCloudSolrServer(CloudSolrClient solrClient) {
    cloudSolrServer = solrClient;
  }

  protected static void deleteCollection(String collectionName) {
    try {
      CollectionAdminRequest.deleteCollection(collectionName);
    } catch (Exception exc) {
      log.error("Failed to delete collection '"+collectionName+"' due to: "+exc);
    }
  }

  protected static void createCollection(String collectionName, int numShards, int replicationFactor, int maxShardsPerNode, String confName) throws Exception {
    createCollection(collectionName, numShards, replicationFactor, maxShardsPerNode, confName, null);
  }

  protected static void createCollection(String collectionName, int numShards, int replicationFactor, int maxShardsPerNode, String confName, File confDir) throws Exception {
    if (confDir != null) {
      assertTrue("Specified Solr config directory '"+
        confDir.getAbsolutePath()+"' not found!", confDir.isDirectory());

      // Aggressively kill any existing collections
      deleteCollection(collectionName);

      // upload the test configs
      SolrZkClient zkClient = cloudSolrServer.getZkStateReader().getZkClient();
      ZkConfigManager zkConfigManager =
        new ZkConfigManager(zkClient);

      zkConfigManager.uploadConfigDir(confDir.toPath(), confName);
    }

    ModifiableSolrParams modParams = new ModifiableSolrParams();
    modParams.set(CoreAdminParams.ACTION, CollectionParams.CollectionAction.CREATE.name());
    modParams.set("name", collectionName);
    modParams.set("numShards", numShards);
    modParams.set("replicationFactor", replicationFactor);
    modParams.set("maxShardsPerNode", maxShardsPerNode);
    modParams.set("collection.configName", confName);
    QueryRequest request = new QueryRequest(modParams);
    request.setPath("/admin/collections");
    cloudSolrServer.request(request);
    ensureAllReplicasAreActive(collectionName, numShards, replicationFactor, 20);
  }

  protected static void ensureAllReplicasAreActive(String testCollectionName, int shards, int rf, int maxWaitSecs) throws Exception {
    long startMs = System.currentTimeMillis();

    ZkStateReader zkr = cloudSolrServer.getZkStateReader();
    zkr.updateLiveNodes(); // force the state to be fresh

    ClusterState cs = zkr.getClusterState();
    Collection<Slice> slices = cs.getCollection(testCollectionName).getActiveSlices();
    assertTrue(slices.size() == shards);
    boolean allReplicasUp = false;
    long waitMs = 0L;
    long maxWaitMs = maxWaitSecs * 1000L;
    Replica leader = null;
    while (waitMs < maxWaitMs && !allReplicasUp) {
      // refresh state every 2 secs
      if (waitMs % 2000 == 0) {
        log.info("Updating ClusterState");
        cloudSolrServer.getZkStateReader().updateLiveNodes();
      }

      cs = cloudSolrServer.getZkStateReader().getClusterState();
      assertNotNull(cs);
      allReplicasUp = true; // assume true
      for (Slice shard : cs.getCollection(testCollectionName).getActiveSlices()) {
        String shardId = shard.getName();
        assertNotNull("No Slice for " + shardId, shard);
        Collection<Replica> replicas = shard.getReplicas();
        assertTrue(replicas.size() == rf);
        leader = shard.getLeader();
        assertNotNull(leader);
        log.info("Found "+replicas.size()+" replicas and leader on "+
          leader.getNodeName()+" for "+shardId+" in "+testCollectionName);

        // ensure all replicas are "active"
        for (Replica replica : replicas) {
          String replicaState = replica.getStr(ZkStateReader.STATE_PROP);
          if (!"active".equals(replicaState)) {
            log.info("Replica " + replica.getName() + " for shard "+shardId+" is currently " + replicaState);
            allReplicasUp = false;
          }
        }
      }

      if (!allReplicasUp) {
        try {
          Thread.sleep(500L);
        } catch (Exception ignoreMe) { ignoreMe.printStackTrace(); }
        waitMs += 500L;
      }
    } // end while

    if (!allReplicasUp)
      fail("Didn't see all replicas for "+testCollectionName+
        " come up within " + maxWaitMs + " ms! ClusterState: " + printClusterStateInfo(testCollectionName));

    long diffMs = (System.currentTimeMillis() - startMs);
    log.info("Took " + diffMs + " ms to see all replicas become active for " + testCollectionName);
  }

  protected static String printClusterStateInfo(String collection) throws Exception {
    cloudSolrServer.getZkStateReader().updateLiveNodes();
    String cs = null;
    ClusterState clusterState = cloudSolrServer.getZkStateReader().getClusterState();
    if (collection != null) {
      cs = clusterState.getCollection(collection).toString();
    } else {
      Map<String,DocCollection> map = new HashMap<String,DocCollection>();
      for (String coll : clusterState.getCollectionsMap().keySet())
        map.put(coll, clusterState.getCollection(coll));
      CharArr out = new CharArr();
      new JSONWriter(out, 2).write(map);
      cs = out.toString();
    }
    return cs;
  }

  protected static void dumpSolrCollection(String collection) {
    dumpSolrCollection(collection, 100);
  }

  protected static void dumpSolrCollection(String collection, int maxRows) {
    SolrQuery q = new SolrQuery("*:*");
    q.setRows(maxRows);
    dumpSolrCollection(collection, q);
  }

  protected static void dumpSolrCollection(String collection, SolrQuery solrQuery) {
    try {
      QueryResponse qr = cloudSolrServer.query(collection, solrQuery);
      System.out.println("Found "+qr.getResults().getNumFound()+" docs in "+collection);
      int i=0;
      for (SolrDocument doc : qr.getResults()) {
        System.out.println(i+": "+doc);
        ++i;
      }
    } catch (Exception exc) {
      log.error("Failed to query Solr collection "+collection+" due to: "+exc);
    }
  }
}
