package com.lucidworks.spark.util

import java.io.File

import com.lucidworks.spark.LazyLogging
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.request.{CollectionAdminRequest, QueryRequest, UpdateRequest}
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.cloud.MiniSolrCloudCluster
import org.apache.solr.common.SolrInputDocument
import org.apache.solr.common.cloud._
import org.apache.solr.common.params.{CollectionParams, CoreAdminParams, ModifiableSolrParams}
import org.apache.spark.SparkContext
import org.junit.Assert._
import org.noggit.{CharArr, JSONWriter}

import scala.collection.JavaConversions._

object SolrCloudUtil extends LazyLogging {

  def deleteCollection(collectionName: String, cluster: MiniSolrCloudCluster): Unit = {
    try {
      CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient)
    } catch {
      case e: Exception => logger.error("Failed to delete collection " + collectionName + " due to: " + e)
    }
  }

  def createCollection(collectionName: String,
                       numShards: Int,
                       replicationFactor: Int,
                       maxShardsPerNode: Int,
                       confName: String,
                       cloudClient: CloudSolrClient): Unit =
    createCollection(collectionName, numShards, replicationFactor, maxShardsPerNode, confName, null, cloudClient)

  def createCollection(collectionName: String,
                       numShards: Int,
                       replicationFactor: Int,
                       maxShardsPerNode: Int,
                       confName: String,
                       confDir: File,
                       cloudClient: CloudSolrClient): Unit = {
    if (confDir != null) {
      assertTrue("Specified Solr config directory '" + confDir.getAbsolutePath + "' not found!", confDir.isDirectory)
      // upload test configs

      val zkClient = cloudClient.getZkStateReader.getZkClient
      val zkConfigManager = new ZkConfigManager(zkClient)
      zkConfigManager.uploadConfigDir(confDir.toPath, confName)
    }

    val modParams = new ModifiableSolrParams()
    modParams.set(CoreAdminParams.ACTION, CollectionParams.CollectionAction.CREATE.name)
    modParams.set("name", collectionName)
    modParams.set("numShards", numShards)
    modParams.set("replicationFactor", replicationFactor)
    modParams.set("maxShardsPerNode", maxShardsPerNode)
    modParams.set("collection.configName", confName)
    val request: QueryRequest = new QueryRequest(modParams)
    request.setPath("/admin/collections")
    cloudClient.request(request)
    ensureAllReplicasAreActive(collectionName, numShards, replicationFactor, 20, cloudClient)
  }

  def ensureAllReplicasAreActive(collectionName: String,
                                 numShards: Int,
                                 replicationFactor: Int,
                                 maxWaitSecs: Int,
                                 cloudClient: CloudSolrClient): Unit = {
    val startMs: Long = System.currentTimeMillis()
    val zkr: ZkStateReader = cloudClient.getZkStateReader
    zkr.updateLiveNodes() // force the state to be fresh

    var cs: ClusterState = zkr.getClusterState
    val slices: java.util.Collection[Slice] = cs.getCollection(collectionName).getActiveSlices
    assert(slices.size() == numShards)
    var allReplicasUp: Boolean = false
    var waitMs = 0L
    val maxWaitMs = maxWaitSecs * 1000L
    var leader: Replica = null
    while (waitMs < maxWaitMs && !allReplicasUp) {
      // refresh state every 2 secs
      if (waitMs % 2000 == 0) {
        logger.info("Updating ClusterState")
        cloudClient.getZkStateReader.updateLiveNodes()
      }

      cs = cloudClient.getZkStateReader.getClusterState
      assertNotNull(cs)
      allReplicasUp = true // assume true
      for (shard: Slice <- cs.getCollection(collectionName).getActiveSlices) {
        val shardId: String = shard.getName
        assertNotNull("No Slice for " + shardId, shard)
        val replicas = shard.getReplicas
        assertTrue(replicas.size() == replicationFactor)
        leader = shard.getLeader
        assertNotNull(leader)
        logger.info("Found " + replicas.size() + " replicas and leader on " + leader.getNodeName + " for " + shardId + " in " + collectionName)

        // ensure all replicas are "active"
        for (replica: Replica <- replicas) {
          val replicaState = replica.getStr(ZkStateReader.STATE_PROP)
          if (!"active".equals(replicaState)) {
            logger.info("Replica " + replica.getName + " for shard " + shardId + " is currently " + replicaState)
            allReplicasUp = false
          }
        }
      }

      if (!allReplicasUp) {
        try {
          Thread.sleep(500L)
        } catch {
          case _: Exception => // Do nothing
        }
        waitMs = waitMs + 500L
      }
    } // end while

    if (!allReplicasUp)
      fail("Didn't see all replicas for " + collectionName +
        " come up within " + maxWaitMs + " ms! ClusterState: " + printClusterStateInfo(collectionName, cloudClient))

    val diffMs = System.currentTimeMillis() - startMs
    logger.info("Took '" + diffMs + "' ms to see all replicas become active for " + collectionName)
  }

  def printClusterStateInfo(collectionName: String, cloudClient: CloudSolrClient): String = {
    cloudClient.getZkStateReader.updateLiveNodes()
    var cs: String = null
    val clusterState: ClusterState = cloudClient.getZkStateReader.getClusterState
    if (collectionName != null) {
      cs = clusterState.getCollection(collectionName).toString
    } else {
      val map = Map.empty[String, DocCollection]
      clusterState.getCollectionsMap.keySet().foreach(coll => {
        map + (coll -> clusterState.getCollection(coll))
      })
      val out: CharArr = new CharArr()
      new JSONWriter(out, 2).write(map)
      cs = out.toString
    }
    cs
  }

  def dumpSolrCollection(collectionName: String, cloudClient: CloudSolrClient): Unit = {
    dumpSolrCollection(collectionName, 100, cloudClient)
  }

  def dumpSolrCollection(collectionName: String, maxRows: Int, cloudClient: CloudSolrClient): Unit = {
    val q = new SolrQuery("*:*")
    q.setRows(maxRows)
    dumpSolrCollection(collectionName, q, cloudClient)
  }

  def dumpSolrCollection(collectionName: String, solrQuery: SolrQuery, cloudClient: CloudSolrClient): Unit = {
    val qr: QueryResponse = cloudClient.query(collectionName, solrQuery)
    logger.info("Found " + qr.getResults.getNumFound + " docs in " + collectionName)
    var i = 0
    for (doc <- qr.getResults) {
      logger.info(i + ":" + doc)
      i += 1
    }
  }

  def buildCollection(zkHost: String,
                      collection: String,
                      cloudClient: CloudSolrClient,
                      sc: SparkContext): Unit = {
    val inputDocs: Array[String] = Array(
      collection + "-1,foo,bar,1,[a;b],[1;2]",
      collection + "-2,foo,baz,2,[c;d],[3;4]",
      collection + "-3,bar,baz,3,[e;f],[5;6]"
    )
    buildCollection(zkHost, collection, inputDocs, 2, cloudClient, sc)
  }

  def buildCollection(zkHost: String,
                      collection: String,
                      numDocs: Int,
                      numShards: Int,
                      cloudClient: CloudSolrClient,
                      sc: SparkContext): Unit = {
    val inputDocs: Array[String] = new Array[String](numDocs)
    for (n: Int <- 0 to numDocs-1) {
      inputDocs.update(n, collection + "-" + n + ",foo" + n + ",bar" + n + "," + n + ",[a;b],[1;2]")
    }
    buildCollection(zkHost, collection, inputDocs, numShards, cloudClient, sc)
  }

  def buildCollection(zkHost: String,
                      collection: String,
                      inputDocs: Array[String],
                      numShards: Int,
                      cloudClient: CloudSolrClient,
                      sc: SparkContext): Unit = {
    val confName = "testConfig"
    val confDir = new File("src/test/resources/conf")
    val replicationFactor: Int = 1
    createCollection(collection, numShards, replicationFactor, numShards, confName, confDir, cloudClient)

    // index some docs in to the new collection
    if (inputDocs != null) {
      val numDocsIndexed: Int = indexDocs(zkHost, collection, inputDocs, cloudClient)
      Thread.sleep(1000L)
      // verify docs got indexed .. relies on soft auto-commits firing frequently
      val solrParams = new ModifiableSolrParams()
      solrParams.set("q", "*:*")
      val response: QueryResponse = cloudClient.query(collection, solrParams)
      assert(response.getStatus == 0)
      val numFound = response.getResults.getNumFound
      assertTrue("expected " + numDocsIndexed + " docs in query results from " + collection + ", but got " + numFound, numFound == numDocsIndexed)
    }
  }

  def indexDocs(zkHost: String,
                collection: String,
                inputDocs: Array[String],
                cloudClient: CloudSolrClient): Int = {
    val updateRequest = new UpdateRequest()

    inputDocs.foreach(row => {
      val fields = row.split(",")
      if (fields.length < 6)
        throw new IllegalArgumentException("Each test input doc should have 6 fields! invalid doc: " + row)

      val doc = new SolrInputDocument()
      doc.setField("id", fields(0))
      doc.setField("field1_s", fields(1))
      doc.setField("field2_s", fields(2))
      doc.setField("field3_i", fields(3).toInt)

      var list = fields(4).substring(1, fields(4).length-1).split(";")
      list.foreach(s => doc.addField("field4_ss", s))

      list = fields(5).substring(1, fields(5).length-1).split(";")
      list.foreach(s => doc.addField("field5_ii", s.toInt))

      updateRequest.add(doc)
      doc
    })

    updateRequest.process(cloudClient, collection)
    inputDocs.length
  }
}
