/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lucidworks.spark

import java.io.File

import com.lucidworks.spark.util.SolrSupport
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.request.QueryRequest
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.cloud.MiniSolrCloudCluster
import org.apache.solr.common.SolrInputDocument
import org.apache.solr.common.cloud._
import org.apache.solr.common.params.{CollectionParams, CoreAdminParams, ModifiableSolrParams}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, Logging}
import org.eclipse.jetty.servlet.ServletHolder
import org.junit.Assert._
import org.noggit.{JSONWriter, CharArr}
import org.restlet.ext.servlet.ServerServlet
import org.scalatest.{Suite, BeforeAndAfterAll, Outcome, FunSuite}

import scala.collection.JavaConverters._

/**
 * Base abstract class for all Scala unit tests in spark-solr for handling common functionality.
 *
 * Copied from SparkFunSuite, which is inaccessible from here.
 */
trait SparkSolrFunSuite extends FunSuite with Logging {

  /**
   * Log the suite name and the test name before and after each test.
   *
   * Subclasses should never override this method. If they wish to run
   * custom code before and after each test, they should mix in the
   * {{org.scalatest.BeforeAndAfter}} trait instead.
   */
  final protected override def withFixture(test: NoArgTest): Outcome = {
    val testName = test.text
    val suiteName = this.getClass.getName
    val shortSuiteName = suiteName.replaceAll("com.lucidworks.spark", "c.l.s")
    try {
      logInfo(s"\n\n===== TEST OUTPUT FOR $shortSuiteName: '$testName' =====\n")
      test()
    } finally {
      logInfo(s"\n\n===== FINISHED $shortSuiteName: '$testName' =====\n")
    }
  }
}

trait SolrCloudTestBuilder extends BeforeAndAfterAll with Logging { this: Suite =>

  @transient var cluster: MiniSolrCloudCluster = _
  @transient var cloudClient: CloudSolrClient = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val solrXml = new File("src/test/resources/solr.xml")
    val targetDir = new File("target")
    if (!targetDir.isDirectory)
      fail("Project 'target' directory not found at :" + targetDir.getAbsolutePath)

    // need the schema stuff
    val extraServlets: java.util.SortedMap[ServletHolder, String] = new java.util.TreeMap[ServletHolder, String]()

    val solrSchemaRestApi : ServletHolder = new ServletHolder("SolrSchemaRestApi", classOf[ServerServlet])
    solrSchemaRestApi.setInitParameter("org.restlet.application", "org.apache.solr.rest.SolrSchemaRestApi")
    extraServlets.put(solrSchemaRestApi, "/schema/*")

    cluster = new MiniSolrCloudCluster(1, null, targetDir, solrXml, extraServlets, null, null)
    cloudClient = new CloudSolrClient(cluster.getZkServer.getZkAddress, true)
    cloudClient.connect()

    assertTrue(!cloudClient.getZkStateReader.getClusterState.getLiveNodes.isEmpty)
  }

  override def afterAll(): Unit = {
    cloudClient.shutdown()
    cluster.shutdown()
    super.afterAll()
  }

  def deleteCollection(collectionName: String): Unit = {
    try {
      cluster.deleteCollection(collectionName)
    } catch {
      case e => log.error("Failed to delete collection " + collectionName + " due to: " + e)
    }
  }


  def createCollection(collectionName: String,
                               numShards: Int,
                               replicationFactor: Int,
                               maxShardsPerNode: Int,
                               confName: String): Unit =
    createCollection(collectionName, numShards, replicationFactor, maxShardsPerNode, confName, null)

  def createCollection(collectionName: String,
                        numShards: Int,
                        replicationFactor: Int,
                        maxShardsPerNode: Int,
                        confName: String,
                        confDir: File): Unit = {
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
    ensureAllReplicasAreActive(collectionName, numShards, replicationFactor, 20)
  }

  def ensureAllReplicasAreActive(collectionName: String,
                                  numShards: Int,
                                  replicationFactor: Int,
                                  maxWaitSecs: Int): Unit = {
    val startMs: Long = System.currentTimeMillis()
    val zkr: ZkStateReader = cloudClient.getZkStateReader
    zkr.updateLiveNodes() // force the state to be fresh

    var cs: ClusterState = zkr.getClusterState
    val slices: java.util.Collection[Slice] = cs.getActiveSlices(collectionName)
    assert(slices.size() === numShards)
    var allReplicasUp: Boolean = false
    var waitMs = 0L
    val maxWaitMs = maxWaitSecs * 1000L
    var leader: Replica = null
    while (waitMs < maxWaitMs && !allReplicasUp) {
      // refresh state every 2 secs
      if (waitMs % 2000 === 0) {
        log.info("Updating ClusterState")
        cloudClient.getZkStateReader.updateLiveNodes()
      }

      cs = cloudClient.getZkStateReader.getClusterState
      assertNotNull(cs)
      allReplicasUp = true // assume true
      for (shard: Slice <- cs.getActiveSlices(collectionName).asScala) {
        val shardId: String = shard.getName
        assertNotNull("No Slice for " + shardId, shard)
        val replicas = shard.getReplicas
        assertTrue(replicas.size() === replicationFactor)
        leader = shard.getLeader
        assertNotNull(leader)
        log.info("Found " + replicas.size() + " replicas and leader on " + leader.getNodeName + " for " + shardId + " in " + collectionName)

        // ensure all replicas are "active"
        for (replica: Replica <- replicas.asScala) {
          val replicaState = replica.getStr(ZkStateReader.STATE_PROP)
          if (!"active".equals(replicaState)) {
            log.info("Replica " + replica.getName + " for shard " + shardId + " is currently " + replicaState)
            allReplicasUp = false
          }
        }
      }

      if (!allReplicasUp) {
        try {
          Thread.sleep(500L)
        } catch {
          case _ => // Do nothing
        }
        waitMs = waitMs + 500L
      }
    } // end while

    if (!allReplicasUp)
      fail("Didn't see all replicas for " + collectionName + " come up within " + maxWaitMs + " ms! ClusterState: " + printClusterStateInfo(collectionName))

    val diffMs = System.currentTimeMillis() - startMs
    log.info("Took '" + diffMs + "' ms to see all replicas become active for " + collectionName)
  }

  def printClusterStateInfo(collectionName: String): String = {
    cloudClient.getZkStateReader.updateLiveNodes()
    var cs: String = null
    val clusterState: ClusterState = cloudClient.getZkStateReader.getClusterState
    if (collectionName != null) {
      cs = clusterState.getCollection(collectionName).toString
    } else {
      val map = Map.empty[String, DocCollection]
      clusterState.getCollections.asScala.foreach(coll => {
        map + (coll -> clusterState.getCollection(coll))
      })
      val out: CharArr = new CharArr()
      new JSONWriter(out, 2).write(map)
      cs = out.toString
    }
    cs
  }

  def dumpSolrCollection(collectionName: String): Unit = {
    dumpSolrCollection(collectionName, 100)
  }

  def dumpSolrCollection(collectionName: String, maxRows: Int): Unit = {
    val q = new SolrQuery("*:*")
    q.setRows(maxRows)
    dumpSolrCollection(collectionName, q)
  }

  def dumpSolrCollection(collectionName: String, solrQuery: SolrQuery): Unit = {
    try {
      val qr: QueryResponse = cloudClient.query(collectionName, solrQuery)
      log.info("Found " + qr.getResults.getNumFound + " docs in " + collectionName)
      var i = 0
      import scala.collection.JavaConverters._
      for (doc <- qr.getResults.asScala) {
        log.info(i + ":" + doc)
        i += 1
      }
    }
  }

}


trait SparkSolrContextBuilder extends SolrCloudTestBuilder { this: Suite =>

  @transient var sc: SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set("spark.default.parallelism", "1")
    sc = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    sc.stop()
    super.afterAll()
  }


  def buildCollection(zkHost: String,
                      collection: String): Unit = {
    val inputDocs: Array[String] = Array(
      collection + "-1,foo,bar,1,[a;b],[1;2]",
      collection + "-2,foo,baz,2,[c;d],[3;4]",
      collection + "-3,bar,baz,3,[e;f],[5;6]"
    )
    buildCollection(zkHost, collection, inputDocs, 2)
  }

  def buildCollection(zkHost: String,
                      collection: String,
                      numDocs: Int,
                      numShards: Int): Unit = {
    val inputDocs: Array[String] = new Array[String](numDocs)
    for (n: Int <- 0 to numDocs) {
      inputDocs.update(n, collection + "-" + n + ",foo" + n + ",bar" + n + "," + n + ",[a;b],[1;2]")
    }
    buildCollection(zkHost, collection, inputDocs, numShards)
  }

  def buildCollection(zkHost: String,
                      collection: String,
                      inputDocs: Array[String],
                      numShards: Int): Unit = {
    val confName = "testConfig"
    val confDir = new File("src/test/resources/conf")
    val replicationFactor: Int = 1
    createCollection(collection, numShards, replicationFactor, numShards, confName, confDir)

    // index some docs in to the new collection
    if (inputDocs != null) {
      val numDocsIndexed: Int = indexDocs(zkHost, collection, inputDocs)
      Thread.sleep(1000L)
      // verify docs got indexed .. relies on soft auto-commits firing frequently
      val solrRDD: SolrScalaRDD = new SolrScalaRDD(zkHost, collection, sc)
      val numFound = solrRDD.count()
      assertTrue("expected " + numDocsIndexed + " docs in query results from " + collection + ", but got " + numFound, numFound === numDocsIndexed)
    }
  }

  def indexDocs(zkHost: String,
                collection: String,
                inputDocs: Array[String]): Int = {
    val input : RDD[String] = sc.parallelize(inputDocs, 1)
    val docs: RDD[SolrInputDocument] = input.map(row => {
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

      doc
    })
    SolrSupport.indexDocs(zkHost, collection, 1, docs)
    inputDocs.length
  }

}
