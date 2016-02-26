package com.lucidworks.spark

import java.io.File
import java.util.UUID

import com.lucidworks.spark.util.{EventsimUtil, SolrCloudUtil}
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.cloud.MiniSolrCloudCluster
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.eclipse.jetty.servlet.ServletHolder
import org.junit.Assert._
import org.restlet.ext.servlet.ServerServlet
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, Suite}


trait SolrCloudTestBuilder extends BeforeAndAfterAll with Logging { this: Suite =>

  @transient var cluster: MiniSolrCloudCluster = _
  @transient var cloudClient: CloudSolrClient = _
  var zkHost: String = _

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
    zkHost = cluster.getZkServer.getZkAddress
  }

  override def afterAll(): Unit = {
    cloudClient.close()
    cluster.shutdown()
    super.afterAll()
  }

}

trait SparkSolrContextBuilder extends BeforeAndAfterAll { this: Suite =>

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
}

// General builder to be used by all the tests that need Solr and Spark running
trait TestSuiteBuilder extends SparkSolrFunSuite with SparkSolrContextBuilder with SolrCloudTestBuilder {}

// Builder to be used by all the tests that need Eventsim data. All test methods will re-use the same collection name
trait EventsimBuilder extends TestSuiteBuilder {

  val collectionName: String = "EventsimTest-" + UUID.randomUUID().toString

  override def beforeAll(): Unit = {
    super.beforeAll()
    SolrCloudUtil.buildCollection(zkHost, collectionName, null, numShards, cloudClient, sc)
    EventsimUtil.defineSchemaForEventSim(zkHost, collectionName)
    EventsimUtil.loadEventSimDataSet(zkHost, collectionName)
  }


  override def afterAll(): Unit = {
    SolrCloudUtil.deleteCollection(collectionName, cluster)
    super.afterAll()
  }

  def eventSimCount: Int = 1000

  def fieldsCount: Int = 18

  def numShards: Int = 2
}

