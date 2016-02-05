package com.lucidworks.spark

import com.lucidworks.spark.util.SolrSupport
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.spark.{SparkContext, SparkConf, Logging}

class RDDTestSuite extends SparkSolrFunSuite with SparkSolrContextBuilder with Logging {

  var zkHost: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    zkHost = cluster.getZkServer.getZkAddress

    // Cloud client for tests on external Solr Clusters
//    zkHost = "172.17.0.2:2181"
//    cloudClient = new CloudSolrClient("172.17.0.2:2181", true)
//    cloudClient.connect()
//    sc = new SparkContext(new SparkConf()
//                            .setMaster("spark://172.17.0.3:7077")
//                            .setAppName("RDD Test")
//                            .set("spark.default.parallelism", "2")
//                            .set("spark.cores.max", "2"))
  }

  test("Test Simple Query") {
    val collectionName = "testSimpleQuery"
    SolrCloudUtil.buildCollection(zkHost, collectionName, 3, 2, cloudClient, sc)
    try {
      val newRDD = new SolrScalaRDD(zkHost, collectionName, sc)
      assert(newRDD.count() === 3)
    } finally {
      SolrCloudUtil.deleteCollection(collectionName, SolrSupport.getSolrBaseUrl(zkHost))
    }
  }

  test("Test RDD Partitions") {
    val collectionName = "testRDDPartitions"
    SolrCloudUtil.buildCollection(zkHost, collectionName, 2, 4, cloudClient, sc)
    try {
      val newRDD = new SolrScalaRDD(zkHost, collectionName, sc)
      val partitions = newRDD.partitions
      assert(partitions.length === 4)
    } finally {
      SolrCloudUtil.deleteCollection(collectionName, SolrSupport.getSolrBaseUrl(zkHost))
    }
  }

}
