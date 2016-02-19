package com.lucidworks.spark

import java.util.UUID
import com.lucidworks.spark.rdd.SolrRDD
import org.apache.spark.Logging

class RDDTestSuite extends SparkSolrFunSuite with SparkSolrContextBuilder with Logging {

  var zkHost: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    zkHost = cluster.getZkServer.getZkAddress
  }

  test("Test Simple Query") {
    val collectionName = "testSimpleQuery" + UUID.randomUUID().toString
    SolrCloudUtil.buildCollection(zkHost, collectionName, 3, 2, cloudClient, sc)
    try {
      val newRDD = new SolrRDD(zkHost, collectionName, sc)
      assert(newRDD.count() === 3)
    } finally {
      SolrCloudUtil.deleteCollection(collectionName, cluster)
    }
  }

  test("Test RDD Partitions") {
    val collectionName = "testRDDPartitions" + UUID.randomUUID().toString
    SolrCloudUtil.buildCollection(zkHost, collectionName, 2, 4, cloudClient, sc)
    try {
      val newRDD = new SolrRDD(zkHost, collectionName, sc)
      val partitions = newRDD.partitions
      assert(partitions.length === 4)
    } finally {
      SolrCloudUtil.deleteCollection(collectionName, cluster)
    }
  }

}
