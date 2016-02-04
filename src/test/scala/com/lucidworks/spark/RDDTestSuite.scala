package com.lucidworks.spark

import org.apache.spark.Logging

class RDDTestSuite extends SparkSolrFunSuite with SparkSolrContextBuilder with Logging {

  var zkHost: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    zkHost = cluster.getZkServer.getZkAddress
  }

  test("Test Simple Query") {
    val collectionName = "testSimpleQuery"
    buildCollection(zkHost, collectionName, 3, 2)
    try {
      val newRDD = new SolrScalaRDD(zkHost, collectionName, sc)
      assert(newRDD.count() === 3)
    } finally {
      deleteCollection(collectionName)
    }
  }

  test("Test RDD Partitions") {
    val collectionName = "testRDDPartitions"
    buildCollection(zkHost, collectionName, 2, 4)
    try {
      val newRDD = new SolrScalaRDD(zkHost, collectionName, sc)
      val partitions = newRDD.partitions
      assert(partitions.length === 4)
    } finally {
      deleteCollection(collectionName)
    }
  }

}
