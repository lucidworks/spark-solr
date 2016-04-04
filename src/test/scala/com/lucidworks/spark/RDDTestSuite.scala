package com.lucidworks.spark

import java.util.UUID
import com.lucidworks.spark.rdd.SolrRDD
import com.lucidworks.spark.util.SolrCloudUtil

import org.apache.spark.Logging
import org.scalatest.Ignore

class RDDTestSuite extends TestSuiteBuilder with Logging {

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

  ignore("Test Simple Query that uses ExportHandler") {
    val collectionName = "testSimpleQuery" + UUID.randomUUID().toString
    SolrCloudUtil.buildCollection(zkHost, collectionName, 3999, 2, cloudClient, sc)
    try {
      val newRDD = new SolrRDD(zkHost, collectionName, sc, rows=Option(Integer.MAX_VALUE)).useExportHandler
      val cnt = newRDD.count()
      print("\n********************** RDD COUNT IS = " + cnt + "\n\n")
      assert(cnt === 3999)
    } finally {
      SolrCloudUtil.deleteCollection(collectionName, cluster)
    }
  }

  ignore("Test RDD Partitions with an RDD that uses query using ExportHandler") {
    val collectionName = "testRDDPartitions" + UUID.randomUUID().toString
    SolrCloudUtil.buildCollection(zkHost, collectionName, 1002, 14, cloudClient, sc)
    try {
      val newRDD = new SolrRDD(zkHost, collectionName, sc, rows=Option(Integer.MAX_VALUE)).useExportHandler
      val partitions = newRDD.partitions
      assert(partitions.length === 14)
    } finally {
      SolrCloudUtil.deleteCollection(collectionName, cluster)
    }
  }

}
