package com.lucidworks.spark

import java.util.UUID
import com.lucidworks.spark.rdd.SolrRDD
import com.lucidworks.spark.util.ConfigurationConstants._
import com.lucidworks.spark.util.QueryConstants._
import com.lucidworks.spark.util.SolrCloudUtil
import org.apache.solr.client.solrj.SolrQuery

import org.apache.spark.Logging

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

  test("Test Streaming Expression") {
    val collectionName = "testStreamingExpr" + UUID.randomUUID().toString
    val numDocs = 10
    SolrCloudUtil.buildCollection(zkHost, collectionName, numDocs, 1, cloudClient, sc)
    
    val expr : String =
      s"""
        |search(${collectionName},
        |       q="*:*",
        |       fl="field1_s",
        |       sort="field1_s asc",
        |       qt="/export")
      """.stripMargin
    try {
      val solrQuery = new SolrQuery()
      solrQuery.set(SOLR_STREAMING_EXPR, expr)
      val streamExprRDD = new SolrRDD(zkHost, collectionName, sc, Some(QT_STREAM))
      assert(streamExprRDD.requestHandler.get == QT_STREAM)
      val results = streamExprRDD.query(solrQuery).collect()
      assert(results.size == numDocs)
    } finally {
      SolrCloudUtil.deleteCollection(collectionName, cluster)
    }
  }
}
