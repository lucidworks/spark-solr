package com.lucidworks.spark

import java.util.UUID

import com.lucidworks.spark.util.{SolrJsonSupport, SolrCloudUtil}
import org.apache.spark.Logging

class SolrJsonTestSuite extends TestSuiteBuilder with Logging {

  test("Test Solr JSON") {
    val collectionName = "testSimpleQuery" + UUID.randomUUID().toString
    SolrCloudUtil.buildCollection(zkHost, collectionName, 3, 1, cloudClient, sc)
  }
}
