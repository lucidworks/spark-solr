package com.lucidworks.spark

import java.util.UUID

import com.lucidworks.spark.util.SolrCloudUtil
import com.typesafe.scalalogging.LazyLogging

class SolrJsonTestSuite extends TestSuiteBuilder with LazyLogging {

  test("Test Solr JSON") {
    val collectionName = "testSimpleQuery" + UUID.randomUUID().toString
    SolrCloudUtil.buildCollection(zkHost, collectionName, 3, 1, cloudClient, sc)
  }
}
