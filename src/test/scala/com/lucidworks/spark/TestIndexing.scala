package com.lucidworks.spark

import java.util.UUID

import com.lucidworks.spark.util.{ConfigurationConstants, SolrCloudUtil}

class TestIndexing extends TestSuiteBuilder {

  test("Load csv file and index to Solr") {
    val collectionName = "testIndexing-" + UUID.randomUUID().toString
    SolrCloudUtil.buildCollection(zkHost, collectionName, 3, 2, cloudClient, sc)
    try {
      val csvFileLocation = "src/test/resources/test-data/nyc_yellow_taxi_sample_1k.csv"
      val csvDF = sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(csvFileLocation)
      assert(csvDF.count() == 999)

      val solrOpts = Map("zkhost" -> zkHost, "collection" -> collectionName, ConfigurationConstants.GENERATE_UNIQUE_KEY -> "true")
      csvDF.write.format("solr").options(solrOpts).mode(org.apache.spark.sql.SaveMode.Overwrite).save()

      val solrDF = sqlContext.read.format("solr").options(solrOpts).load()
      assert (solrDF.count() == 999)
    } finally {
      SolrCloudUtil.deleteCollection(collectionName, cluster)
    }

  }

}
