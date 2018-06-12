package com.lucidworks.spark

import java.util.UUID

import com.lucidworks.spark.util.SolrDataFrameImplicits._
import com.lucidworks.spark.util.{ConfigurationConstants, SolrCloudUtil, SolrQuerySupport, SolrSupport}
import org.apache.spark.sql.functions.{concat, lit}

class TestIndexing extends TestSuiteBuilder {

  test("Load csv file and index to Solr") {
    val collectionName = "testIndexing-" + UUID.randomUUID().toString
    SolrCloudUtil.buildCollection(zkHost, collectionName, null, 2, cloudClient, sc)
    try {
      val csvFileLocation = "src/test/resources/test-data/nyc_yellow_taxi_sample_1k.csv"
      val csvDF = sparkSession.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(csvFileLocation)
      assert(csvDF.count() == 999)

      val solrOpts = Map("zkhost" -> zkHost, "collection" -> collectionName)
      val newDF = csvDF
        .withColumn("pickup_location", concat(csvDF.col("pickup_latitude"), lit(","), csvDF.col("pickup_longitude")))
        .withColumn("dropoff_location", concat(csvDF.col("dropoff_latitude"), lit(","), csvDF.col("dropoff_longitude")))
      newDF.write.option("zkhost", zkHost).option(ConfigurationConstants.GENERATE_UNIQUE_KEY, "true").solr(collectionName)

      // Explicit commit to make sure all docs are visible
      val solrCloudClient = SolrSupport.getCachedCloudClient(zkHost)
      solrCloudClient.commit(collectionName, true, true)

      val solrDF = sparkSession.read.format("solr").options(solrOpts).load()
      solrDF.printSchema()
      assert (solrDF.count() == 999)
      solrDF.take(10)
    } finally {
      SolrCloudUtil.deleteCollection(collectionName, cluster)
    }
  }

  test("Solr field types config") {
    val collectionName = "testIndexing-" + UUID.randomUUID().toString
    SolrCloudUtil.buildCollection(zkHost, collectionName, null, 2, cloudClient, sc)
    try {
      val csvFileLocation = "src/test/resources/test-data/simple.csv"
      val csvDF = sparkSession.read.format("com.databricks.spark.csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(csvFileLocation)
      val solrOpts = Map("zkhost" -> zkHost, "collection" -> collectionName, ConfigurationConstants.SOLR_FIELD_TYPES -> "ntitle:text_en,nrating:string")
      csvDF.write.options(solrOpts).solr(collectionName)

      // Explicit commit to make sure all docs are visible
      val solrCloudClient = SolrSupport.getCachedCloudClient(zkHost)
      solrCloudClient.commit(collectionName, true, true)

      val solrBaseUrl = SolrSupport.getSolrBaseUrl(zkHost)
      val solrUrl = solrBaseUrl + collectionName + "/"

      val fieldTypes = SolrQuerySupport.getFieldTypes(Set.empty, solrUrl, cloudClient, collectionName)
      assert(fieldTypes("nrating").fieldType === "string")
      assert(fieldTypes("ntitle").fieldType === "text_en")
    } finally {
      SolrCloudUtil.deleteCollection(collectionName, cluster)
    }
  }
}
