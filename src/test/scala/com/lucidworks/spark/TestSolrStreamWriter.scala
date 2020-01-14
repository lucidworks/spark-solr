package com.lucidworks.spark

import java.io.File
import java.util.UUID

import com.lucidworks.spark.util.{ConfigurationConstants, SolrCloudUtil, SolrQuerySupport, SolrSupport}
import org.apache.commons.io.FileUtils
import org.apache.spark.solr.SparkInternalObjects

class TestSolrStreamWriter extends TestSuiteBuilder {

  test("Stream data into Solr") {
    val collectionName = "testStreaming-" + UUID.randomUUID().toString
    SolrCloudUtil.buildCollection(zkHost, collectionName, null, 1, cloudClient, sc)
    sparkSession.conf.set("spark.sql.streaming.schemaInference", "true")
    sparkSession.sparkContext.setLogLevel("DEBUG")
    val offsetsDir = FileUtils.getTempDirectory + "/spark-stream-offsets-" + UUID.randomUUID().toString
    try {
      val datasetPath = "src/test/resources/test-data/oneusagov"
      val streamingJsonDF = sparkSession.readStream.json(datasetPath)
      val accName = "acc-" + UUID.randomUUID().toString
      assert(streamingJsonDF.isStreaming)
      val writeOptions = Map(
        "collection" -> collectionName,
        "zkhost" -> zkHost,
        "checkpointLocation" -> offsetsDir,
        ConfigurationConstants.GENERATE_UNIQUE_KEY -> "true",
        ConfigurationConstants.ACCUMULATOR_NAME -> accName)
      val streamingQuery = streamingJsonDF
        .drop("_id")
        .writeStream
        .outputMode("append")
        .format("solr")
        .options(writeOptions)
        .start()
      try {
        logger.info(s"Explain ${streamingQuery.explain()}")
        streamingQuery.processAllAvailable()
        logger.info(s"Status ${streamingQuery.status}")
        SolrSupport.getCachedCloudClient(zkHost).commit(collectionName)
        assert(SolrQuerySupport.getNumDocsFromSolr(collectionName, zkHost, None) === 13)
        val acc = SparkInternalObjects.getAccumulatorById(SparkSolrAccumulatorContext.getId(accName).get)
        assert(acc.isDefined)
        assert(acc.get.value == 13)
      } finally {
        streamingQuery.stop()
      }
    } finally {
      SolrCloudUtil.deleteCollection(collectionName, cluster)
      FileUtils.deleteDirectory(new File(offsetsDir))
    }
  }
}
