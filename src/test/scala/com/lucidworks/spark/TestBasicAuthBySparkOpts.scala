package com.lucidworks.spark

import java.util.UUID

import com.lucidworks.spark.util.{SolrCloudUtil, SolrSupport}
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}


class TestBasicAuthBySparkOpts extends TestSuiteBuilder {
  val securityJson = "{\n\"authentication\":{ \n   \"blockUnknown\": true, \n   \"class\":\"solr.BasicAuthPlugin\",\n   \"credentials\":{\"solr\":\"IV0EHq1OnNrj6gvRCwvFwTrZ1+z1oBbnQdiVC3otuq0= Ndd7LKvVBAaZIF0QAVi1ekCfAJXr1GGfLtRUXhgrF8c=\"} \n},\n\"authorization\":{\n   \"class\":\"solr.RuleBasedAuthorizationPlugin\",\n   \"permissions\":[{\"name\":\"security-edit\",\n      \"role\":\"admin\"}], \n   \"user-role\":{\"solr\":\"admin\"} \n}}"

  test("auth by spark options"){
    val collectionName = "testBasicAuth-" + UUID.randomUUID().toString
    SolrCloudUtil.buildCollection(zkHost, collectionName, null, 1, cloudClient, sc)

    // Enable basic authentication
    val zk = new ZooKeeper(zkHost, 500000, new Watcher() {
      override def process(event: WatchedEvent): Unit = {}
    })
    val bytes: Array[Byte] = zk.getData("/security.json", false, null)
    zk.setData("/security.json", securityJson.getBytes, -(1))

    try {
      val csvDF = buildTestData()
      val solrOpts = Map("zkhost" -> zkHost,
        "httpBasicAuthUser"->"solr",
        "httpBasicAuthPassword"->"SolrRocks",
        "collection" -> collectionName)
      csvDF.write.format("solr").options(solrOpts).mode(Overwrite).save()

      // Explicit commit to make sure all docs are visible
      val solrCloudClient = SolrSupport.getCachedCloudClient(zkHost)
      solrCloudClient.commit(collectionName, true, true)

      val solrDF = sparkSession.read.format("solr").options(solrOpts).load()
      assert(solrDF.count == 3)

    }finally {
      zk.setData("/security.json", bytes, -(1))
      zk.close()
      SolrCloudUtil.deleteCollection(collectionName, cluster)
    }
  }

  def buildTestData() : DataFrame = {
    val testDataSchema : StructType = StructType(
      StructField("id", IntegerType, true) ::
        StructField("one_txt", StringType, false) ::
        StructField("two_txt", StringType, false) ::
        StructField("three_s", StringType, false) :: Nil)

    val rows = Seq(
      Row(1, "A", "B", "C"),
      Row(2, "C", "D", "E"),
      Row(3, "F", "G", "H")
    )

    val csvDF : DataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.makeRDD(rows, 1), testDataSchema)
    assert(csvDF.count == 3)
    return csvDF
  }

}
