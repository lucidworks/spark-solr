package com.lucidworks.spark

import com.lucidworks.spark.util.{SolrCloudUtil, SolrSupport}
import org.apache.spark.sql.SaveMode._

/**
 This class is used to test the PartitionByTimeQuerySupport class
  */
class TestPartitionByTimeQuerySupport extends TestSuiteBuilder {


  test("Test partition selection for query") {
    val collection1Name = "test" + "_2014_11_24_17_30"
    val collection2Name="test" + "_2014_11_24_17_31"
    val collection3Name="test" + "_2014_11_24_17_33"
    val baseCollectionName="test"
    SolrCloudUtil.buildCollection(zkHost, collection1Name, null, 1, cloudClient, sc)
    SolrCloudUtil.buildCollection(zkHost, collection2Name, null, 1, cloudClient, sc)
    SolrCloudUtil.buildCollection(zkHost, collection3Name, null, 1, cloudClient, sc)
    try {
      val jsonFileLocation = "src/test/resources/test-data/events.json"
      val jsonDF = sparkSession.read.json(jsonFileLocation)
      assert(jsonDF.count == 100)

      var col1DF=jsonDF.filter(jsonDF("timestamp_tdt") >= "2014-11-24T17:30" && jsonDF("timestamp_tdt") < "2014-11-24T17:31")
      assert(col1DF.count == 32)
      col1DF=col1DF.drop(col1DF("_version_"))
      var col2DF=jsonDF.filter(jsonDF("timestamp_tdt") >= "2014-11-24T17:31" && jsonDF("timestamp_tdt") < "2014-11-24T17:32")
      assert(col2DF.count == 31)
      col2DF=col2DF.drop(col2DF("_version_"))
      var col3DF=jsonDF.filter(jsonDF("timestamp_tdt") >= "2014-11-24T17:33" && jsonDF("timestamp_tdt") < "2014-11-24T17:34")
      assert(col3DF.count == 37)
      col3DF=col3DF.drop(col3DF("_version_"))

      val solrOpts_writing1 = Map("zkhost" -> zkHost, "collection" -> collection1Name)
      val solrOpts_writing2 = Map("zkhost" -> zkHost, "collection" -> collection2Name)
      val solrOpts_writing3 = Map("zkhost" -> zkHost, "collection" -> collection3Name)

      col1DF.write.format("solr").options(solrOpts_writing1).mode(Overwrite).save()
      col2DF.write.format("solr").options(solrOpts_writing2).mode(Overwrite).save()
      col3DF.write.format("solr").options(solrOpts_writing3).mode(Overwrite).save()

      // Explicit commit to make sure all docs are visible
      val solrCloudClient = SolrSupport.getCachedCloudClient(zkHost)
      solrCloudClient.commit(collection1Name, true, true)
      solrCloudClient.commit(collection2Name, true, true)
      solrCloudClient.commit(collection3Name, true, true)

      // No query, return all the partitons
      var solrOpts = Map("zkhost" -> zkHost, "collection" -> baseCollectionName,"partition_by" -> "time","time_period" -> "1MINUTES")
      var solrDF = sparkSession.read.format("solr").options(solrOpts).load()
      assert(solrDF.count == 100)

      //query to select all partitions
      solrOpts = Map("zkhost" -> zkHost, "collection" -> baseCollectionName,"partition_by" -> "time","time_period" -> "1MINUTES","solr.params" -> "fq=timestamp_tdt:[* TO *]")
      solrDF = sparkSession.read.format("solr").options(solrOpts).load()
      assert(solrDF.count == 100)

      // querying a range
      solrOpts = Map("zkhost" -> zkHost, "collection" -> baseCollectionName,"partition_by" -> "time","time_period" -> "1MINUTES","filters" -> "timestamp_tdt:[2014-11-24T17:30:00Z TO 2014-11-24T17:32:00Z]")
      solrDF = sparkSession.read.format("solr").options(solrOpts).load()
      assert(solrDF.count == 63)


    } finally {
      SolrCloudUtil.deleteCollection(collection1Name, cluster)
      SolrCloudUtil.deleteCollection(collection2Name, cluster)
      SolrCloudUtil.deleteCollection(collection3Name, cluster)
    }
  }


}
