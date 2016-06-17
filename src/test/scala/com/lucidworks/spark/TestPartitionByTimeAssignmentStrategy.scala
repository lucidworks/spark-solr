package com.lucidworks.spark

import java.util.UUID

import com.lucidworks.spark.util.{SolrCloudUtil, SolrSupport}
import org.apache.spark.sql.SaveMode._

/**
  * Created by akashmehta on 6/17/16.
  */
class TestPartitionByTimeAssignmentStrategy extends TestSuiteBuilder {


  test("Test partition selection for query") {
    val collection1Name = "testQuerying-" + "_2016_01_01"
    val collection2Name="testQuerying-" + "_2016_01_02"
    SolrCloudUtil.buildCollection(zkHost, collection1Name, null, 2, cloudClient, sc)
    SolrCloudUtil.buildCollection(zkHost, collection2Name, null, 2, cloudClient, sc)
    try {
      val jsonFileLocation = "src/test/resources/test-data/events.json"
      val jsonDF = sqlContext.read.json(jsonFileLocation)
      assert(jsonDF.count == 100)

      val solrOpts_writing1 = Map("zkhost" -> zkHost, "collection" -> collection1Name)
      val solrOpts_writing2 = Map("zkhost" -> zkHost, "collection" -> collection2Name)
      val solrOpts = Map("zkhost" -> zkHost, "collection" -> s"$collection1Name,$collection2Name")


      jsonDF.write.format("solr").options(solrOpts_writing1).mode(Overwrite).save()
      jsonDF.write.format("solr").options(solrOpts_writing2).mode(Overwrite).save()

      // Explicit commit to make sure all docs are visible
      val solrCloudClient = SolrSupport.getCachedCloudClient(zkHost)
      solrCloudClient.commit(collection1Name, true, true)
      solrCloudClient.commit(collection2Name, true, true)

      val solrDF = sqlContext.read.format("solr").options(solrOpts).load()
      assert(solrDF.count == 6)
      assert(solrDF.schema.fields.length === 6) // id one_txt two_txt three_s _version_ _indexed_at_tdt
      val oneColFirstRow = solrDF.select("one_txt").head()(0) // query for one column
      assert(oneColFirstRow != null)
      val firstRow = solrDF.head.toSeq                        // query for all columns
      assert(firstRow.size === 6)
      firstRow.foreach(col => assert(col != null))            // no missing values
    } finally {
      SolrCloudUtil.deleteCollection(collection1Name, cluster)
      SolrCloudUtil.deleteCollection(collection2Name, cluster)
    }
  }


}
