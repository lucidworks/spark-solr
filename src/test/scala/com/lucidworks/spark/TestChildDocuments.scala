package com.lucidworks.spark

import java.util.UUID

import com.lucidworks.spark.util.{SolrCloudUtil, SolrSupport}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode._

class TestChildDocuments extends TestSuiteBuilder {
  test("child document should have root as parent document's id") {
    val collectionName = "testChildDocuments-" + UUID.randomUUID().toString
    SolrCloudUtil.buildCollection(zkHost, collectionName, null, 1, cloudClient, sc)

    try {
      val testDf = buildTestDataFrame()
      val solrOpts = Map(
        "zkhost" -> zkHost,
        "collection" -> collectionName,
        "gen_uniq_key" -> "true",
        "gen_uniq_child_key" -> "true",
        "child_doc_fieldname" -> "tags",
        "flatten_multivalued" -> "false" // for correct reading column "date"
      )
      testDf.write.format("solr").options(solrOpts).mode(Overwrite).save()

      // Explicit commit to make sure all docs are visible
      val solrCloudClient = SolrSupport.getCachedCloudClient(zkHost)
      solrCloudClient.commit(collectionName, true, true)

      val solrDf = sparkSession.read.format("solr").options(solrOpts).load()
      solrDf.show()

      val userA = solrDf.filter(solrDf("user") === "a")
      val userB = solrDf.filter(solrDf("user") === "b")
      val childrenFromA = solrDf.filter(solrDf("parent") === "a")
      val childrenFromB = solrDf.filter(solrDf("parent") === "b")

      assert(userA.count == 1)
      assert(userB.count == 1)
      assert(childrenFromA.count == 2)
      assert(childrenFromB.count == 2)

      val idOfUserA = userA.select("id").rdd.map(r => r(0).asInstanceOf[String]).collect().head
      val idOfUserB = userB.select("id").rdd.map(r => r(0).asInstanceOf[String]).collect().head

      val rootsOfChildrenFromA = childrenFromA.select("_root_").rdd.map(r => r(0).asInstanceOf[String]).collect()
      val rootsOfChildrenFromB = childrenFromB.select("_root_").rdd.map(r => r(0).asInstanceOf[String]).collect()
      rootsOfChildrenFromA.foreach (root => assert(root == idOfUserA))
      rootsOfChildrenFromB.foreach (root => assert(root == idOfUserB))
    } finally {
      SolrCloudUtil.deleteCollection(collectionName, cluster)
    }
  }

  def buildTestDataFrame(): DataFrame = {
    val df = sparkSession.read.json("src/test/resources/test-data/child_documents.json")
    df.printSchema()
    df.show()
    assert(df.count == 2)
    return df
  }
}
