package org.apache.spark.sql.hive.solr

import com.lucidworks.spark.TestSuiteBuilder
import com.lucidworks.spark.util.SolrCloudUtil
import org.apache.spark.sql.DataFrame

class TestSolrSQLPermissions extends TestSuiteBuilder {
  var sHiveContext: SolrSQLHiveContext = _
  val collectionName = "testSolrSQLPermissions"

  override def beforeAll(): Unit = {
    super.beforeAll()
    val opts = Map("zkhost" -> zkHost, "collection" -> collectionName)
    SolrCloudUtil.buildCollection(zkHost, collectionName, null, 1, cloudClient, sc)
    sHiveContext = new SolrSQLHiveContext(sc, opts, Some(new TestTablePermissionChecker))
  }

  override def afterAll(): Unit = {
    SolrCloudUtil.deleteCollection(collectionName, cluster)
    super.afterAll()
  }

  test("Check query access") {
    val ex = intercept[RuntimeException] {
      sHiveContext.read.format("solr").options(sHiveContext.config).load()
    }
    assert(ex.getCause.isInstanceOf[NoQueryAccessTableException])
  }

  test("Check write access") {
    val ex = intercept[RuntimeException] {
      val datasetPath : String = "src/test/resources/eventsim/sample_eventsim_1000.json"
      val df : DataFrame = sHiveContext.read.json(datasetPath)
      df.write.format("solr").options(sHiveContext.config).save()
    }
    assert(ex.getCause.isInstanceOf[NoWriteAccessTableException])
  }
}

class TestTablePermissionChecker extends TablePermissionChecker {
  override def checkQueryAccess(collection: String): Unit = {
    if (collection.equals("testSolrSQLPermissions"))
      throw new NoQueryAccessTableException
  }

  override def checkWriteAccess(collection: String): Unit = {
    if (collection.equals("testSolrSQLPermissions"))
      throw new NoWriteAccessTableException
  }
}

class NoQueryAccessTableException extends Exception

class NoWriteAccessTableException extends Exception