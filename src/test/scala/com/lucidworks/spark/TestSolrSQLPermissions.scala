package com.lucidworks.spark

import com.lucidworks.spark.util.SolrCloudUtil
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.solr.{SolrSparkSession, SecuredResource, TablePermissionChecker}

class TestSolrSQLPermissions extends TestSuiteBuilder {
  var sHiveContext: SolrSparkSession = _
  val collectionName = "testSolrSQLPermissions"

  override def beforeAll(): Unit = {
    super.beforeAll()
    val opts = Map("zkhost" -> zkHost, "collection" -> collectionName)
    SolrCloudUtil.buildCollection(zkHost, collectionName, null, 1, cloudClient, sc)
    sHiveContext = new SolrSparkSession(sc, opts, Some(new TestTablePermissionChecker))
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
  override def checkQueryAccess(sparkSession: SolrSparkSession, ugi: UserGroupInformation, securedResource: SecuredResource): Unit = {
    if (securedResource.resource == "testSolrSQLPermissions")
      throw new NoQueryAccessTableException
  }

  override def checkWriteAccess(sparkSession: SolrSparkSession, ugi: UserGroupInformation, securedResource: SecuredResource): Unit = {
    if (securedResource.resource == "testSolrSQLPermissions")
      throw new NoWriteAccessTableException
  }
}

class NoQueryAccessTableException extends Exception

class NoWriteAccessTableException extends Exception
