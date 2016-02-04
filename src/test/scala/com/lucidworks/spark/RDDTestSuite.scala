package com.lucidworks.spark


class RDDTestSuite extends SparkSolrFunSuite with SparkSolrContextBuilder {

  test("Test RDD") {
    val zkHost = cluster.getZkServer.getZkAddress
    val collectioName = "test_new_rdd"
    deleteCollection(collectioName)
    buildCollection(zkHost, collectioName)
    val newRDD = new SolrScalaRDD(zkHost, collectioName, sc)
                                .query("*:*")
    assert(newRDD.count() === 3)
    deleteCollection(collectioName)
  }
}
