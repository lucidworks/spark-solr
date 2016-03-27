package com.lucidworks.spark

import com.lucidworks.spark.util.ConfigurationConstants._

class RelationTestSuite extends SparkSolrFunSuite {

  test("Unknown params") {
    val paramsToCheck = Set(SOLR_ZK_HOST_PARAM, SOLR_COLLECTION_PARAM, SOLR_QUERY_PARAM, ESCAPE_FIELDNAMES_PARAM, "fl", "q")
    val unknownParams = SolrRelation.checkUnknownParams(paramsToCheck)
    assert(unknownParams.size == 2)
    assert(unknownParams("q"))
    assert(unknownParams("fl"))
  }

}
