package com.lucidworks.spark

class TestSolrRelation extends SparkSolrFunSuite with SparkSolrContextBuilder {

  test("empty solr relation") {
    intercept[IllegalArgumentException] {
      new SolrRelation(Map.empty, None, sparkSession)
    }
  }

  test("Missing collection property") {
    intercept[IllegalArgumentException] {
      new SolrRelation(Map("zkhost" -> "localhost:121"), None, sparkSession).collection
    }
  }

  test("relation object creation") {
    val options = Map("zkhost" -> "dummy:9983", "collection" -> "test")
    val relation = new SolrRelation(options, None, sparkSession)
    assert(relation != null)
  }
}
