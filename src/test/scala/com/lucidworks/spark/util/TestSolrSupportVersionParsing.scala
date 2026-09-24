package com.lucidworks.spark.util

import java.util.LinkedHashMap

import com.lucidworks.spark.SparkSolrFunSuite
import org.apache.solr.common.util.SimpleOrderedMap

/**
 * Unit tests for SolrSupport.extractSolrSpecVersion, which must handle both response shapes
 * that /admin/info/system's "lucene" entry can come back as: a SimpleOrderedMap (a NamedList,
 * returned by Solr <= 10.0.x) and a plain java.util.LinkedHashMap (returned starting with Solr
 * 10.1 after SOLR-16458 migrated the endpoint to a JAX-RS/typed response). Neither shape needs a
 * live Solr cluster to construct, so this runs as a fast, non-integration unit test.
 */
class TestSolrSupportVersionParsing extends SparkSolrFunSuite {

  test("extractSolrSpecVersion reads a SimpleOrderedMap (Solr <= 10.0.x response shape)") {
    val lucene = new SimpleOrderedMap[Object]()
    lucene.add("solr-spec-version", "10.0.0")
    assert(SolrSupport.extractSolrSpecVersion(lucene) == "10.0.0")
  }

  test("extractSolrSpecVersion reads a java.util.LinkedHashMap (Solr 10.1+ response shape)") {
    val lucene = new LinkedHashMap[String, Object]()
    lucene.put("solr-spec-version", "10.1.0")
    assert(SolrSupport.extractSolrSpecVersion(lucene) == "10.1.0")
  }

  test("extractSolrSpecVersion fails loudly on an unexpected shape") {
    intercept[IllegalStateException] {
      SolrSupport.extractSolrSpecVersion("not-a-map-or-namedlist")
    }
  }
}
