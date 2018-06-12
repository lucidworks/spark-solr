package com.lucidworks.spark

import com.lucidworks.spark.util.SolrRelationUtil
import org.apache.solr.client.solrj.SolrQuery
import org.apache.spark.sql.sources.{And, EqualTo, Or}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

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

  test("Scala filter expressions") {
    val filterExpr = Or(And(EqualTo("gender", "F"), EqualTo("artist", "Bernadette Peters")),And(EqualTo("gender", "M"), EqualTo("artist", "Girl Talk")))
    val solrQuery = new SolrQuery
    val schema = StructType(Seq(StructField("gender", DataTypes.StringType), StructField("artist", DataTypes.StringType)))
    SolrRelationUtil.applyFilter(filterExpr, solrQuery, schema)
    val fq = solrQuery.getFilterQueries
    assert(fq.length == 1)
    assert(fq(0) === """((gender:"F" AND artist:"Bernadette Peters") OR (gender:"M" AND artist:"Girl Talk"))""")
  }

  test("custom field types option") {
    val fieldTypeOption = "a:b,c,d:e"
    val fieldTypes = SolrRelation.parseUserSuppliedFieldTypes(fieldTypeOption)
    assert(fieldTypes.size ===  2)
    assert(fieldTypes.keySet === Set("a", "d"))
    assert(fieldTypes("a") === "b")
    assert(fieldTypes("d") === "e")
  }
}
