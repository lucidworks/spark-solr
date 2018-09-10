package com.lucidworks.spark

import com.lucidworks.spark.util.SolrRelationUtil
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.solr.client.solrj.SolrQuery
import org.apache.spark.sql.sources.{And, EqualTo, Or}
import org.apache.spark.sql.types._

class TestSolrRelation extends SparkSolrFunSuite with SparkSolrContextBuilder {

  test("Streaming expr schema") {
    val arrayJson = StringEscapeUtils.escapeJson(ArrayType(StringType).json)
    val longJson = StringEscapeUtils.escapeJson(LongType.json)
    val exprSchema = s"""arrayField:"${arrayJson}",longField:long"""
    val structFields = SolrRelation.parseSchemaExprSchemaToStructFields(exprSchema)
    assert(structFields.size == 2)
    logger.info(s"Parsed fields: ${structFields}")
    val arrayStructField = structFields.head
    val longStructField = structFields.last
    assert(arrayStructField.name === "arrayField")
    assert(arrayStructField.dataType === ArrayType(StringType))
    assert(arrayStructField.metadata.getBoolean("multiValued"))
    assert(longStructField.name  === "longField")
    assert(longStructField.dataType === LongType)
  }

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

  test("test commas in filter values") {
    val fieldValues = """a:"c,d e",f:g,h:"1, 35, 2""""
    val parsedFilters = SolrRelationUtil.parseCommaSeparatedValuesToList(fieldValues)
    assert(parsedFilters.head === """a:"c,d e"""")
    assert(parsedFilters(1) === "f:g")
    assert(parsedFilters(2) === """h:"1, 35, 2"""")
  }
}
