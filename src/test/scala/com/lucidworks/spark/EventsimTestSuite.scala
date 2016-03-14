package com.lucidworks.spark

import com.lucidworks.spark.rdd.SolrRDD
import com.lucidworks.spark.util.ConfigurationConstants._
import org.apache.spark.sql.DataFrame

class EventsimTestSuite extends EventsimBuilder {

  test("Simple Query using RDD") {
    val solrRDD = new SolrRDD(zkHost, collectionName, sc)
      .query("*:*")
      .rows(10)
      .select(Array("id"))
    assert(solrRDD.getNumPartitions == numShards)
    testCommons(solrRDD)
  }

  test("Split partitions default") {
    val solrRDD = new SolrRDD(zkHost, collectionName, sc).doSplits()
    testCommons(solrRDD)
  }

  test("Split partitions by field name") {
    val solrRDD = new SolrRDD(zkHost, collectionName, sc).splitField("id").splitsPerShard(2)
    testCommons(solrRDD)
  }

  test("SQL fields option") {
    val df = sqlContext.read.format("solr")
      .option(SOLR_ZK_HOST_PARAM, zkHost)
      .option(SOLR_COLLECTION_PARAM, collectionName)
      .option(SOLR_FIELD_PARAM, "id, userId,")
      .option(SOLR_QUERY_PARAM, "userId:93")
      .load()
    val singleRow = df.take(1)(0)
    assert(singleRow.length == 2)
    assert(singleRow(df.schema.fieldIndex("userId")).toString.toInt == 93)
  }

  test("SQL query") {
    val df: DataFrame = sqlContext.read.format("solr").option("zkHost", zkHost).option("collection", collectionName).load()
    assert(df.count() == eventSimCount)
  }

  test("SQL query splits") {
    val options = Map(
      "zkHost" -> zkHost,
      "collection" -> collectionName,
      SOLR_DO_SPLITS -> "true"
    )
    val df: DataFrame = sqlContext.read.format("solr").options(options).load()
    assert(df.rdd.getNumPartitions > numShards)
  }

  test("SQL query no params should produce IllegalArgumentException") {
    intercept[IllegalArgumentException] {
      sqlContext.read.format("solr").load()
    }
  }

  test("SQL query no zkHost should produce IllegalArgumentException") {
    intercept[IllegalArgumentException] {
      sqlContext.read.format("solr").option("zkHost", zkHost).load()
    }
  }

  test("SQL query no collection should produce IllegalArgumentException") {
    intercept[IllegalArgumentException] {
      sqlContext.read.format("solr").option("collection", collectionName).load()
    }
  }

  def testCommons(solrRDD: SolrRDD): Unit = {
    val sparkCount = solrRDD.count()

    // assert counts
    assert(sparkCount == solrRDD.solrCount.toLong)
    assert(sparkCount == eventSimCount)
  }


}
