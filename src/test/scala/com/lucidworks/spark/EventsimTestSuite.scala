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

  test("Test arbitrary params using the param string") {
    val options = Map(
      SOLR_ZK_HOST_PARAM -> zkHost,
      SOLR_COLLECTION_PARAM -> collectionName,
      ARBITRARY_PARAMS_STRING -> "fl=id,registration&fq=lastName:Powell&fq=artist:Interpol&defType=edismax&df=id"
    )
    val df = sqlContext.read.format("solr").options(options).load()
    val count = df.count()
    assert(count == 1)
    val singleRow = df.take(1)(0)
    assert(singleRow.length == 2)
  }

  test("SQL collect query with export handler") {
    val df: DataFrame = sqlContext.read.format("solr")
      .option("zkHost", zkHost)
      .option("collection", collectionName)
      .option(USE_EXPORT_HANDLER, "true")
      .option(ARBITRARY_PARAMS_STRING, "sort=userId desc")
      .load()

    // The below two options are not working with the streaming iterator right now due to parsing issues.
    //      .option(ARBITRARY_PARAMS_STRING, "fq=lastName:Powell&fq=artist:Interpol&defType=edismax&df=id&sort=userId desc")
    //      .option(ARBITRARY_PARAMS_STRING, "fq=lastName:Powell&fq=artist:Interpol&sort=userId desc")

    df.registerTempTable("events")

    val queryDF = sqlContext.sql("SELECT artist FROM events")
    //  queryDF.count() // count is not going to work with StreamIterator because Spark does not set 'fields' param for
    // methods that do not need a callback. Better to set fl in the arbitrary params string or 'fields' option
    val rows = queryDF.collect()
    assert(rows.length == eventSimCount)
  }

  test("SQL count query with export handler") {
    val df: DataFrame = sqlContext.read.format("solr")
      .option("zkHost", zkHost)
      .option("collection", collectionName)
      .option(USE_EXPORT_HANDLER, "true")
      .option(ARBITRARY_PARAMS_STRING, "fl=artist&sort=userId desc") // The test will fail without the fl param here
      .load()
    df.registerTempTable("events")

    val queryDF = sqlContext.sql("SELECT artist FROM events")
    queryDF.count()
    assert(queryDF.count() == eventSimCount)
  }

  test("Timestamp filter queries") {
    val df: DataFrame = sqlContext.read.format("solr")
      .option("zkHost", zkHost)
      .option("collection", collectionName)
      .load()
    df.registerTempTable("events")

    val timeQueryDF = sqlContext.sql("SELECT * from events WHERE `registration` = '2015-05-31 11:03:07Z'")
    assert(timeQueryDF.count() == 11)
  }

  test("Length range filter queries") {
    val df: DataFrame = sqlContext.read.format("solr")
      .option("zkHost", zkHost)
      .option("collection", collectionName)
      .load()
    df.registerTempTable("events")

    val timeQueryDF = sqlContext.sql("SELECT * from events WHERE `length` >= '700' and `length` <= '1000'")
    assert(timeQueryDF.count() == 1)
  }

  // Ignored since Spark is not passing timestamps filters to the buildScan method. Range timestamp filtering is being done at Spark layer
  ignore("Timestamp range filter queries") {
    val df: DataFrame = sqlContext.read.format("solr")
      .option("zkHost", zkHost)
      .option("collection", collectionName)
      .load()
    df.registerTempTable("events")

    val timeQueryDF = sqlContext.sql("SELECT * from events WHERE `registration` >= '2015-05-28' AND `registration` <= '2015-05-29' ")
    assert(timeQueryDF.count() == 21)
  }

  test("Streaming query with int field") {
    val df: DataFrame = sqlContext.read.format("solr")
    .option("zkHost", zkHost)
    .option("collection", collectionName)
    .option(USE_EXPORT_HANDLER, "true")
    .option(ARBITRARY_PARAMS_STRING, "fl=status,length&sort=userId desc") // The test will fail without the fl param here
    .load()
    df.registerTempTable("events")

    val queryDF = sqlContext.sql("SELECT count(distinct status), avg(length) FROM events")
    val values = queryDF.collect()
  }

  test("Non streaming query with int field") {
    val df: DataFrame = sqlContext.read.format("solr")
    .option("zkHost", zkHost)
    .option("collection", collectionName)
    .option(ARBITRARY_PARAMS_STRING, "fl=status,length&sort=id desc") // The test will fail without the fl param here
    .load()
    df.registerTempTable("events")

    val queryDF = sqlContext.sql("SELECT count(distinct status), avg(length) FROM events")
    val values = queryDF.collect()
    assert(values(0)(0) == 3)
  }

  def testCommons(solrRDD: SolrRDD): Unit = {
    val sparkCount = solrRDD.count()

    // assert counts
    assert(sparkCount == solrRDD.solrCount.toLong)
    assert(sparkCount == eventSimCount)
  }

}
