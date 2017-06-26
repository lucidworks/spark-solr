package com.lucidworks.spark

import java.util.Collections

import com.lucidworks.spark.rdd.SelectSolrRDD
import com.lucidworks.spark.util.ConfigurationConstants._
import com.lucidworks.spark.util.{QueryField, SolrRelationUtil}
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.SolrQuery.SortClause
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._
import com.lucidworks.spark.util.SolrDataFrameImplicits._

class EventsimTestSuite extends EventsimBuilder {

  test("Simple Query using RDD") {
    val solrRDD = new SelectSolrRDD(zkHost, collectionName, sc)
      .query("*:*")
      .rows(10)
      .select(Array("id"))
    assert(solrRDD.getNumPartitions == numShards*2)
    testCommons(solrRDD)
  }

  test("Split partitions default") {
    val solrRDD = new SelectSolrRDD(zkHost, collectionName, sc).doSplits()
    testCommons(solrRDD)
  }

  test("Split partitions by field name") {
    val solrRDD = new SelectSolrRDD(zkHost, collectionName, sc).splitField("id").splitsPerShard(2)
    testCommons(solrRDD)
  }

  test("SQL fields option") {
    val df = sparkSession.read.format("solr")
      .option(SOLR_ZK_HOST_PARAM, zkHost)
      .option(SOLR_COLLECTION_PARAM, collectionName)
      .option(SOLR_FIELD_PARAM, "userId, ts")
      .option(SOLR_QUERY_PARAM, "userId:93")
      .load()
    val singleRow = df.take(1)(0)
    assert(singleRow.length == 2)
    assert(singleRow(df.schema.fieldIndex("userId")).toString.toInt == 93)
  }

  test("Get Solr score as field") {
    val df = sparkSession.read.format("solr")
      .option(SOLR_ZK_HOST_PARAM, zkHost)
      .option(SOLR_COLLECTION_PARAM, collectionName)
      .option(SOLR_FIELD_PARAM, "id, userId, score")
      .option(SOLR_QUERY_PARAM, "userId:93")
      .load()
    val singleRow = df.take(1)(0)
    assert(singleRow.length == 3)
    assert(singleRow(df.schema.fieldIndex("userId")).toString.toInt == 93)
  }

  test("count query") {
    val df: DataFrame = sparkSession.read.format("solr").option("zkHost", zkHost).option("collection", collectionName).load()
    assert(df.count() == eventSimCount)
  }

  test("SQL query splits") {
    val options = Map(
      "zkHost" -> zkHost,
      "collection" -> collectionName,
      SOLR_DO_SPLITS -> "true"
    )
    val df: DataFrame = sparkSession.read.format("solr").options(options).load()
    assert(df.rdd.getNumPartitions > numShards)
    assert(df.rdd.getNumPartitions == 4)
  }

  test("SQL query splits with export handler") {
    val options = Map(
      "zkHost" -> zkHost,
      "collection" -> collectionName,
      SOLR_DO_SPLITS -> "true",
      SOLR_FIELD_PARAM -> "artist, auth, firstName, gender, id",
      SOLR_SPLITS_PER_SHARD_PARAM -> "5"
    )
    val df: DataFrame = sparkSession.read.format("solr").options(options).load()
    assert(df.rdd.getNumPartitions > numShards)
    assert(df.rdd.getNumPartitions == 10)
    val rows = df.collectAsList()
  }

  test("SQL query splits with export handler and no splits") {
    val options = Map(
      "zkHost" -> zkHost,
      "collection" -> collectionName,
      SOLR_DO_SPLITS -> "true",
      SOLR_FIELD_PARAM -> "artist, auth, firstName, gender, id",
      SOLR_SPLITS_PER_SHARD_PARAM -> "1"
    )
    val df: DataFrame = sparkSession.read.format("solr").options(options).load()
    assert(df.rdd.getNumPartitions == numShards)
    assert(df.rdd.getNumPartitions == 2)
    val rows = df.collectAsList()
  }

  test("SQL query no params should produce IllegalArgumentException") {
    intercept[IllegalArgumentException] {
      sparkSession.read.format("solr").load()
    }
  }

  test("SQL query no zkHost should produce IllegalArgumentException") {
    intercept[IllegalArgumentException] {
      sparkSession.read.format("solr").option("zkHost", zkHost).load()
    }
  }

  test("SQL query no collection should produce IllegalArgumentException") {
    intercept[IllegalArgumentException] {
      sparkSession.read.format("solr").option("collection", collectionName).load()
    }
  }

  test("Test arbitrary params using the param string") {
    val options = Map(
      SOLR_ZK_HOST_PARAM -> zkHost,
      SOLR_COLLECTION_PARAM -> collectionName,
      SOLR_FIELD_PARAM -> "id,registration",
      SOLR_FILTERS_PARAM -> "lastName:Powell",
      ARBITRARY_PARAMS_STRING -> "fq=artist:Interpol&defType=edismax&df=id"
    )
    val df = sparkSession.read.format("solr").options(options).load()
    val count = df.count()
    assert(count == 1)
    val singleRow = df.take(1)(0)
    assert(singleRow.length == 2)
  }

  test("SQL collect query with export handler") {
    val df: DataFrame = sparkSession.read.format("solr")
      .option("zkHost", zkHost)
      .option("collection", collectionName)
      .option(REQUEST_HANDLER, "/export")
      .option(ARBITRARY_PARAMS_STRING, "sort=userId desc")
      .load()

    df.createOrReplaceTempView("events")

    val queryDF = sparkSession.sql("SELECT artist FROM events")
    val count = queryDF.count()
    val rows = queryDF.collect()
    assert(rows.length == eventSimCount)
    assert(rows.length == count)
  }

  test("SQL count query with export handler") {
    val df: DataFrame = sparkSession.read.format("solr")
      .option("zkHost", zkHost)
      .option("collection", collectionName)
      .option(REQUEST_HANDLER, "/export")
      .option(ARBITRARY_PARAMS_STRING, "fl=artist&sort=userId desc") // The test will fail without the fl param here
      .load()
    df.createOrReplaceTempView("events")

    val queryDF = sparkSession.sql("SELECT artist FROM events")
    queryDF.count()
    assert(queryDF.count() == eventSimCount)
  }

  test("Timestamp filter queries") {
    val df: DataFrame = sparkSession.read.format("solr")
      .option("zkHost", zkHost)
      .option("collection", collectionName)
      .load()
    df.createOrReplaceTempView("events")

    val timeQueryDF = sparkSession.sql("SELECT * from events WHERE `registration` = '2015-05-31 11:03:07Z'")
    assert(timeQueryDF.count() == 11)
  }

  test("Length range filter queries") {
    val df: DataFrame = sparkSession.read.option("zkhost", zkHost).solr(collectionName)
    df.createOrReplaceTempView("events")

    val timeQueryDF = sparkSession.sql("SELECT * from events WHERE `length` >= '700' and `length` <= '1000'")
    assert(timeQueryDF.count() == 1)
  }

  // Ignored since Spark is not passing timestamps filters to the buildScan method. Range timestamp filtering is being done at Spark layer
  ignore("Timestamp range filter queries") {
    val df: DataFrame = sparkSession.read.format("solr")
      .option("zkHost", zkHost)
      .option("collection", collectionName)
      .load()
    df.createOrReplaceTempView("events")

    val timeQueryDF = sparkSession.sql("SELECT * from events WHERE `registration` >= '2015-05-28' AND `registration` <= '2015-05-29' ")
    assert(timeQueryDF.count() == 21)
  }

  test("Streaming query with int field") {
    val df: DataFrame = sparkSession.read.format("solr")
    .option("zkHost", zkHost)
    .option("collection", collectionName)
    .option(ARBITRARY_PARAMS_STRING, "sort=userId desc")
    .load()
    df.createOrReplaceTempView("events")

    val queryDF = sparkSession.sql("SELECT count(distinct status), avg(length) FROM events")
    val values = queryDF.collect()
  }

  test("Non streaming query with int field") {
    val df: DataFrame = sparkSession.read.format("solr")
    .option("zkHost", zkHost)
    .option("collection", collectionName)
    .option(ARBITRARY_PARAMS_STRING, "sort=id desc")
    .load()
    df.createOrReplaceTempView("events")

    val queryDF = sparkSession.sql("SELECT count(distinct status), avg(length) FROM events")
    val values = queryDF.collect()
    assert(values(0)(0) == 3)
  }

  test("Non streaming query with cursor marks option") {
    val df: DataFrame = sparkSession.read.format("solr")
      .option("zkHost", zkHost)
      .option("collection", collectionName)
      .option(USE_CURSOR_MARKS, "true")
      .load()
    df.createOrReplaceTempView("events")

    val queryDF = sparkSession.sql("SELECT count(distinct status), avg(length) FROM events")
    val values = queryDF.collect()
    assert(values(0)(0) == 3)
  }

  test("Test if auto check streaming feature works") {
    val options = Map(
      SOLR_ZK_HOST_PARAM -> zkHost,
      SOLR_COLLECTION_PARAM -> collectionName
    )
    val solrRelation = new SolrRelation(options, None, sparkSession)
    solrRelation.querySchema // Invoking querySchema builds the baseSchema
    val querySchema = SolrRelationUtil.deriveQuerySchema(Array("userId", "status", "artist", "song", "length").map(QueryField(_)), solrRelation.baseSchema.get)
    val areFieldsDocValues = SolrRelation.checkQueryFieldsForDV(querySchema)
    assert(areFieldsDocValues)

    solrRelation.initialQuery.addSort("registration", SolrQuery.ORDER.asc)
    val sortClauses = solrRelation.initialQuery.getSorts.asScala.toList
    val isSortFieldDocValue = SolrRelation.checkSortFieldsForDV(solrRelation.baseSchema.get, sortClauses)
    assert(isSortFieldDocValue)

    solrRelation.initialQuery.setSorts(Collections.emptyList())
    SolrRelation.addSortField(solrRelation.baseSchema.get, querySchema, solrRelation.initialQuery, solrRelation.uniqueKey)
    assert(solrRelation.initialQuery.getSorts == Collections.singletonList(new SortClause(solrRelation.uniqueKey, SolrQuery.ORDER.asc)))
  }

  test("Get documents with max_rows config") {
    val df: DataFrame = sparkSession.read.format("solr")
      .option("zkHost", zkHost)
      .option("collection", collectionName)
      .option(SOLR_FIELD_PARAM, "artist,firstName")
      .option(MAX_ROWS, 100)
      .load()
    val docs = df.collect()
    assert(docs.length == 100)
  }

  test("Multiple queries with same Dataframe to test request handler switch") {
    val options = Map(
      SOLR_ZK_HOST_PARAM -> zkHost,
      SOLR_COLLECTION_PARAM -> collectionName
    )
    val df = sparkSession.read.format("solr").options(options).load()
    df.take(1) // This should use the '/select' handler because of the field 'artist_txt'

    df.select("artist").take(1) // This should use '/export' handler because we are only requesting the field 'artist'
  }

  def testCommons(solrRDD: SelectSolrRDD): Unit = {
    val sparkCount = solrRDD.count()

    // assert counts
    assert(sparkCount == solrRDD.solrCount.toLong)
    assert(sparkCount == eventSimCount)
  }

}
