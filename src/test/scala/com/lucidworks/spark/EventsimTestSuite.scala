package com.lucidworks.spark

import java.util.Collections

import com.lucidworks.spark.rdd.SelectSolrRDD
import com.lucidworks.spark.util.ConfigurationConstants._
import com.lucidworks.spark.util.SolrDataFrameImplicits._
import com.lucidworks.spark.util.{QueryField, SolrRelationUtil}
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.SolrQuery.SortClause
import org.apache.spark.solr.SparkInternalObjects
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

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

  test("User requested fields with schema") {
    val df = sparkSession.read.format("solr")
      .option(SOLR_ZK_HOST_PARAM, zkHost)
      .option(SOLR_COLLECTION_PARAM, collectionName)
      .option(SOLR_FIELD_PARAM, "userId, ts")
      .option(SOLR_QUERY_PARAM, "userId:93")
      .option(SCHEMA, "userId:string,ts:timestamp")
      .load()
    val singleRow = df.take(1)(0)
    assert(singleRow.length == 2)
  }

  test("Sort by two fields") {
    val df = sparkSession.read.format("solr")
      .option(SOLR_ZK_HOST_PARAM, zkHost)
      .option(SOLR_COLLECTION_PARAM, collectionName)
      .option(SOLR_FIELD_PARAM, "userId, ts")
      .option(SOLR_QUERY_PARAM, "userId:93")
      .option(SORT_PARAM, "userId asc, ts asc")
      .load()
    val singleRow = df.take(1)(0)
    assert(singleRow.length == 2)
    assert(singleRow(df.schema.fieldIndex("userId")).toString.toInt == 93)
  }

  test("Alias and Sort by field") {
    val df = sparkSession.read.format("solr")
      .option(SOLR_ZK_HOST_PARAM, zkHost)
      .option(SOLR_COLLECTION_PARAM, collectionName)
      .option(SOLR_FIELD_PARAM, "user:userId,userId")
      .option(SCHEMA, """userId:\"string\"""")
      .load()
    df.printSchema()
    val singleRow = df.take(1)(0)
    assert(singleRow.length == 2)
  }

  test("Sort by score") {
    val df = sparkSession.read.format("solr")
      .option(SOLR_ZK_HOST_PARAM, zkHost)
      .option(SOLR_COLLECTION_PARAM, collectionName)
      .option(SOLR_FIELD_PARAM, "userId, ts")
      .option(SOLR_QUERY_PARAM, "userId:93")
      .option(SORT_PARAM, "score asc")
      .option(MAX_ROWS, "5")
      .load()
    val singleRow = df.take(1)(0)
    assert(singleRow.length == 2)
    assert(singleRow(df.schema.fieldIndex("userId")).toString.toInt == 93)
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

  test("count query with select") {
    val df: DataFrame = sparkSession.read.format("solr")
        .option("zkHost", zkHost)
        .option("collection", collectionName)
        .option("request_handler", "/select")
        .load()
    assert(df.count() == eventSimCount)
  }

  test("count query with custom accumulator name") {
    val acc_name = "custom_acc_name_records_read"
    val df: DataFrame = sparkSession.read.format("solr")
      .option("zkHost", zkHost)
      .option("collection", collectionName)
      .option(ACCUMULATOR_NAME, acc_name)
      .load()
    assert(df.count() == eventSimCount)
    val acc_id: Option[Long] = SparkSolrAccumulatorContext.getId(acc_name)
    assert(acc_id.isDefined)
    val acc = SparkInternalObjects.getAccumulatorById(acc_id.get)
    assert(acc.isDefined)
    assert(acc.get.value == eventSimCount)
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
      ARBITRARY_PARAMS_STRING -> "fq=artist:Interpol"
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

  test("Multiple WHERE clauses") {
    val df: DataFrame = sparkSession.read.option("zkhost", zkHost).solr(collectionName)
    df.createOrReplaceTempView("events")

    val timeQueryDF = sparkSession.sql("SELECT * from events WHERE `status` == 200 OR `status` == 404 or `status` == 300 or `status` == 400")
    assert(timeQueryDF.count() == 987)
  }

  test("Nested SQL Filter queries with OR/AND") {
    val df: DataFrame = sparkSession.read.option("zkhost", zkHost).solr(collectionName)
    df.createOrReplaceTempView("events")

    val sqlQuery =
      """
        | SELECT userId, sessionId, page, lastName, firstName, method, level, gender, artist
        |   FROM events
        | WHERE page IN ('NextSong')
        |       AND (
        |         (gender = 'F' AND artist = 'Bernadette Peters')
        |         OR
        |         (gender = 'M' AND artist = 'Girl Talk')
        |       )
      """.stripMargin
    val queryResults = sparkSession.sql(sqlQuery).collectAsList()
    assert(queryResults.size == 3)
  }

  test("Nested SQL Filter queries with And/OR") {
    val df: DataFrame = sparkSession.read.option("zkhost", zkHost).solr(collectionName)
    df.createOrReplaceTempView("events")

    val sqlQuery =
      """
        | SELECT userId, sessionId, page, lastName, firstName, method, level, gender, artist
        |   FROM events
        | WHERE page IN ('NextSong')
        |       AND (
        |         (method = 'PUT' OR method = 'GET')
        |         AND
        |         (artist = 'Gorillaz' OR artist = 'Girl Talk')
        |       )
      """.stripMargin
    val queryResults = sparkSession.sql(sqlQuery).collectAsList()
    assert(queryResults.size == 4)
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

  test("Test if * in field config works") {
    val options = Map(
      SOLR_ZK_HOST_PARAM -> zkHost,
      SOLR_COLLECTION_PARAM -> collectionName,
      SOLR_FIELD_PARAM -> "*"
    )
    val solrRelation = new SolrRelation(options, None, sparkSession)
    assert(solrRelation.conf.getFields.isEmpty)
    assert(solrRelation.initialQuery.getFields === null)
    assert(solrRelation.schema.nonEmpty)
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

  test("Test dynamic extensions") {
    val options = Map(
      SOLR_ZK_HOST_PARAM -> zkHost,
      SOLR_COLLECTION_PARAM -> collectionName
    )
    val solrRelation = new SolrRelation(options, None, sparkSession)
    val suffixes = solrRelation.dynamicSuffixes
    assert(suffixes.contains("s_"))
    assert(suffixes.contains("random_"))
    assert(suffixes.contains("_l"))
    assert(suffixes.contains("_dpf"))
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
