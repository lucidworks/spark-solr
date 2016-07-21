package com.lucidworks.spark

import java.util.UUID

import com.lucidworks.spark.util.ConfigurationConstants._
import com.lucidworks.spark.util.SolrCloudUtil
import org.apache.solr.client.solrj.request.UpdateRequest
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.Logging
import org.apache.spark.sql.types.{TimestampType, StringType, LongType, DoubleType}

class RelationTestSuite extends TestSuiteBuilder with Logging {

  test("Unknown params") {
    val paramsToCheck = Set(SOLR_ZK_HOST_PARAM, SOLR_COLLECTION_PARAM, SOLR_QUERY_PARAM, ESCAPE_FIELDNAMES_PARAM, "fl", "q")
    val unknownParams = SolrRelation.checkUnknownParams(paramsToCheck)
    assert(unknownParams.size == 2)
    assert(unknownParams("q"))
    assert(unknownParams("fl"))
  }

  test("movielens data analytics") {

    // movies: id, movie_id, title
    val moviesCollection = "movies" + UUID.randomUUID().toString
    val numMovies = buildMoviesCollection(moviesCollection)

    var moviesDF = sqlContext.read.format("solr").options(
      Map("zkhost" -> zkHost, "collection" -> moviesCollection, "fields" -> "movie_id,title")).load
    assert(moviesDF.count == numMovies)
    var schema = moviesDF.schema
    assert(schema.fields.length == 2)
    assert(schema.fields(0).name == "movie_id")
    assert(schema.fields(0).dataType == StringType)
    assert(schema.fields(1).name == "title")
    assert(schema.fields(1).dataType == StringType)

    // movie ratings: id, user_id, movie_id, rating, rating_timestamp
    val ratingsCollection = "ratings" + UUID.randomUUID().toString
    val numRatings = buildRatingsCollection(ratingsCollection)

    var ratingsDF = sqlContext.read.format("solr").options(
      Map("zkhost" -> zkHost, "collection" -> ratingsCollection, "fields" -> "user_id,movie_id,rating,rating_timestamp")).load
    assert(ratingsDF.count == numRatings)
    schema = ratingsDF.schema
    assert(schema.fields.length == 4)
    assert(schema.fields(0).name == "user_id")
    assert(schema.fields(0).dataType == StringType)
    assert(schema.fields(1).name == "movie_id")
    assert(schema.fields(1).dataType == StringType)
    assert(schema.fields(2).name == "rating")
    assert(schema.fields(2).dataType == LongType)
    assert(schema.fields(3).name == "rating_timestamp")
    assert(schema.fields(3).dataType == TimestampType)

    // perform a facet streaming operation on the ratings collection to get avg rating for each movie
    val facetExpr = s"""
      |  facet(
      |    ${ratingsCollection},
      |    q="*:*",
      |    buckets="movie_id",
      |    bucketSorts="count(*) desc",
      |    bucketSizeLimit=100,
      |    count(*),
      |    avg(rating)
      |  )
    """.stripMargin
    var ratingFacetsDF = sqlContext.read.format("solr").options(
      Map("zkhost" -> zkHost, "collection" -> ratingsCollection, "expr" -> facetExpr)).load
    assert(ratingFacetsDF.count == 2)
    schema = ratingFacetsDF.schema
    assert(schema.fields.length == 3)
    assert(schema.fieldNames(0) == "avg(rating)")
    assert(schema.fields(0).dataType == DoubleType)
    assert(schema.fieldNames(1) == "count(*)")
    assert(schema.fields(1).dataType == LongType)
    assert(schema.fieldNames(2) == "movie_id")
    assert(schema.fields(2).dataType == StringType)

    val hashJoinExpr =
      s"""
         |parallel(${ratingsCollection},
         | hashJoin(
         |    search(${ratingsCollection},
         |           q="*:*",
         |           fl="movie_id,user_id,rating",
         |           sort="movie_id asc",
         |           qt="/export",
         |           partitionKeys="movie_id"),
         |    hashed=search(${moviesCollection},
         |                  q="*:*",
         |                  fl="movie_id,title",
         |                  sort="movie_id asc",
         |                  qt="/export",
         |                  partitionKeys="movie_id"),
         |    on="movie_id"
         |  ), workers="1", sort="movie_id asc")
       """.stripMargin
    var joinDF = sqlContext.read.format("solr").options(
      Map("zkhost" -> zkHost, "collection" -> ratingsCollection, "expr" -> hashJoinExpr)).load
    assert(joinDF.count == numRatings)
    joinDF.printSchema()
    schema = joinDF.schema
    assert(schema.fields.length == 4)
    assert(schema.fields(0).name == "movie_id")
    assert(schema.fields(0).dataType == StringType)
    assert(schema.fields(1).name == "rating")
    assert(schema.fields(1).dataType == LongType)
    assert(schema.fields(2).name == "title")
    assert(schema.fields(2).dataType == StringType)
    assert(schema.fields(3).name == "user_id")
    assert(schema.fields(3).dataType == StringType)

    // clean-up
    SolrCloudUtil.deleteCollection(ratingsCollection, cluster)
    SolrCloudUtil.deleteCollection(moviesCollection, cluster)
  }

  def buildMoviesCollection(moviesCollection: String) : Int = {
    SolrCloudUtil.buildCollection(zkHost, moviesCollection, null, 1, cloudClient, sc)

    val movieDocs : Array[String] = Array(
      UUID.randomUUID().toString+",movie200,The Big Short",
      UUID.randomUUID().toString+",movie201,Moneyball"
    )
    val indexMoviesRequest = new UpdateRequest()
    movieDocs.foreach(row => {
      val fields = row.split(",")
      val doc = new SolrInputDocument()
      doc.setField("id", fields(0))
      doc.setField("movie_id", fields(1))
      doc.setField("title", fields(2))
      indexMoviesRequest.add(doc)
      doc
    })
    indexMoviesRequest.process(cloudClient, moviesCollection)
    indexMoviesRequest.commit(cloudClient, moviesCollection)

    return movieDocs.length
  }

  def buildRatingsCollection(ratingsCollection: String) : Int = {
    SolrCloudUtil.buildCollection(zkHost, ratingsCollection, null, 1, cloudClient, sc)

    val ratingDocs : Array[String] = Array(
      UUID.randomUUID().toString+",user1,movie200,3,2016-01-01T00:00:01.999Z",
      UUID.randomUUID().toString+",user1,movie201,4,2016-01-02T00:00:02.999Z",
      UUID.randomUUID().toString+",user2,movie200,5,2016-01-03T00:00:03.999Z",
      UUID.randomUUID().toString+",user2,movie201,2,2016-01-04T00:00:04.999Z"
    )
    val indexRatingsRequest = new UpdateRequest()
    ratingDocs.foreach(row => {
      val fields = row.split(",")
      val doc = new SolrInputDocument()
      doc.setField("id", fields(0))
      doc.setField("user_id", fields(1))
      doc.setField("movie_id", fields(2))
      doc.setField("rating", fields(3).toInt)
      doc.setField("rating_timestamp", fields(4))
      indexRatingsRequest.add(doc)
      doc
    })
    indexRatingsRequest.process(cloudClient, ratingsCollection)
    indexRatingsRequest.commit(cloudClient, ratingsCollection)

    return ratingDocs.length
  }
}
