package com.lucidworks.spark

import com.lucidworks.spark.util.SolrQuerySupport
import org.apache.spark.sql.{DataFrame, Row}

class TestFacetQuerying extends MovielensBuilder {

  test("JSON Facet terms") {
    val facetQuery =
      """
        | {
        |   aggr_genre: {
        |     type: terms,
        |     field: genre,
        |     limit: 30,
        |     refine: true
        |   }
        | }
      """.stripMargin
    val queryString = s"q=*:*&json.facet=${facetQuery}"
    val dataFrame: DataFrame = SolrQuerySupport.getDataframeFromFacetQuery(SolrQuerySupport.toQuery(queryString), moviesColName, zkHost, sparkSession)
    assert(dataFrame.schema.fieldNames.length === 2)
    assert(dataFrame.schema.fieldNames.toSet === Set("aggr_genre", "aggr_genre_count"))

    val rows = dataFrame.collect()
    assert(rows(0) == Row("drama", 531))
    assert(rows(rows.length - 1) == Row("fantasy", 1))

    // dataFrame.printSchema()
    // dataFrame.show(20)
  }


  test("JSON facet nested top 2") {
    val facetQuery =
      """
        | {
        |   aggr_rating: {
        |     type: terms,
        |     field: "rating",
        |     limit: 30,
        |     refine: true,
        |     facet: {
        |       top_movies: {
        |         type: terms,
        |         field: movie_id,
        |         limit: 2
        |       }
        |     }
        |   }
        | }
      """.stripMargin
    val queryString = s"q=*:*&json.facet=${facetQuery}"
    val dataFrame: DataFrame = SolrQuerySupport.getDataframeFromFacetQuery(SolrQuerySupport.toQuery(queryString), ratingsColName, zkHost, sparkSession)
    assert(dataFrame.schema.fieldNames.length === 4)
    assert(dataFrame.schema.fieldNames.toSet === Set("aggr_rating", "aggr_rating_count", "top_movies", "top_movies_count"))

    val rows = dataFrame.collect()
    assert(rows(0) == Row(4, 3383, "9", 23))
    assert(rows(1) == Row(4, 3383, "237", 21))
    assert(rows(2) == Row(3, 2742, "294", 20))
    assert(rows(3) == Row(3, 2742, "405", 19))
    assert(rows(4) == Row(5, 2124, "50", 40))
    assert(rows(5) == Row(5, 2124, "56", 25))
    assert(rows(6) == Row(2, 1149, "118", 12))
    assert(rows(7) == Row(2, 1149, "678", 10))
    assert(rows(8) == Row(1, 602, "21", 5))
    assert(rows(9) == Row(1, 602, "225", 4))
//    dataFrame.printSchema()
//    dataFrame.show(20)
  }

  test("JSON facet aggrs") {
    val facetQuery =
      """
        | {
        |   "avg_rating" : "avg(rating)",
        |   "num_users" : "unique(user_id)",
        |   "no_of_movies_rated" : "unique(movie_id)",
        |   "median_rating" : "percentile(rating, 50)"
        | }
      """.stripMargin
    val queryString = s"q=*:*&json.facet=${facetQuery}"
    val dataFrame: DataFrame = SolrQuerySupport.getDataframeFromFacetQuery(SolrQuerySupport.toQuery(queryString), ratingsColName, zkHost, sparkSession)

    assert(dataFrame.schema.fieldNames.toSet === Set("count", "num_users", "avg_rating", "no_of_movies_rated", "median_rating"))
    val data = dataFrame.collect()

    assert(data(0) == Row(10000, 922, 4.0, 1238, 3.5278))
    //    dataFrame.printSchema()
    //    dataFrame.show()
  }


  test("Test cores") {
    val response = SolrQuerySupport.getSolrCores(cloudClient)
    assert(response.responseHeader.status == 0)
    assert(response.status.nonEmpty)
  }

}
