package com.lucidworks.spark

import com.lucidworks.spark.util.{QueryConstants, ConfigurationConstants}
import org.apache.spark.sql.types.DoubleType

class MovieLensTestSuite extends MovielensBuilder {

  test("Score column in SQL statement pushdown to Solr") {
    val sqlStmt = "SELECT movie_id,title,score from movielens_movies where _query_='title_txt_en:dog' order by score desc LIMIT 100"
    val opts = Map(
      "zkhost" -> zkHost,
      "collection" -> moviesColName,
      ConfigurationConstants.REQUEST_HANDLER -> QueryConstants.QT_SQL,
      ConfigurationConstants.SOLR_SQL_STMT -> sqlStmt)
    val df = sparkSession.read.format("solr").options(opts).load()

    val schema = df.schema
    assert (schema.fieldNames.contains("score"))
    assert (schema("score").dataType == DoubleType)
    val rows = df.take(10)
    assert(rows(0).length==3)
  }

  test("Provide SQL schema via config") {
    val sqlStmt = "SELECT movie_id,title,score from movielens_movies where _query_='title_txt_en:dog' order by score desc LIMIT 100"
    val sqlSchema = "movie_id:string,title:string,score:double"
    val opts = Map(
      "zkhost" -> zkHost,
      "collection" -> moviesColName,
      ConfigurationConstants.REQUEST_HANDLER -> QueryConstants.QT_SQL,
      ConfigurationConstants.SOLR_SQL_STMT -> sqlStmt,
      ConfigurationConstants.SOLR_SQL_SCHEMA -> sqlSchema)
    val df = sparkSession.read.format("solr").options(opts).load()

    val schema = df.schema
    assert (schema.fieldNames.contains("score"))
    assert (schema("score").dataType == DoubleType)
    val rows = df.take(10)
    assert(rows(0).length==3)
  }

}
