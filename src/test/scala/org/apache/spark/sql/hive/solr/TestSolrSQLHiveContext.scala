package org.apache.spark.sql.hive.solr

import com.lucidworks.spark.{MovielensBuilder, EventsimBuilder}

// Do all the unit testing
class TestSolrSQLHiveContext extends EventsimBuilder{

}

// end to end usecase testing
class TestSHiveContextEventsim extends MovielensBuilder {

  var sHiveContext: SolrSQLHiveContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val opts = Map("zkhost" -> zkHost)
    sHiveContext = new SolrSQLHiveContext(sc, opts)
  }

  test("Execute SQL query as Solr") {
    val sqlStmt = "SELECT m.title as title, solr.aggCount as aggCount FROM movies m INNER JOIN (SELECT movie_id, COUNT(*) as aggCount FROM movielens_ratings WHERE rating >= 4 GROUP BY movie_id ORDER BY aggCount desc LIMIT 10) as solr ON solr.movie_id = m.movie_id ORDER BY aggCount DESC"

    // Register movies collection as a table
    val opts = Map("zkhost" -> zkHost, "collection" -> moviesColName)
    sHiveContext.read.format("solr").options(opts).load().registerTempTable("movies")

    sHiveContext.sql(sqlStmt).show()
  }
}