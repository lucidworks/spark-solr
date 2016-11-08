package com.lucidworks.spark

import java.util.Locale


class TestSolrSQLHiveContext extends MovielensBuilder {

  var sHiveContext: SolrSQLHiveContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val opts = Map("zkhost" -> zkHost)
    sHiveContext = new SolrSQLHiveContext(sc, opts)
  }

  test("find table from SQL statement") {
    val sqlStmt = "select * from dummy where a=b"
    val tableName = SolrSQLHiveContext.findSolrCollectionNameInSql(sqlStmt.replaceAll("\\s+"," "))
    assert(tableName.isDefined)
    assert(tableName.get.equals("dummy"))
  }

  // end to end usecase testing
  test("Execute SQL sub-query as Solr") {
    // Filter on movie_id = 9
    val solrSQLStmt = "SELECT movie_id, COUNT(*) as aggCount FROM movielens_ratings WHERE (rating >= 4 AND movie_id = 9) GROUP BY movie_id ORDER BY aggCount desc"
    val sqlStmt = "SELECT m.title as title, solr.aggCount as aggCount FROM movies m INNER JOIN (" + solrSQLStmt + ") as SOLR ON solr.movie_id = m.movie_id ORDER BY aggCount DESC"

    val cacheKey = "("+solrSQLStmt.toLowerCase(Locale.US)+") as solr"

    val tempTableName = SolrSQLHiveContext.getTempTableName(cacheKey, "movielens_ratings")

    // Non cached lookup
    {
      // Register movies collection as a table
      val opts = Map("zkhost" -> zkHost, "collection" -> moviesColName)
      sHiveContext.read.format("solr").options(opts).load().registerTempTable("movies")

      // verify the count(*) short circuit works
      val countStarDf = sHiveContext.sql("select count(*) from "+moviesColName)
      val countStarRows = countStarDf.collect()
      assert(countStarRows.length == 1)
      assert(countStarRows(0)(0) == 1682)

      val df = sHiveContext.sql(sqlStmt)
      val rows = df.collect()
      assert(rows.length == 1)
      assert(rows(0)(1) == 23)

      // Check if the temp table is created

      val tables = sHiveContext.tableNames()
      assert(tables.contains(tempTableName))

      // Check if the temp table is cached
      assert(sHiveContext.cachedSQLQueries.contains(cacheKey))

      // Check if the cached temp table is actually part of the physical plan
      // TODO: we don't want to have to be in the spark package
      //assert(df.logicalPlan.toJSON.contains(tempTableName))
    }

    // Cached lookup
    {
      val df = sHiveContext.sql(sqlStmt)
      val rows = df.collect()
      assert(rows.length == 1)
      assert(rows(0)(1) == 23)

      assert(sHiveContext.cachedSQLQueries.size == 1)
    }
  }
}
