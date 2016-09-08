package org.apache.spark.sql.hive.solr

import com.lucidworks.spark.MovielensBuilder


class TestSolrSQLHiveContext extends MovielensBuilder {

  var sHiveContext: SolrSQLHiveContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val opts = Map("zkhost" -> zkHost)
    sHiveContext = new SolrSQLHiveContext(sc, opts)
  }

  test("find table from SQL statement") {
    val sqlStmt = "select * from dummy where a=b"
    val tableName = SolrSQLHiveContext.findTableFromSql(sqlStmt)
    assert(tableName.isDefined)
    assert(tableName.get.equals("dummy"))
  }

  // end to end usecase testing
  test("Execute SQL query as Solr") {
    // Filter on movie_id = 9
    val solrSQLStmt = "SELECT movie_id, COUNT(*) as aggCount FROM movielens_ratings WHERE (rating >= 4 AND movie_id = 9) GROUP BY movie_id ORDER BY aggCount desc"
    val sqlStmt = "SELECT m.title as title, solr.aggCount as aggCount FROM movies m INNER JOIN (" + solrSQLStmt + ") as SOLR ON solr.movie_id = m.movie_id ORDER BY aggCount DESC"
    val tempTableName = SolrSQLHiveContext.getTempTableName(solrSQLStmt.toLowerCase, "movielens_ratings")

    // Non cached lookup
    {
      // Register movies collection as a table
      val opts = Map("zkhost" -> zkHost, "collection" -> moviesColName)
      sHiveContext.read.format("solr").options(opts).load().registerTempTable("movies")

      val df = sHiveContext.sql(sqlStmt)
      val rows = df.collect()
      assert(rows.length == 1)
      assert(rows(0)(1) == 23)

      // Check if the temp table is created

      val tables = sHiveContext.catalog.getTables(None)
      assert(tables.contains(tempTableName, true))

      // Check if the temp table is cached
      assert(sHiveContext.cachedSQLQueries.contains(solrSQLStmt.toLowerCase))

      // Check if the cached temp table is actually part of the physical plan
      assert(df.logicalPlan.toJSON.contains(tempTableName))
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
