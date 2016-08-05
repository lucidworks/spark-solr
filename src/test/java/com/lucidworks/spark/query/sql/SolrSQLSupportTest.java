package com.lucidworks.spark.query.sql;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SolrSQLSupportTest {
  @Test
  public void testSQLParse() throws Exception {
    String sqlStmt = "SELECT DISTINCT movie_id, COUNT(*) as agg_count, avg(rating) as avg_rating, sum(rating) as sum_rating, min(rating) as min_rating, max(rating) as max_rating " +
            "FROM ratings GROUP BY movie_id ORDER BY movie_id asc";
    Map<String,String> cols = SolrSQLSupport.parseColumns(sqlStmt);
    assertNotNull(cols);
    assertTrue(cols.size() == 6);
    assertEquals("agg_count", cols.get("COUNT(*)"));
    assertEquals("movie_id", cols.get("movie_id"));
    assertEquals("avg_rating", cols.get("avg(rating)"));
    assertEquals("sum_rating", cols.get("sum(rating)"));
    assertEquals("min_rating", cols.get("min(rating)"));
    assertEquals("max_rating", cols.get("max(rating)"));

    String selectStar = "SELECT * FROM ratings";
    cols = SolrSQLSupport.parseColumns(selectStar);
    assertNotNull(cols);
    assertTrue(cols.size() == 0);
  }
}
