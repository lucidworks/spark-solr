package com.lucidworks.spark.query.sql;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper for working with Solr SQL statements, such as parsing out the column list.
 */
public class SolrSQLSupport {

  private static final String SELECT = "select ";
  private static final String FROM = " from";
  private static final String AS = "as ";
  private static final String DISTINCT = "distinct ";

  /**
   * Given a valid Solr SQL statement, parse out the columns and aliases as a map.
   */
  public static Map<String,String> parseColumns(String sqlStmt) throws Exception {

    // NOTE: While I prefer using a SQL parser here, the presto / calcite / Spark parsers were too complex
    // for this basic task and pulled in unwanted / incompatible dependencies, e.g. presto requires a different
    // version of guava than what Spark supports

    String tmp = sqlStmt.replaceAll("\\s+", " ").trim();

    String lc = tmp.toLowerCase();
    if (!lc.startsWith(SELECT))
      throw new IllegalArgumentException("Expected SQL to start with '"+SELECT+"' but found ["+sqlStmt+"] instead!");

    int fromAt = lc.indexOf(FROM, SELECT.length());
    if (fromAt == -1)
      throw new IllegalArgumentException("No FROM keyword found in SQL: "+sqlStmt);

    String columnList = tmp.substring(SELECT.length(),fromAt).trim();

    // SELECT * not supported yet
    if ("*".equals(columnList))
      return Collections.emptyMap();

    Map<String,String> columns = new HashMap<>();
    for (String pair : columnList.split(",")) {
      pair = pair.trim();

      // trim off distinct indicator
      if (pair.toLowerCase().startsWith(DISTINCT)) {
        pair = pair.substring(DISTINCT.length());
      }

      String col;
      String alias;
      int spaceAt = pair.indexOf(" ");
      if (spaceAt != -1) {
        col = pair.substring(0,spaceAt);
        alias = pair.substring(spaceAt+1);
        if (alias.toLowerCase().startsWith(AS)) {
          alias = alias.substring(AS.length());
        }
      } else {
        col = pair;
        alias = pair;
      }

      columns.put(col.replace("`","").replace("'",""), alias.replace("`","").replace("'",""));
    }
    return columns;
  }
}
