package org.apache.spark.sql.hive.solr

import java.util.regex.{Matcher, Pattern}

import com.lucidworks.spark.query.sql.SolrSQLSupport
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkContext}

import scala.collection.JavaConversions.mapAsScalaMap

class SolrSQLHiveContext(
    sparkContext: SparkContext,
    val config: Map[String, String],
    val tablePermissionChecker: Option[TablePermissionChecker] = None)
  extends HiveContext(sparkContext) with Logging {

  var cachedSQLQueries: Map[String, String] = Map.empty
  protected val solrSubQueryPattern: Pattern = Pattern.compile(SolrSQLHiveContext.subQueryRegex, Pattern.CASE_INSENSITIVE)

  override def sql(sqlText: String): DataFrame = {
    // process the statement and check for sub-queries
    val modifiedSqlText = processSqlStmt(sqlText)
    super.sql(modifiedSqlText)
  }

  def processSqlStmt(sqlText: String) : String =
    if (SolrSQLHiveContext.hasSolrAlias(sqlText)) processPushDownSql(sqlText) else sqlText

  def processPushDownSql(sqlText: String): String = {
    val matcher: Matcher = solrSubQueryPattern.matcher(sqlText)
    if (!matcher.find()) return sqlText

    val subQueryMatch = matcher.group()

    // remove the ') as solr' part
    val solrQueryStmt = subQueryMatch.substring(1, subQueryMatch.length - 9).trim
    if (solrQueryStmt == null) return sqlText
    val solrQueryStmtLowerCase = solrQueryStmt.toLowerCase

    // Check if the query is cached and return the modified sql
    val cachedPushdownQuery = cachedSQLQueries.get(solrQueryStmtLowerCase)
    if (cachedPushdownQuery.isDefined) {
      val replaceSql = matcher.replaceFirst(cachedPushdownQuery.get)
      logInfo("Found push-down in cache, re-wrote SQL statement as [" + replaceSql + "] to use cached push-down sub-query; original query: " + sqlText)
      return replaceSql
    }

    // Determine if the columns in the query are compatible with Solr SQL
    val cols = SolrSQLHiveContext.parseColumns(sqlText).getOrElse(return sqlText)
    logDebug("Parsed columns: " + cols)
    logInfo("Attempting to push-down sub-query into Solr: " + solrQueryStmt)

    val tableName = SolrSQLHiveContext.findTableFromSql(solrQueryStmtLowerCase).getOrElse(return sqlText)
    logInfo("Extracted table name '" + tableName + "' from sub-query: " + solrQueryStmt)
    val tempTableName = SolrSQLHiveContext.getTempTableName(solrQueryStmtLowerCase, tableName)

    try {
      // load the push down query as a temp table
      registerSolrPushdownQuery(tempTableName, solrQueryStmt, tableName)
      val newCachedPushdownQuery = "(SELECT * FROM " + tempTableName + ") as solr"
      cachedSQLQueries += (solrQueryStmtLowerCase -> newCachedPushdownQuery)

      val rewrittenSql = matcher.replaceFirst(newCachedPushdownQuery)
      logInfo("Re-wrote SQL statement as [" + rewrittenSql + "] to use cached push-down sub-query; original query: " + sqlText)
      return rewrittenSql
    } catch {
      case exc: Exception =>
        logError("Failed to push-down sub-query [" + solrQueryStmt + "] to Solr due to: " + exc)
    }

    sqlText
  }

  def registerSolrPushdownQuery(
    tempTableName: String,
    sqlText: String,
    tableName: String): Unit = {
    var opts = Map("request_handler" -> "/sql", "sql" -> sqlText, "collection" -> tableName)
    val zkhost = config.get("zkhost")
    if (zkhost.isDefined) opts += ("zkhost" -> zkhost.get)
    logInfo("Registering temp table '" + tempTableName + "' for Solr push-down SQL using options " + opts)
    this.read.format("solr").options(opts).load().registerTempTable(tempTableName)
  }
}

object SolrSQLHiveContext extends Logging {
  val subQueryRegex= "\\((SELECT .*?\\)) as solr"

  def hasSolrAlias(sqlText: String): Boolean =
    sqlText.toLowerCase.indexOf(") as solr") != -1

  def parseColumns(sqlText: String): Option[Map[String, String]] = {
    try {
      val cols = SolrSQLSupport.parseColumns(sqlText)
      if (cols.isEmpty) {
        logInfo("No columns found for sub-query [" + sqlText + "], cannot push down into Solr")
        return None
      }
      Some(cols.toMap)
    } catch {
      case e: Exception =>
        logWarning("Failed to parse columns for sub-query [" + sqlText + "] due to: " + e)
        None
    }
  }

  def findTableFromSql(sqlText: String): Option[String] = {
    val fromAt = sqlText.toLowerCase.indexOf("from ")
    if (fromAt != -1) {
      val partialTableStmt = sqlText.substring(fromAt + 5)
      val spaceAt = partialTableStmt.indexOf(" ")
      if (spaceAt != -1) Some(partialTableStmt.substring(0, spaceAt)) else Some(partialTableStmt)
    } else {
      logWarning("No push-down to Solr! Cannot determine table name from sql-query: " + sqlText)
      None
    }
  }

  def getTempTableName(sqlText: String, tableName: String): String = {
    val md5 = DigestUtils.md5Hex(sqlText)
    tableName + "_sspd_" + md5
  }

}

trait TablePermissionChecker {

  def checkQueryAccess(collection: String): Unit

  def checkWriteAccess(collection: String): Unit
}
