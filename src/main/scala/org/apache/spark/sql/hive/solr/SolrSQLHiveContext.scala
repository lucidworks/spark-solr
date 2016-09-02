package org.apache.spark.sql.hive.solr

import java.util.regex.{Matcher, Pattern}

import com.lucidworks.spark.query.sql.SolrSQLSupport
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.OverrideCatalog
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkContext}
import scala.collection.JavaConversions.mapAsScalaMap

class SolrSQLHiveContext(
    sparkContext: SparkContext,
    val config: Map[String, String],
    tablePermissionChecker: Option[TablePermissionChecker] = None)
  extends HiveContext(sparkContext) with Logging {

  val cachedSQLQueries: Map[String, String] = Map.empty

  @transient
  override protected[sql] lazy val catalog =
    new SolrHiveMetastoreCatalog(metadataHive, this, tablePermissionChecker) with OverrideCatalog

  override def sql(sqlText: String): DataFrame = {
    // process the statement and check for sub-queries
    val modifiedSqlText = SolrSQLHiveContext.processSqlStmt(sqlText, this)
    super.sql(modifiedSqlText)
  }

}

object SolrSQLHiveContext extends Logging {
  protected val solrSubQueryPattern: Pattern = Pattern.compile("\\((SELECT .*?\\)) as solr", Pattern.CASE_INSENSITIVE)

  def processSqlStmt(sqlText: String, sHiveContext: SolrSQLHiveContext): String =
    if (hasSolrAlias(sqlText)) processPushDownSql(sqlText, sHiveContext) else sqlText

  def hasSolrAlias(sqlText: String): Boolean =
    sqlText.toLowerCase.indexOf(") as solr") != -1

  def processPushDownSql(sqlText: String, sHiveContext: SolrSQLHiveContext): String = {
    val matcher: Matcher = solrSubQueryPattern.matcher(sqlText)
    if (!matcher.find()) return sqlText

    val subQueryMatch = matcher.group()
    // remove the ') as solr' part
    val solrQueryStmt = subQueryMatch.substring(1, subQueryMatch.length - 9).trim

    if (solrQueryStmt == null) return sqlText

    // Check if the query is cached and return the modified sql
    val cachedPushdownQuery = sHiveContext.cachedSQLQueries.get(solrQueryStmt.toLowerCase)
    if (cachedPushdownQuery.isDefined) {
      val replaceSql = matcher.replaceFirst(cachedPushdownQuery.get)
      logInfo("Found push-down in cache, re-wrote SQL statement as [" + replaceSql + "] to use cached push-down sub-query; original query: " + sqlText)
      return replaceSql
    }

    val cols = parseColumns(sqlText).getOrElse(return sqlText)
    log.debug("Parsed columns: " + cols)
    log.info("Attempting to push-down sub-query into Solr: " + solrQueryStmt)

    val tableName = findTableFromSql(solrQueryStmt.toLowerCase).getOrElse(return sqlText)
    logInfo("Extracted table name '" + tableName + "' from sub-query: " + solrQueryStmt)

    val md5 = DigestUtils.md5Hex(sqlText)
    val tempTableName = tableName + "_sspd_" + md5
    try {
      // load the push down query as a temp table
      registerSolrPushdownQuery(tempTableName, solrQueryStmt, tableName, sHiveContext)
      val newCachedPushdownQuery = "(SELECT * FROM " + tempTableName + ") as solr"
      sHiveContext.cachedSQLQueries + (solrQueryStmt.toLowerCase -> newCachedPushdownQuery)

      val rewrittenSql = matcher.replaceFirst(newCachedPushdownQuery)
      log.info("Re-wrote SQL statement as [" + rewrittenSql + "] to use cached push-down sub-query; original query: " + sqlText)
      return rewrittenSql
    } catch {
      case exc: Exception =>
        log.error("Failed to push-down sub-query [" + solrQueryStmt + "] to Solr due to: " + exc)
    }
    sqlText
  }

  def parseColumns(sqlText: String): Option[Map[String, String]] = {
    try {
      val cols = SolrSQLSupport.parseColumns(sqlText)
      if (cols.isEmpty) {
        log.info("No columns found for sub-query [" + sqlText + "], cannot push down into Solr")
        return None
      }
      Some(cols.toMap)
    } catch {
      case e: Exception =>
        log.warn("Failed to parse columns for sub-query [" + sqlText + "] due to: " + e)
        None
    }
  }

  def findTableFromSql(sqlText: String): Option[String] = {
    val fromAt = sqlText.indexOf("from ")
    if (fromAt != -1) {
      val partialTableStmt = sqlText.substring(fromAt + 5)
      val spaceAt = partialTableStmt.indexOf(" ")
      if (spaceAt != -1) Some(partialTableStmt.substring(0, spaceAt)) else Some(partialTableStmt)
    } else {
      logWarning("No push-down to Solr! Cannot determine table name from sql-query: " + sqlText)
      None
    }
  }

  def registerSolrPushdownQuery(
      tempTableName: String,
      sqlText: String,
      tableName: String,
      sHiveContext: SolrSQLHiveContext): Unit = {
    val zkhost = sHiveContext.config.getOrElse("zkhost", null)
    val opts = Map("request_handler" -> "/sql", "sql" -> sqlText, "collection" -> tableName, "zkhost" -> zkhost)
    logInfo("Registering temp table '" + tempTableName + "' for Solr push-down SQL using options " + opts)
    sHiveContext.read.format("solr").options(opts).load().registerTempTable(tempTableName)
  }
}
