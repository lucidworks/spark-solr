package org.apache.spark.sql.solr

import java.security.AccessControlException
import java.util.Locale
import java.util.regex.{Matcher, Pattern}

import com.lucidworks.spark.query.sql.SolrSQLSupport
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.CatalogImpl
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions.mapAsScalaMap

case class SecuredResource(resource:String, resourceType:String)

class SolrSparkSession(
    sparkContext: SparkContext,
    val config: Map[String, String],
    val tablePermissionChecker: Option[TablePermissionChecker] = None)
  extends SparkSession(sparkContext) with LazyLogging {

  var cachedSQLQueries: Map[String, String] = Map.empty
  var tableToResource : Map[String, SecuredResource] = Map.empty

  override lazy val catalog = new SolrSessionCatalog(this)
  override def sql(sqlText: String): DataFrame = {
    // process the statement and check for sub-queries
    val modifiedSqlText = processSqlStmt(sqlText)
    super.sql(modifiedSqlText)
  }

  def clearQueryCache() : Unit = {
    cachedSQLQueries = Map.empty
  }

  def checkReadAccess(resource: String, resourceType: String) : Unit = {
    if (tablePermissionChecker.isEmpty) {
      return
    }

    val ugi : UserGroupInformation = UserGroupInformation.getCurrentUser()
    if (ugi == null) {
      throw new AccessControlException("No UserGroupInformation found for current user verify access!")
    }

    val cacheKey = resourceType + "." + resource
    val securedResource = tableToResource.getOrElse(cacheKey, {new SecuredResource(resource, resourceType)})
    tablePermissionChecker.get.checkQueryAccess(this, ugi, securedResource)
    logInfo(s"User ${ugi.getUserName} has read access to ${resourceType} ${resource}")
  }

  def checkWriteAccess(resource: String, resourceType: String) : Unit = {
    if (tablePermissionChecker.isEmpty) {
      return
    }

    val ugi : UserGroupInformation = UserGroupInformation.getCurrentUser()
    if (ugi == null) {
      throw new AccessControlException("No UserGroupInformation found for current user verify access!")
    }

    val cacheKey = resourceType + "." + resource
    val securedResource = tableToResource.getOrElse(cacheKey, {new SecuredResource(resource, resourceType)})
    tablePermissionChecker.get.checkWriteAccess(this, ugi, securedResource)
    logInfo(s"User ${ugi.getUserName} has write access to ${resourceType} ${resource}")
  }

  def processSqlStmt(sqlText: String) : String = {
    val matcher: Matcher = SolrSparkSession.solrSubQueryPattern.matcher(sqlText)
    if (matcher.find()) {
      return processPushDownSql(sqlText, matcher)
    } else {
      val maybeSolrCollectionId : Option[String] = isSolrQuery(sqlText)
      if (maybeSolrCollectionId.isDefined) {
        val collectionId = maybeSolrCollectionId.get
        // first, verify access
        checkReadAccess(collectionId, "solr")

        val cacheKey = sqlText.toLowerCase(Locale.US)
        val maybeCachedSqlQuery = cachedSQLQueries.get(cacheKey)
        if (maybeCachedSqlQuery.isDefined) {
          return maybeCachedSqlQuery.get
        }

        val tempTableName = SolrSparkSession.getTempTableName(sqlText.toLowerCase(Locale.US), collectionId)
        logInfo(s"Sending Solr SQL query [${sqlText}}] directly to Solr collection ${collectionId} and saving result into temp table ${tempTableName}")
        // SQL query should be routed directly to Solr and by-pass SparkSQL query parsing
        val solrSqlOpts = Map("collection" -> collectionId, "sql" -> sqlText)
        val solrSqlDF = this.read.format("solr").options(solrSqlOpts).load()
        solrSqlDF.registerTempTable(tempTableName)
        val selectAllFromTemp = "select * from "+tempTableName
        cachedSQLQueries += (cacheKey -> selectAllFromTemp)
        return selectAllFromTemp
      }
    }

    val tableNameMatcher : Matcher = SolrSparkSession.solrCollectionInSqlPattern.matcher(sqlText)
    while (tableNameMatcher.find()) {
      checkReadAccess(tableNameMatcher.group(1), "table")
    }

    sqlText
  }

  def processPushDownSql(sqlText: String, matcher: Matcher): String = {

    val subQueryMatch = matcher.group()

    // remove the ') as solr' part
    val solrQueryStmt = subQueryMatch.substring(1, subQueryMatch.length - 9).trim
    if (solrQueryStmt.isEmpty) return sqlText

    // Check if the query is cached and return the modified sql
    val cacheKey = subQueryMatch.toLowerCase(Locale.US) // cacheKey must contain () as solr part
    val cachedPushdownQuery = cachedSQLQueries.get(cacheKey)
    if (cachedPushdownQuery.isDefined) {
      val replaceSql = matcher.replaceFirst(cachedPushdownQuery.get)
      logInfo(s"Found push-down in cache, re-wrote SQL statement as [${replaceSql}] to use cached push-down sub-query; original query: ${sqlText}")
      return replaceSql
    }

    // Determine if the columns in the query are compatible with Solr SQL
    val cols = SolrSparkSession.parseColumns(sqlText).getOrElse(return sqlText)
    logDebug("Parsed columns: " + cols)
    logInfo(s"Attempting to push-down sub-query into Solr: ${solrQueryStmt}")

    val tableName = SolrSparkSession.findSolrCollectionNameInSql(solrQueryStmt.replaceAll("\\s+"," ")).getOrElse(return sqlText)

    checkReadAccess(tableName, "solr")

    logInfo(s"Extracted table name '${tableName}' from sub-query: ${solrQueryStmt}")
    val tempTableName = SolrSparkSession.getTempTableName(cacheKey, tableName)
    try {
      // load the push down query as a temp table
      registerSolrPushdownQuery(tempTableName, solrQueryStmt, tableName)
      val newCachedPushdownQuery = "(SELECT * FROM " + tempTableName + ") as solr"
      cachedSQLQueries += (cacheKey -> newCachedPushdownQuery)

      val rewrittenSql = matcher.replaceFirst(newCachedPushdownQuery)
      logInfo(s"Re-wrote SQL statement as [${rewrittenSql}] to use cached push-down sub-query; original query: ${sqlText}")
      return rewrittenSql
    } catch {
      case exc: Exception =>
        logError("Failed to push-down sub-query [" + solrQueryStmt + "] to Solr due to: " + exc)
    }

    sqlText
  }

  def isSolrQuery(sqlText: String) : Option[String] = {
    val sqlTextWs = sqlText.replaceAll("\\s+"," ")
    val solrQueryMatcher : Matcher = SolrSparkSession.solrQueryPattern.matcher(sqlTextWs)
    if (!solrQueryMatcher.find()) return None

    SolrSparkSession.findSolrCollectionNameInSql(sqlTextWs)
  }

  def registerSolrPushdownQuery(
    tempTableName: String,
    sqlText: String,
    tableName: String): Unit = {
    var opts = Map("request_handler" -> "/sql", "sql" -> sqlText, "collection" -> tableName)
    val zkhost = config.get("zkhost")
    if (zkhost.isDefined) opts += ("zkhost" -> zkhost.get)
    logInfo(s"Registering temp table '${tempTableName}' for Solr push-down SQL using options ${opts}")
    this.read.format("solr").options(opts).load().registerTempTable(tempTableName)
  }
}

object SolrSparkSession extends LazyLogging {
  val solrSubQueryPattern: Pattern = Pattern.compile("\\((SELECT .*?\\)) as solr", Pattern.CASE_INSENSITIVE)
  // also supports executing Solr queries directly if they use _query_ in the where clause, one of our few hints we can rely on
  val solrQueryPattern: Pattern = Pattern.compile("\\s_query_\\s?=\\s?'.*?'\\s?")
  val solrCollectionInSqlPattern = Pattern.compile("\\sfrom\\s([\\w\\-\\.]+)\\s?", Pattern.CASE_INSENSITIVE)

  def hasSolrAlias(sqlText: String): Boolean =
    sqlText.toLowerCase.indexOf(") as solr") != -1

  def parseColumns(sqlText: String): Option[Map[String, String]] = {
    try {
      val cols = SolrSQLSupport.parseColumns(sqlText)
      if (cols.isEmpty) {
        logger.info("No columns found for sub-query [" + sqlText + "], cannot push down into Solr")
        return None
      }
      Some(cols.toMap)
    } catch {
      case e: Exception =>
        logger.warn("Failed to parse columns for sub-query [" + sqlText + "] due to: " + e)
        None
    }
  }

  def findSolrCollectionNameInSql(sqlText: String): Option[String] = {
    val collectionIdMatcher = solrCollectionInSqlPattern.matcher(sqlText)
    if (!collectionIdMatcher.find()) {
      logger.warn(s"No push-down to Solr! Cannot determine collection name from Solr SQL query: ${sqlText}")
      return None
    }
    Some(collectionIdMatcher.group(1))
  }

  def getTempTableName(sqlText: String, tableName: String): String = {
    val md5 = DigestUtils.md5Hex(sqlText)
    tableName + "_sspd_" + md5
  }
}

class SolrSessionCatalog(
    solrSQLSession: SolrSparkSession)
  extends CatalogImpl(solrSQLSession) {

  override def clearCache(): Unit = {
    solrSQLSession.clearQueryCache()
    super.clearCache()
  }
}

trait TablePermissionChecker {
  def checkQueryAccess(sparkSession: SolrSparkSession, ugi: UserGroupInformation, securedResource: SecuredResource): Unit
  def checkWriteAccess(sparkSession: SolrSparkSession, ugi: UserGroupInformation, securedResource: SecuredResource): Unit
}
