package com.lucidworks.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.solr.common.params.ModifiableSolrParams
import com.lucidworks.spark.util.QueryConstants._
import com.lucidworks.spark.util.ConfigurationConstants._

class SolrConf(config: Map[String, String]) extends Serializable with LazyLogging {

  require(config != null, "Config cannot be null")
  require(config.nonEmpty, "Config cannot be empty")

  def getZkHost: Option[String] = {
    if (config.contains(SOLR_ZK_HOST_PARAM)) return config.get(SOLR_ZK_HOST_PARAM)

    // allow users to set the zkhost using a Java system property
    var zkHostSysProp = System.getProperty("solr.zkhost")
    if (zkHostSysProp != null) return Some(zkHostSysProp)

    None
  }

  def getCollection: Option[String] = {
    if (config.contains(SOLR_COLLECTION_PARAM)) return config.get(SOLR_COLLECTION_PARAM)
    None
  }

  def getQuery: Option[String] = {
    if (config.contains(SOLR_QUERY_PARAM)) return config.get(SOLR_QUERY_PARAM)
    None
  }

  def getStreamingExpr: Option[String] = {
    if (config.contains(SOLR_STREAMING_EXPR) && config.get(SOLR_STREAMING_EXPR).isDefined) {
      return config.get(SOLR_STREAMING_EXPR)
    }
    None
  }

  def getSqlStmt: Option[String] = {
    if (config.contains(SOLR_SQL_STMT) && config.get(SOLR_SQL_STMT).isDefined) {
      return config.get(SOLR_SQL_STMT)
    }
    None
  }

  def getFields: Array[String] = {
    if (config.contains(SOLR_FIELD_PARAM) && config.get(SOLR_FIELD_PARAM).isDefined) {
      return config.get(SOLR_FIELD_PARAM).get.split(",").map(field => field.trim)
    }
    Array.empty[String]
  }

  def getFilters: Array[String] = {
    if (config.contains(SOLR_FILTERS_PARAM) && config.get(SOLR_FILTERS_PARAM).isDefined) {
      return config(SOLR_FILTERS_PARAM).split(",").map(filter => filter.trim)
    }
    Array.empty[String]
  }

  def getRows: Option[Int] = {
    if (config.contains(SOLR_ROWS_PARAM) && config.get(SOLR_ROWS_PARAM).isDefined) {
      return Some(config.get(SOLR_ROWS_PARAM).get.toInt)
    }
    None
  }

  def splits: Option[Boolean] = {
    if (config.contains(SOLR_DO_SPLITS) && config.get(SOLR_DO_SPLITS).isDefined) {
      return Some(config.get(SOLR_DO_SPLITS).get.toBoolean)
    }
    None
  }

  def docValues: Option[Boolean] = {
    if (config.contains(SOLR_DOC_VALUES) && config.get(SOLR_DOC_VALUES).isDefined) {
      return Some(config.get(SOLR_DOC_VALUES).get.toBoolean)
    }
    None
  }

  def getSplitField: Option[String] = {
    if (config.contains(SOLR_SPLIT_FIELD_PARAM)) return config.get(SOLR_SPLIT_FIELD_PARAM)
    None
  }

  def getSplitsPerShard: Option[Int] = {
    if (config.contains(SOLR_SPLITS_PER_SHARD_PARAM) && config.get(SOLR_SPLITS_PER_SHARD_PARAM).isDefined) {
      return Some(config.get(SOLR_SPLITS_PER_SHARD_PARAM).get.toInt)
    }
    None
  }

  def escapeFieldNames: Option[Boolean] = {
    if (config.contains(ESCAPE_FIELDNAMES_PARAM) && config.get(ESCAPE_FIELDNAMES_PARAM).isDefined) {
      return Some(config.get(ESCAPE_FIELDNAMES_PARAM).get.toBoolean)
    }
    None
  }

  def flattenMultivalued: Option[Boolean] = {
    if (config.contains(FLATTEN_MULTIVALUED) && config.get(FLATTEN_MULTIVALUED).isDefined) {
      return Some(config.get(FLATTEN_MULTIVALUED).get.toBoolean)
    }
    None
  }

  def softAutoCommitSecs: Option[Int] = {
    if (config.contains(SOFT_AUTO_COMMIT_SECS) && config.get(SOFT_AUTO_COMMIT_SECS).isDefined) {
      return Some(config.get(SOFT_AUTO_COMMIT_SECS).get.toInt)
    }
    None
  }

  def commitWithin: Option[Int] = {
    if (config.contains(COMMIT_WITHIN_MILLI_SECS) && config.get(COMMIT_WITHIN_MILLI_SECS).isDefined) {
      return Some(config.get(COMMIT_WITHIN_MILLI_SECS).get.toInt)
    }
    None
  }

  def batchSize: Option[Int] = {
    if (config.contains(BATCH_SIZE) && config.get(BATCH_SIZE).isDefined) {
      return Some(config.get(BATCH_SIZE).get.toInt)
    }
    None
  }

  def requestHandler: Option[String] = {

    if (!config.contains(REQUEST_HANDLER) && config.contains(SOLR_STREAMING_EXPR) && config.get(SOLR_STREAMING_EXPR).isDefined) {
      // they didn't specify a request handler but gave us an expression, so we know the request handler should be /stream
      logger.debug(s"Set ${REQUEST_HANDLER} to ${QT_STREAM} because the ${SOLR_STREAMING_EXPR} option is set.")
      return Some(QT_STREAM)
    }

    if (!config.contains(REQUEST_HANDLER) && config.contains(SOLR_SQL_STMT) && config.get(SOLR_SQL_STMT).isDefined) {
      // they didn't specify a request handler but gave us an expression, so we know the request handler should be /stream
      logger.debug(s"Set ${REQUEST_HANDLER} to ${QT_SQL} because the ${SOLR_SQL_STMT} option is set.")
      return Some(QT_SQL)
    }

    if (config.contains(REQUEST_HANDLER) && config.get(REQUEST_HANDLER).isDefined) {
      return Some(config.get(REQUEST_HANDLER).get)
    }

    None
  }

  def useCursorMarks: Option[Boolean] = {
    if (config.contains(USE_CURSOR_MARKS) && config.get(USE_CURSOR_MARKS).isDefined) {
      return Some(config.get(USE_CURSOR_MARKS).get.toBoolean)
    }
    None
  }

  def genUniqKey: Option[Boolean] = {
    if (config.contains(GENERATE_UNIQUE_KEY) && config.get(GENERATE_UNIQUE_KEY).isDefined) {
      return Some(config.get(GENERATE_UNIQUE_KEY).get.toBoolean)
    }
    None
  }

  def genUniqChildKey: Option[Boolean] = {
    if (config.contains(GENERATE_UNIQUE_CHILD_KEY) && config.get(GENERATE_UNIQUE_CHILD_KEY).isDefined) {
      return Some(config.get(GENERATE_UNIQUE_CHILD_KEY).get.toBoolean)
    }
    None
  }

  def sampleSeed: Option[Int] = {
    if (config.contains(SAMPLE_SEED) && config.get(SAMPLE_SEED).isDefined) {
      return Some(config.get(SAMPLE_SEED).get.toInt)
    }
    None
  }

  def samplePct: Option[Float] = {
    if (config.contains(SAMPLE_PCT) && config.get(SAMPLE_PCT).isDefined) {
      return Some(config.get(SAMPLE_PCT).get.toFloat)
    }
    None
  }

  def partition_by: Option[String]={
    if (config.contains(PARTITION_BY) && config.get(PARTITION_BY).isDefined) {
      return Some(config.get(PARTITION_BY).get.toString)
    }
    None
  }

  def getTimeStampFieldName: Option[String]={
    if (config.contains(TIME_STAMP_FIELD_NAME) && config.get(TIME_STAMP_FIELD_NAME).isDefined) return (config.get(TIME_STAMP_FIELD_NAME))
    None
  }

  def getTimePeriod: Option[String]={
    if (config.contains(TIME_PERIOD) && config.get(TIME_PERIOD).isDefined) return (config.get(TIME_PERIOD))
    None
  }

  def getDateTimePattern: Option[String]={
    if (config.contains(DATETIME_PATTERN) && config.get(DATETIME_PATTERN).isDefined) return (config.get(DATETIME_PATTERN))
    None
  }

  def getTimeZoneId: Option[String]={
    if (config.contains(TIMEZONE_ID) && config.get(TIMEZONE_ID).isDefined) return (config.get(TIMEZONE_ID))
    None
  }

  def getMaxActivePartitions: Option[String]={
    if (config.contains(MAX_ACTIVE_PARTITIONS) && config.get(MAX_ACTIVE_PARTITIONS).isDefined) return (config.get(MAX_ACTIVE_PARTITIONS))
    None
  }

  def getSort: Option[String] = {
    if (config.contains(SORT_PARAM) && config.get(SORT_PARAM).isDefined) return config.get(SORT_PARAM)
    None
  }

  def getArbitrarySolrParams: ModifiableSolrParams = {
    val solrParams = new ModifiableSolrParams()
    if (config.contains(ARBITRARY_PARAMS_STRING) && config.get(ARBITRARY_PARAMS_STRING).isDefined) {
      val paramString = config.get(ARBITRARY_PARAMS_STRING).get
      val params = paramString.split("&")
      for (param <- params) {
        val eqAt = param.indexOf('=')
        if (eqAt != -1) {
          val key = param.substring(0,eqAt)
          val value = param.substring(eqAt+1)
          solrParams.add(key, value)
        }
      }
    }
    solrParams
  }

  def getStreamingExpressionSchema: Option[String] = {
    if (config.contains(STREAMING_EXPR_SCHEMA) && config.get(STREAMING_EXPR_SCHEMA).isDefined) return config.get(STREAMING_EXPR_SCHEMA)
    None
  }

  def getSolrSQLSchema: Option[String] = {
    if (config.contains(SOLR_SQL_SCHEMA) && config.get(SOLR_SQL_SCHEMA).isDefined) return config.get(SOLR_SQL_SCHEMA)
    None
  }

  def getExcludeFields: Option[String] = {
    if (config.contains(EXCLUDE_FIELDS) && config.get(EXCLUDE_FIELDS).isDefined) return config.get(EXCLUDE_FIELDS)
    None
  }

  def skipNonDocValueFields: Option[Boolean] = {
    if (config.contains(SKIP_NON_DOCVALUE_FIELDS) && config.get(SKIP_NON_DOCVALUE_FIELDS).isDefined) {
      return Some(config.get(SKIP_NON_DOCVALUE_FIELDS).get.toBoolean)
    }
    None
  }

  def getChildDocFieldName: Option[String] = {
    if (config.contains(CHILD_DOC_FIELDNAME) && config.get(CHILD_DOC_FIELDNAME).isDefined) return config.get(CHILD_DOC_FIELDNAME)
    None
  }

  def maxRows: Option[Int] = {
    if (config.contains(MAX_ROWS) && config.get(MAX_ROWS).isDefined) {
      return Some(config.get(MAX_ROWS).get.toInt)
    }
    None
  }

  override def toString = {
    val sb = new StringBuilder
    sb ++= "SolrConf("
    sb ++= s"${SOLR_ZK_HOST_PARAM}=${getZkHost}"
    sb ++= s", ${SOLR_COLLECTION_PARAM}=${getCollection}"
    if (getQuery.isDefined) {
      sb ++= s", ${SOLR_QUERY_PARAM}=${getQuery.get}"
    }
    if (!getFields.isEmpty) {
      sb ++= s", ${SOLR_FIELD_PARAM}=${getFields.mkString(",")}"
    }
    if (getRows.isDefined) {
      sb ++= s", ${SOLR_ROWS_PARAM}=${getRows.get}"
    }
    if (maxRows.isDefined) {
      sb ++= s", ${MAX_ROWS}=${maxRows.get}"
    }
    if (splits.isDefined) {
      sb ++= s", ${SOLR_DO_SPLITS}=${splits.get}"
    }
    if (docValues.isDefined) {
      sb ++= s", ${SOLR_DOC_VALUES}=${docValues.get}"
    }
    if (getSplitField.isDefined) {
      sb ++= s", ${SOLR_SPLIT_FIELD_PARAM}=${getSplitField.get}"
    }
    if (getSplitsPerShard.isDefined) {
      sb ++= s", ${SOLR_SPLITS_PER_SHARD_PARAM}=${getSplitsPerShard.get}"
    }
    if (escapeFieldNames.isDefined) {
      sb ++= s", ${ESCAPE_FIELDNAMES_PARAM}=${escapeFieldNames.get}"
    }
    if (flattenMultivalued.isDefined) {
      sb ++= s", ${FLATTEN_MULTIVALUED}=${flattenMultivalued.get}"
    }
    if (requestHandler.isDefined) {
      sb ++= s", ${REQUEST_HANDLER}=${requestHandler.get}"
    }
    if (useCursorMarks.isDefined) {
      sb ++= s", ${USE_CURSOR_MARKS}=${useCursorMarks.get}"
    }
    if (sampleSeed.isDefined) {
      sb ++= s", ${SAMPLE_SEED}=${sampleSeed.get}"
    }
    if (samplePct.isDefined) {
      sb ++= s", ${SAMPLE_PCT}=${samplePct.get}"
    }
    if (getSort.isDefined) {
      sb ++= s", ${SORT_PARAM}=${getSort.get}"
    }
    if (getArbitrarySolrParams != null && getArbitrarySolrParams.size() > 0) {
      sb ++= s", ${ARBITRARY_PARAMS_STRING}=${getArbitrarySolrParams}"
    }
    if (getExcludeFields.isDefined) {
      sb ++= s", ${EXCLUDE_FIELDS}=${getExcludeFields.get}"
    }
    if (skipNonDocValueFields.isDefined) {
      sb ++= s", ${SKIP_NON_DOCVALUE_FIELDS}=${skipNonDocValueFields.get}"
    }
    if (getStreamingExpr.isDefined) {
      sb ++= s", ${SOLR_STREAMING_EXPR}=${getStreamingExpr.get}"
    }
    if (getStreamingExpressionSchema.isDefined) {
      sb ++= s", ${STREAMING_EXPR_SCHEMA}=${getStreamingExpressionSchema.get}"
    }
    if (getSqlStmt.isDefined) {
      sb ++= s", ${SOLR_SQL_STMT}=${getSqlStmt.get}"
    }
    if (getSolrSQLSchema.isDefined) {
      sb ++= s", ${SOLR_SQL_SCHEMA}=${getSolrSQLSchema.get}"
    }

    // time-based partitioning options
    if (partition_by.isDefined) {
      sb ++= s", ${PARTITION_BY}=${partition_by.get}"
    }
    if (getTimeStampFieldName.isDefined) {
      sb ++= s", ${TIME_STAMP_FIELD_NAME}=${getTimeStampFieldName.get}"
    }
    if (getTimePeriod.isDefined) {
      sb ++= s", ${TIME_PERIOD}=${getTimePeriod.get}"
    }
    if (getDateTimePattern.isDefined) {
      sb ++= s", ${DATETIME_PATTERN}=${getDateTimePattern.get}"
    }
    if (getTimeZoneId.isDefined) {
      sb ++= s", ${TIMEZONE_ID}=${getTimeZoneId.get}"
    }
    if (getMaxActivePartitions.isDefined) {
      sb ++= s", ${MAX_ACTIVE_PARTITIONS}=${getMaxActivePartitions.get}"
    }

    // indexing options
    if (genUniqKey.isDefined) {
      sb ++= s", ${GENERATE_UNIQUE_KEY}=${genUniqKey.get}"
    }
    if (softAutoCommitSecs.isDefined) {
      sb ++= s", ${SOFT_AUTO_COMMIT_SECS}=${softAutoCommitSecs.get}"
    }
    if (commitWithin.isDefined) {
      sb ++= s", ${COMMIT_WITHIN_MILLI_SECS}=${commitWithin.get}"
    }
    if (batchSize.isDefined) {
      sb ++= s", ${BATCH_SIZE}=${batchSize.get}"
    }
    if (getChildDocFieldName.isDefined) {
      sb ++= s", ${CHILD_DOC_FIELDNAME}=${getChildDocFieldName.get}"
    }
    sb ++= ")"
    sb.toString
  }
}
