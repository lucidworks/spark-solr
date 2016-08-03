package com.lucidworks.spark

import org.apache.spark.Logging
import org.apache.solr.common.params.ModifiableSolrParams
import com.lucidworks.spark.util.QueryConstants._
import com.lucidworks.spark.util.ConfigurationConstants._

class SolrConf(config: Map[String, String]) extends Logging {

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

  def requestHandler: String = {

    if (!config.contains(REQUEST_HANDLER) && config.contains(USE_EXPORT_HANDLER) && config.get(USE_EXPORT_HANDLER).isDefined) {
      logWarning(s"The ${USE_EXPORT_HANDLER} option is no longer supported, please switch to using the ${REQUEST_HANDLER} -> ${QT_EXPORT} option!")
      if (config.get(USE_EXPORT_HANDLER).get.toBoolean) {
        return QT_EXPORT
      }
    }

    if (!config.contains(REQUEST_HANDLER) && config.contains(SOLR_STREAMING_EXPR) && config.get(SOLR_STREAMING_EXPR).isDefined) {
      // they didn't specify a request handler but gave us an expression, so we know the request handler should be /stream
      logInfo(s"Set ${REQUEST_HANDLER} to ${QT_STREAM} because the ${SOLR_STREAMING_EXPR} option is set.")
      return QT_STREAM
    }

    if (!config.contains(REQUEST_HANDLER) && config.contains(SOLR_SQL_STMT) && config.get(SOLR_SQL_STMT).isDefined) {
      // they didn't specify a request handler but gave us an expression, so we know the request handler should be /stream
      logInfo(s"Set ${REQUEST_HANDLER} to ${QT_SQL} because the ${SOLR_SQL_STMT} option is set.")
      return QT_SQL
    }

    if (config.contains(REQUEST_HANDLER) && config.get(REQUEST_HANDLER).isDefined) {
      return config.get(REQUEST_HANDLER).get
    }

    DEFAULT_REQUEST_HANDLER
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

  def getArbitrarySolrParams: ModifiableSolrParams = {
    val solrParams = new ModifiableSolrParams()
    if (config.contains(ARBITRARY_PARAMS_STRING) && config.get(ARBITRARY_PARAMS_STRING).isDefined) {
      val paramString = config.get(ARBITRARY_PARAMS_STRING).get
      val params = paramString.split("&")

      for (param <- params) {
        val keyValue = param.split("=")
        val key = keyValue(0)
        val value = keyValue(1)
        solrParams.add(key, value)
      }
    }
    solrParams
  }
}
