package com.lucidworks.spark

import com.lucidworks.spark.util.ConfigurationConstants._
import com.lucidworks.spark.util.QueryConstants._
import com.lucidworks.spark.util.SolrRelationUtil
import org.apache.commons.lang3.StringUtils
import org.apache.solr.common.params.ModifiableSolrParams

class SolrConf(config: Map[String, String]) extends Serializable with LazyLogging {

  require(config != null, "Config cannot be null")
  require(config.nonEmpty, "Config cannot be empty")

  var zkHostFromDriverEnv : Option[String] = None

  def getZkHost: Option[String] = {
    if (zkHostFromDriverEnv.isDefined) return zkHostFromDriverEnv
    if (config.contains(SOLR_ZK_HOST_PARAM)) return config.get(SOLR_ZK_HOST_PARAM)

    // allow users to set the zkhost using a Java system property or env property
    val zkHostSysProp = System.getProperty("solr.zkhost", System.getenv("SOLR_ZKHOST"))
    if (zkHostSysProp != null) {
      // stash it for later use in executors
      zkHostFromDriverEnv = Some(zkHostSysProp)
      return zkHostFromDriverEnv
    }

    None
  }

  def getCollection: Option[String] = config.get(SOLR_COLLECTION_PARAM)

  def getCollectionAlias: Option[String] = config.get(COLLECTION_ALIAS)

  def getQuery: Option[String] = config.get(SOLR_QUERY_PARAM)

  def getStreamingExpr: Option[String] = config.get(SOLR_STREAMING_EXPR)

  def getSqlStmt: Option[String] = config.get(SOLR_SQL_STMT)

  def getSplitField: Option[String] = config.get(SOLR_SPLIT_FIELD_PARAM)

  def getFields: Array[String] =
    if (config.get(SOLR_FIELD_PARAM).isDefined && !StringUtils.equals(config(SOLR_FIELD_PARAM), "*"))
      config(SOLR_FIELD_PARAM).split(",").map(_.trim)
    else
      Array.empty

  def getFilters: List[String] =
    if (config.get(SOLR_FILTERS_PARAM).isDefined)
      SolrRelationUtil.parseCommaSeparatedValuesToList(config(SOLR_FILTERS_PARAM))
    else List.empty

  def partitionBy: Option[String] = config.get(PARTITION_BY)

  def getTimestampFieldName: Option[String] = config.get(TIMESTAMP_FIELD_NAME)

  def getTimePeriod: Option[String] = config.get(TIME_PERIOD)

  def getDateTimePattern: Option[String] = config.get(DATETIME_PATTERN)

  def getTimeZoneId: Option[String] = config.get(TIMEZONE_ID)

  def getMaxActivePartitions: Option[String]= config.get(MAX_ACTIVE_PARTITIONS)

  def getSort: Option[String] = config.get(SORT_PARAM)

  def getStreamingExpressionSchema: Option[String] = config.get(STREAMING_EXPR_SCHEMA)

  def getSolrSQLSchema: Option[String] = config.get(SOLR_SQL_SCHEMA)

  def getExcludeFields: Option[String] = config.get(EXCLUDE_FIELDS)

  def getChildDocFieldName: Option[String] = config.get(CHILD_DOC_FIELDNAME)

  def skipNonDocValueFields: Option[Boolean] =
    if (config.get(SKIP_NON_DOCVALUE_FIELDS).isDefined) Some(config(SKIP_NON_DOCVALUE_FIELDS).toBoolean) else None

  def maxRows: Option[Int] =
    if (config.get(MAX_ROWS).isDefined) Some(config(MAX_ROWS).toInt) else None

  def getAccumulatorName: Option[String] = config.get(ACCUMULATOR_NAME)

  def getRows: Option[Int] =
    if (config.get(SOLR_ROWS_PARAM).isDefined) Some(config(SOLR_ROWS_PARAM).toInt) else None

  def splits: Option[Boolean] =
    if (config.get(SOLR_DO_SPLITS).isDefined) Some(config(SOLR_DO_SPLITS).toBoolean) else None

  def docValues: Option[Boolean] =
    if (config.get(SOLR_DOC_VALUES).isDefined) Some(config(SOLR_DOC_VALUES).toBoolean) else None

  def getSplitsPerShard: Option[Int] =
    if (config.get(SOLR_SPLITS_PER_SHARD_PARAM).isDefined) Some(config(SOLR_SPLITS_PER_SHARD_PARAM).toInt) else None

  def escapeFieldNames: Option[Boolean] =
    if (config.get(ESCAPE_FIELDNAMES_PARAM).isDefined) Some(config(ESCAPE_FIELDNAMES_PARAM).toBoolean) else None

  def flattenMultivalued: Option[Boolean] =
    if (config.get(FLATTEN_MULTIVALUED).isDefined) Some(config(FLATTEN_MULTIVALUED).toBoolean) else None

  def softAutoCommitSecs: Option[Int] =
    if (config.get(SOFT_AUTO_COMMIT_SECS).isDefined) Some(config(SOFT_AUTO_COMMIT_SECS).toInt) else None

  def commitWithin: Option[Int] =
    if (config.get(COMMIT_WITHIN_MILLI_SECS).isDefined) Some(config(COMMIT_WITHIN_MILLI_SECS).toInt) else None

  def batchSize: Option[Int] =
    if (config.get(BATCH_SIZE).isDefined) Some(config(BATCH_SIZE).toInt) else None

  def useCursorMarks: Option[Boolean] =
    if (config.get(USE_CURSOR_MARKS).isDefined) Some(config(USE_CURSOR_MARKS).toBoolean) else None

  def genUniqKey: Option[Boolean] =
    if (config.get(GENERATE_UNIQUE_KEY).isDefined) Some(config(GENERATE_UNIQUE_KEY).toBoolean) else None

  def genUniqChildKey: Option[Boolean] =
    if (config.get(GENERATE_UNIQUE_CHILD_KEY).isDefined) Some(config(GENERATE_UNIQUE_CHILD_KEY).toBoolean) else None

  def sampleSeed: Option[Int] =
    if (config.get(SAMPLE_SEED).isDefined) Some(config(SAMPLE_SEED).toInt) else None

  def samplePct: Option[Float] =
    if (config.get(SAMPLE_PCT).isDefined) Some(config(SAMPLE_PCT).toFloat) else None

  def schema: Option[String] = config.get(SCHEMA)

  def getMaxShardsForSchemaSampling: Option[Int] = {
    if (config.get(MAX_SHARDS_FOR_SCHEMA_SAMPLING).isDefined) Some(config(MAX_SHARDS_FOR_SCHEMA_SAMPLING).toInt) else None
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

  def getSolrFieldTypes: Option[String] = config.get(SOLR_FIELD_TYPES)

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

  def getExtraOptions: Map[String, String] = {
    val extraParams = SolrRelation.checkUnknownParams(config.keySet)
    config.filter(c => extraParams.contains(c._1))
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
    if (getMaxShardsForSchemaSampling.isDefined) {
      sb ++= s", ${MAX_SHARDS_FOR_SCHEMA_SAMPLING}=${getMaxShardsForSchemaSampling.get}"
    }

    sb ++= s", extraOptions=${getExtraOptions}"

    // time-based partitioning options
    if (partitionBy.isDefined) {
      sb ++= s", ${PARTITION_BY}=${partitionBy.get}"
    }
    if (getTimestampFieldName.isDefined) {
      sb ++= s", ${TIMESTAMP_FIELD_NAME}=${getTimestampFieldName.get}"
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
    if (getAccumulatorName.isDefined) {
      sb ++= s", ${ACCUMULATOR_NAME}=${getAccumulatorName.get}"
    }
    if (getSolrFieldTypes.isDefined) {
      sb ++= s", ${SOLR_FIELD_TYPES}=${getSolrFieldTypes.get}"
    }
    if (getCollectionAlias.isDefined) {
      sb ++= s", ${COLLECTION_ALIAS}=${getCollectionAlias.get}"
    }
    sb ++= ")"
    sb.toString
  }
}
