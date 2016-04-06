package com.lucidworks.spark

import org.apache.solr.common.params.ModifiableSolrParams
import com.lucidworks.spark.util.ConfigurationConstants._

class SolrConf(config: Map[String, String]) {

  require(config != null, "Config cannot be null")
  require(config.nonEmpty, "Config cannot be empty")

  def getZkHost: Option[String] = {
    if (config.contains(SOLR_ZK_HOST_PARAM)) return config.get(SOLR_ZK_HOST_PARAM)
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

  def batchSize: Option[Int] = {
    if (config.contains(BATCH_SIZE) && config.get(BATCH_SIZE).isDefined) {
      return Some(config.get(BATCH_SIZE).get.toInt)
    }
    None
  }

  def useExportHandler: Option[Boolean] = {
    if (config.contains(USE_EXPORT_HANDLER) && config.get(USE_EXPORT_HANDLER).isDefined) {
      return Some(config.get(USE_EXPORT_HANDLER).get.toBoolean)
    }
    None
  }

  def genUniqKey: Option[Boolean] = {
    if (config.contains(GENERATE_UNIQUE_KEY) && config.get(GENERATE_UNIQUE_KEY).isDefined) {
      return Some(config.get(GENERATE_UNIQUE_KEY).get.toBoolean)
    }
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
