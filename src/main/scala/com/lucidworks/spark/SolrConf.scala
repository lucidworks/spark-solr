package com.lucidworks.spark

import org.apache.solr.common.params.ModifiableSolrParams
import com.lucidworks.spark.util.ConfigurationConstants._

class SolrConf(config: Map[String, String]) {

  require(config != null, "Config cannot be null")
  require(config.nonEmpty, "Config cannot be empty")

  val solrConfigParams: ModifiableSolrParams = SolrConf.parseSolrParams(config)

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


}

object SolrConf {

  // Anything in the form of "solr.*" will override the "q", "fields", "rows" in the config
  def parseSolrParams(config: Map[String, String]): ModifiableSolrParams = {
    val params = new ModifiableSolrParams()

    for (key <- config.keySet) {
      if (key.startsWith(CONFIG_PREFIX)) {
        val param = key.substring(CONFIG_PREFIX.length+1)
        if (config.get(key).isDefined) {
          params.add(param, config.get(key).get)
        }
      }
    }
    params
  }
}

