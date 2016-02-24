package com.lucidworks.spark.util


object ConfigurationConstants {
  val CONFIG_PREFIX: String = "solr."
  val SOLR_ZK_HOST_PARAM: String = "zkhost"
  val SOLR_COLLECTION_PARAM: String = "collection"
  val SOLR_QUERY_PARAM: String = "q"
  val SOLR_FIELD_PARAM: String = "fl"
  val SOLR_ROWS_PARAM: String = "rows"
  val SOLR_DO_SPLITS: String = "splits"
  val SOLR_SPLIT_FIELD_PARAM: String = "split.field"
  val SOLR_SPLITS_PER_SHARD_PARAM: String = "splits.per.shard"
  val ESCAPE_FIELDNAMES_PARAM: String = "escape.fieldnames"
  val SOLR_DOC_VALUES: String = "dv"
}
