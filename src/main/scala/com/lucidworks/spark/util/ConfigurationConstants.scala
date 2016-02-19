package com.lucidworks.spark.util


trait ConfigurationConstants {
  val CONFIG_PREFIX: String = "solr."
  val SOLR_ZK_HOST_PARAM: String = "zkhost"
  val SOLR_COLLECTION_PARAM: String = "collection"
  val SOLR_QUERY_PARAM: String = "query"
  val SOLR_FIELD_LIST_PARAM: String = "query.fields"
  val SOLR_ROWS_PARAM: String = "query.rows"
  val SOLR_SPLIT_FIELD_PARAM: String = "query.split.field"
  val SOLR_SPLITS_PER_SHARD_PARAM: String = "query.splits.per.shard"
  val ESCAPE_FIELDNAMES: String = "escape.fieldnames"
}
