package com.lucidworks.spark.util

// This should only be used for config options for the sql statements [SolrRelation]
object ConfigurationConstants {
  val ARBITRARY_PARAMS_STRING: String = "solr.params"
  val SOLR_ZK_HOST_PARAM: String = "zkhost"
  val SOLR_COLLECTION_PARAM: String = "collection"
  val SOLR_QUERY_PARAM: String = "query"
  val SOLR_FIELD_PARAM: String = "fields"
  val SOLR_ROWS_PARAM: String = "rows"
  val SOLR_DO_SPLITS: String = "splits"
  val SOLR_SPLIT_FIELD_PARAM: String = "split_field"
  val SOLR_SPLITS_PER_SHARD_PARAM: String = "splits_per_shard"
  val ESCAPE_FIELDNAMES_PARAM: String = "escape_fieldnames"
  val SOLR_DOC_VALUES: String = "dv"
  val FLATTEN_MULTIVALUED: String = "flatten_multivalued"
  val SOFT_AUTO_COMMIT_SECS: String = "soft_commit_secs"
  val BATCH_SIZE: String = "batch_size"
  val USE_EXPORT_HANDLER: String = "use_export_handler"
  val GENERATE_UNIQUE_KEY: String = "gen_uniq_key"
}
