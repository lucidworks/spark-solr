package com.lucidworks.spark.util

// This should only be used for config options for the sql statements [SolrRelation]
object ConfigurationConstants {
  val SOLR_ZK_HOST_PARAM: String = "zkhost"
  val SOLR_COLLECTION_PARAM: String = "collection"

  // Query params
  val SOLR_QUERY_PARAM: String = "query"
  val SOLR_FIELD_PARAM: String = "fields"
  val SOLR_ROWS_PARAM: String = "rows"
  val SOLR_DO_SPLITS: String = "splits"
  val SOLR_SPLIT_FIELD_PARAM: String = "split_field"
  val SOLR_SPLITS_PER_SHARD_PARAM: String = "splits_per_shard"
  val ESCAPE_FIELDNAMES_PARAM: String = "escape_fieldnames"
  val SKIP_NON_DOCVALUE_FIELDS: String = "skip_non_dv"
  val SOLR_DOC_VALUES: String = "dv"
  val FLATTEN_MULTIVALUED: String = "flatten_multivalued"
  val REQUEST_HANDLER: String = "request_handler"
  val USE_CURSOR_MARKS: String = "use_cursor_marks"
  val SOLR_STREAMING_EXPR: String = "expr"
  val SOLR_SQL_STMT: String = "sql"
  val SORT_PARAM: String = "sort"

  // Index params
  val SOFT_AUTO_COMMIT_SECS: String = "soft_commit_secs"
  val BATCH_SIZE: String = "batch_size"
  val GENERATE_UNIQUE_KEY: String = "gen_uniq_key"
  val COMMIT_WITHIN_MILLI_SECS: String = "commit_within"

  val SAMPLE_SEED: String = "sample_seed"
  val SAMPLE_PCT: String = "sample_pct"

  // Time series partitioning params

  val PARTITION_BY:String="partition_by"
  val TIME_STAMP_FIELD_NAME:String="time_stamp_field_name"
  val TIME_PERIOD:String="time_period"
  val DATETIME_PATTERN:String="datetime_pattern"
  val TIMEZONE_ID:String="timezone_id"
  val MAX_ACTIVE_PARTITIONS:String="max_active_partitions"

  val ARBITRARY_PARAMS_STRING: String = "solr.params"

  val STREAMING_EXPR_SCHEMA: String = "expr_schema"
  val SOLR_SQL_SCHEMA: String = "sql_schema"
  val EXCLUDE_FIELDS: String = "exclude_fields"
  val MAX_ROWS: String = "max_rows"
}
