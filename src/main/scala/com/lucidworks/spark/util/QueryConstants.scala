package com.lucidworks.spark.util

object QueryConstants {
  val DEFAULT_REQUIRED_FIELD: String = "id"
  val DEFAULT_PAGE_SIZE: Int = 1000
  val DEFAULT_QUERY: String = "*:*"
  val DEFAULT_SPLITS_PER_SHARD: Int = 20
  val DEFAULT_SPLIT_FIELD: String = "_version_"
  val DEFAULT_REQUEST_HANDLER: String = "/select"
  val DEFAULT_TIME_STAMP_FIELD_NAME: String = "timestamp_tdt"
  val DEFAULT_TIME_PERIOD: String = "1DAYS"
  val DEFAULT_TIMEZONE_ID: String = "UTC"
  val DEFAULT_DATETIME_PATTERN: String = "yyyy_MM_dd"
}
