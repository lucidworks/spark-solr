package com.lucidworks.spark.util

object QueryConstants {
  // Request handlers
  val QT_STREAM = "/stream"
  val QT_SQL = "/sql"
  val QT_EXPORT = "/export"
  val QT_SELECT = "/select"

  val DEFAULT_REQUIRED_FIELD: String = "id"
  val DEFAULT_PAGE_SIZE: Int = 5000
  val DEFAULT_QUERY: String = "*:*"
  val DEFAULT_SPLITS_PER_SHARD: Int = 10
  val DEFAULT_SPLIT_FIELD: String = "_version_"
  val DEFAULT_REQUEST_HANDLER: String = QT_SELECT
  val DEFAULT_TIMESTAMP_FIELD_NAME: String = "timestamp_tdt"
  val DEFAULT_TIME_PERIOD: String = "1DAYS"
  val DEFAULT_TIMEZONE_ID: String = "UTC"
  val DEFAULT_DATETIME_PATTERN: String = "yyyy_MM_dd"
  val DEFAULT_CHILD_DOC_FIELD_NAME: String = "_childDocuments_"
}
