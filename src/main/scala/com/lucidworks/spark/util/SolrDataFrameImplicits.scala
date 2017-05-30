package com.lucidworks.spark.util

import org.apache.spark.sql.{DataFrameReader, DataFrameWriter, Row, SaveMode}

/**
  * Usage:
  *
  * import SolrDataFrameImplicits._
  * // then you can:
  * val spark: SparkSession
  * val collectionName: String
  * val df = spark.read.solr(collectionName)
  * // do stuff
  * df.write.solr(collectionName, overwrite = true)
  * // or various other combinations, like setting your own options earlier
  * df.write.option("zkhost", "some other solr cluster's zk host").solr(collectionName)
  */
object SolrDataFrameImplicits {

  implicit class SolrReader(reader: DataFrameReader) {
    def solr(collection: String, query: String = "*:*") =
      reader.format("solr").option("collection", collection).option("query", query).load()
    def solr(collection: String, options: Map[String, String]) =
      reader.format("solr").option("collection", collection).options(options).load()
  }

  implicit class SolrWriter(writer: DataFrameWriter[Row]) {
    def solr(collectionName: String, softCommitSecs: Int = 10, overwrite: Boolean = false, format: String = "solr") = {
      writer
        .format(format)
        .option("collection", collectionName)
        .option("soft_commit_secs", softCommitSecs.toString)
        .mode(if(overwrite) SaveMode.Overwrite else SaveMode.Append)
        .save()
    }
  }
}
