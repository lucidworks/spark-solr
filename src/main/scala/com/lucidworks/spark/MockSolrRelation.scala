package com.lucidworks.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

class MockSolrRelation(parameters: Map[String, String], sparkSession: SparkSession) extends SolrRelation(parameters, None, sparkSession) {

  override def schema: StructType = {
    StructType(StructField("user_id", StringType) :: StructField("age", IntegerType) :: StructField("gender", StringType) :: Nil)
  }
  override def buildScan(fields: Array[String], filters: Array[Filter]): RDD[Row] = {
    sparkSession.emptyDataFrame.rdd
  }

  override val uniqueKey: String = "id"
  override val querySchema: StructType = StructType(StructField("user_id", StringType) :: StructField("age", IntegerType) :: StructField("gender", StringType) :: Nil)

}
