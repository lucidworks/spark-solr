package com.lucidworks.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.json4s.{JObject, JValue}

import scala.collection.mutable.ArrayBuffer

object JsonFacetUtil extends LazyLogging {

  def parseFacetResponse(facets: JValue, spark: SparkSession): DataFrame = {
    facets match {
      case jo : JObject =>
        val nestedFacetKeys = jo.values.filter(f => isNestedFacetBlock(f._2)).keySet
        if (nestedFacetKeys.nonEmpty) {
          if (nestedFacetKeys.size > 1) {
            logger.info(s"Multiple nested level facets defined. Only using field ${nestedFacetKeys.head} to generate dataframe")
          }
          jo.values(nestedFacetKeys.head) match {
            case fjo: Map[String, _] @unchecked =>
              if (fjo.contains("buckets")) {
                // process facet values into dataframe
                fjo("buckets") match {
                  case buckets: List[Map[String, _]] @unchecked =>
                    val schema : StructType = StructType(formSchema(nestedFacetKeys.head, buckets.head, Array.empty[StructField]))
                    val data = convertFacetBucketsToDataFrame(buckets, schema)
                    val rows: RDD[Row] = spark.sparkContext.parallelize(data, 1)
                    return spark.createDataFrame(rows, schema)
                  case a: Any => logger.info(s"Unknown type ${a.getClass}")
                }
              }
            case a: Any => logger.info(s"Unknown type ${a.getClass}")
          }
        } else {
          // no nested fields, just parse the keys and values
          val fields: ArrayBuffer[StructField] = ArrayBuffer.empty[StructField]
          val row: ArrayBuffer[Any] = ArrayBuffer.empty
          for ((k, v) <- jo.values) {
            val dv = getDataTypeAndValue(v)
            fields.+=(StructField(k, dv._1))
            row.+=(dv._2)
          }
          val rddRow = spark.sparkContext.parallelize(Seq(Row.fromSeq(row)), 1)
          return spark.createDataFrame(rddRow, StructType(fields))
        }
      case _ => //
    }
    spark.emptyDataFrame
  }

  private def getDataTypeAndValue(a: Any) : (DataType, Any) = {
    a match {
      case bd : BigDecimal =>
        (DataTypes.DoubleType, bd.toDouble)
      case bi: BigInt =>
        (DataTypes.LongType, bi.toLong)
      case jd : java.lang.Double =>
        (DataTypes.DoubleType, jd.toDouble)
      case s : String =>
        (DataTypes.StringType, s)
      case a: Any => throw new Exception(s"Non compatible type: ${a.getClass}")
    }
  }

  private def isNestedFacetBlock(any: Any): Boolean = {
    any match {
      case m : Map[String, _] @unchecked =>
        if (m.contains("buckets")) return true
      case _ => //
    }
    false
  }

  private def formSchema(key: String, head: Map[String, _], schema: Array[StructField]) : Array[StructField] = {
    val buffer = ArrayBuffer.empty[StructField]
    buffer.++=(schema)
    if (head.contains("val")) {
      val dv = getDataTypeAndValue(head("val"))
      buffer.+=(StructField(key, dv._1))
    }
    if (head.contains("count")) {
      buffer.+=(StructField(s"${key}_count", DataTypes.LongType))
    }
    for ((k, v) <- head.filter(p => p._1 != "count" && p._1 != "val")) {
      v match {
        case jo : Map[String, _] @unchecked=>
          if (jo.contains("buckets")) {
            jo("buckets") match {
              case ja: List[Map[String, _]] @unchecked => return formSchema(k, ja.head, buffer.toArray)
            }
          }
        case _ => //
      }
    }
    buffer.toArray
  }

  private def convertFacetBucketsToDataFrame(value: List[Map[String, _]], schema: StructType) : Array[Row] = {
    convertFacetBucketsToDataFrame(value, ArrayBuffer.empty, Array.empty, schema)
  }

  private def convertFacetBucketsToDataFrame(value: List[Map[String, _]], rows: ArrayBuffer[Row], array: Array[Any], schema: StructType) : Array[Row] = {
    for (bucket <- value) {
      val buffer = ArrayBuffer.empty[Any]
      buffer.++=(array)
      if (bucket.contains("val")) {
        val dv = getDataTypeAndValue(bucket("val"))
        buffer.+=(dv._2)
      }
      if (bucket.contains("count")) {
        buffer.+=(bucket("count").asInstanceOf[BigInt].toLong)
      }
      if (buffer.length == schema.fieldNames.length) {
        val row = Row.fromSeq(buffer)
        rows.+=(row)
      }
      for ((_, v) <- bucket.filter(p => p._1 != "count" && p._1 != "val")) {
        v match {
          case jo : Map[String, _] @unchecked =>
            if (jo.contains("buckets")) {
              jo("buckets") match {
                case ja: List[Map[String, _]] @unchecked => convertFacetBucketsToDataFrame(ja, rows, buffer.toArray, schema)
              }
            }
        }
      }
    }
    rows.toArray
  }
}
