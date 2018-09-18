package com.lucidworks.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.json4s.{JObject, JValue}

import scala.collection.mutable
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
                    val schema : StructType = StructType(formSchema(nestedFacetKeys.head, buckets.head, ArrayBuffer.empty[StructField]))
                    logger.debug(s"Dataframe schema for JSON facet query:  ${schema.mkString(",")}")
                    val data = convertFacetBucketsToDataFrame(buckets, schema)
                    val rows: RDD[Row] = spark.sparkContext.parallelize(data, 1)
                    return spark.createDataFrame(rows, schema)
                  case a: Any => logger.info(s"Unknown type ${a.getClass}")
                }
              }
            case a: Any => logger.info(s"Unknown type in facet buckets ${a.getClass}")
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
      case it: Seq[_] =>
        val dataType = it.head match {
          case _ : BigDecimal => DataTypes.createArrayType(DoubleType)
          case _: BigInt => DataTypes.createArrayType(LongType)
          case _: java.lang.Double => DataTypes.createArrayType(DoubleType)
          case _: String => DataTypes.createArrayType(StringType)
          case _: Any => throw new Exception(s"Non supported data type : ${a.getClass}")
        }
        val values = ArrayBuffer.empty[Any]
        it.iterator.foreach {
          case bd : BigDecimal => values.+=(bd.toDouble)
          case bi: BigInt => values.+=(bi.toLong)
          case jd: java.lang.Double => values.+=(jd.toDouble)
          case s: String => values.+=(s)
          case a: Any => throw new Exception(s"Non supported data type : ${a.getClass}")
        }
        (dataType, mutable.WrappedArray.make(values.toArray))
      case a: Any => throw new Exception(s"Non supported type: ${a.getClass}")
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

  private def formSchema(key: String, head: Map[String, _], schemaBuffer: ArrayBuffer[StructField]) : Array[StructField] = {
    if (head.contains("val")) {
      val dv = getDataTypeAndValue(head("val"))
      schemaBuffer.+=(StructField(key, dv._1))
    }
    if (head.contains("count")) {
      schemaBuffer.+=(StructField(s"${key}_count", DataTypes.LongType))
    }
    for ((k, v) <- head.filter(p => p._1 != "count" && p._1 != "val")) {
      v match {
        case _: java.lang.Number =>
          val dv = getDataTypeAndValue(v)
          schemaBuffer.+=(StructField(s"${k}", dv._1))
        case _ : String =>
          val dv = getDataTypeAndValue(v)
          schemaBuffer.+=(StructField(s"${k}", dv._1))
        case _: Seq[_] =>
          val dv = getDataTypeAndValue(v)
          schemaBuffer.+=(StructField(s"${k}", dv._1))
        case jo : Map[String, _] @unchecked=>
          if (jo.contains("buckets")) {
            jo("buckets") match {
              case ja: List[Map[String, _]] @unchecked => formSchema(k, ja.head, schemaBuffer)
            }
          }
        case a => throw new Exception(s"Non supported type: ${a.getClass}")
      }
    }
    schemaBuffer.toArray
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
      for ((_, v) <- bucket.filter(p => p._1 != "count" && p._1 != "val")) {
        if (buffer.length == schema.fieldNames.length) {
          val row = Row.fromSeq(buffer)
          rows.+=(row)
        }
        v match {
          case _ : java.lang.Number =>
            val dv = getDataTypeAndValue(v)
            buffer.+=(dv._2)
          case _ : String =>
            val dv = getDataTypeAndValue(v)
            buffer.+=(dv._2)
          case _: Seq[_] =>
            val dv = getDataTypeAndValue(v)
            buffer.+=(dv._2)
          case jo : Map[String, _] @unchecked =>
            if (jo.contains("buckets")) {
              jo("buckets") match {
                case ja: List[Map[String, _]] @unchecked => convertFacetBucketsToDataFrame(ja, rows, buffer.toArray, schema)
              }
            }
          case a => throw new Exception(s"Non supported type: ${a.getClass}")
        }
      }
      if (buffer.length == schema.fieldNames.length) {
        val row = Row.fromSeq(buffer)
        rows.+=(row)
      }
    }
    rows.toArray
  }
}
