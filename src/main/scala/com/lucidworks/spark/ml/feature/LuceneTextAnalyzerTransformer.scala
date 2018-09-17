/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lucidworks.spark.ml.feature

import java.io.{PrintWriter, StringWriter}

import com.lucidworks.spark.LazyLogging
import com.lucidworks.spark.analysis.LuceneTextAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.param.{Param, _}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{HasInputColsTransformer, TransformerParamsReader}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructType, _}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.util.control.NonFatal


/**
 * Specify an analysis schema as a JSON string to build a custom Lucene analyzer, which
 * transforms the input column(s) into a sequence of tokens in the output column.
 * See [[LuceneTextAnalyzer]] for a description of the schema format.
 */
@Experimental
class LuceneTextAnalyzerTransformer(override val uid: String) extends HasInputColsTransformer with LazyLogging with MLWritable {
  def this() = this(Identifiable.randomUID("LuceneAnalyzer"))

  /** @group setParam */
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)
  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)
  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCols, Array(value))

  val prefixTokensWithInputCol: BooleanParam = new BooleanParam(this, "prefixTokensWithInputCol",
    s"If true, the input column name will be prepended to every output token, separated by "
    + s""""${LuceneTextAnalyzerTransformer.OutputTokenSeparator}"; default: false.""")
  /** @group setParam */
  def setPrefixTokensWithInputCol(value: Boolean): this.type = set(prefixTokensWithInputCol, value)
  /** @group getParam */
  def getPrefixTokensWithInputCol: Boolean = $(prefixTokensWithInputCol)
  setDefault(prefixTokensWithInputCol -> false)

  val analysisSchema: Param[String] = new Param(this, "analysisSchema",
    "JSON analysis schema: Analyzers (named analysis pipelines: charFilters, tokenizer, filters)"
      + " and field (input column) -> analyzer mappings",
    validateAnalysisSchema _)
  /** @group setParam */
  def setAnalysisSchema(value: String): this.type = set(analysisSchema, value)
  /** @group getParam */
  def getAnalysisSchema: String = $(analysisSchema)
  setDefault(analysisSchema -> s"""
                                  |{
                                  |  "analyzers": [{
                                  |    "name": "StdTok_LowerCase",
                                  |    "charFilters": [],
                                  |    "tokenizer": {
                                  |      "type": "standard"
                                  |    },
                                  |    "filters": [{
                                  |      "type": "lowercase"
                                  |    }]
                                  |  }],
                                  |  "fields": [{
                                  |    "regex": ".+",
                                  |    "analyzer": "StdTok_LowerCase"
                                  |  }]
                                  |}""".stripMargin)
  @transient var analyzerInitFailure: Option[String] = None
  def validateAnalysisSchema(analysisSchema: String): Boolean = {
    analyzer = None
    analyzerInitFailure = None
    try {
      analyzer = Some(new LuceneTextAnalyzer(analysisSchema))
    } catch {
      case NonFatal(e) => val writer = new StringWriter
        writer.write("Exception initializing analysis schema: ")
        e.printStackTrace(new PrintWriter(writer))
        analyzerInitFailure = Some(writer.toString)
    } finally {
      analyzerInitFailure.foreach(logError(_))
      if ( ! analyzer.exists(_.isValid)) {
        analyzer.foreach(a => logError(a.invalidMessages))
      }
    }
    analyzer.exists(_.isValid)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateParams()
    val fieldNames = schema.fieldNames.toSet
    $(inputCols).foreach { colName =>
      if (fieldNames.contains(colName)) {
        schema(colName).dataType match {
          case StringType | ArrayType(StringType, _) =>
          case other => throw new IllegalArgumentException(
            s"Input column $colName : data type $other is not supported.")
        }
      }
    }
    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${$(outputCol)} already exists.")
    }
    StructType(schema.fields :+ new StructField($(outputCol), outputDataType, nullable = false))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val schema = dataset.schema
    val existingInputCols = $(inputCols).filter(schema.fieldNames.contains(_))
    if (analyzer == null || (analyzer.isEmpty && analyzerInitFailure.isEmpty)) {
      validateAnalysisSchema($(analysisSchema)) // make sure analyzer has been instantiated
    }
    val analysisFunc = udf { row: Row =>
      if (analyzer == null || (analyzer.isEmpty && analyzerInitFailure.isEmpty)) {
        validateAnalysisSchema($(analysisSchema)) // make sure analyzer has been instantiated
      }
      val seqBuilder = Seq.newBuilder[String]
      val colNameIter = existingInputCols.iterator
      row.toSeq foreach { column: Any =>
        val field = colNameIter.next()
        column match {
          case null => // skip missing values
          case value: String => seqBuilder ++= analyze(field, value)
          case values: Seq[String @unchecked] =>
            values.foreach { case null => // skip missing values
              case value => seqBuilder ++= analyze(field, value)
            }
        }
      }
      seqBuilder.result()
    }
    val args = existingInputCols.map(dataset(_))
    val outputSchema = transformSchema(schema)
    val metadata = outputSchema($(outputCol)).metadata
    val resultDF = dataset.select(col("*"), analysisFunc(struct(args: _*)).as($(outputCol), metadata))
    resultDF
  }

  def validateParams(): Unit = {
    if (analyzer.exists(_.isValid)) {
      buildReferencedAnalyzers()
    }
    require(analyzer.exists(_.isValid),
      analyzer.map(_.invalidMessages).getOrElse(analyzerInitFailure.getOrElse(s"Invalid analyzer ${analyzer}")))
  }

  private def buildReferencedAnalyzers(): Unit = {
    $(inputCols) foreach { inputCol =>
      require(analyzer.get.getFieldAnalyzer(inputCol).isDefined,
        s"Input column '$inputCol': no matching inputColumn name or regex in analysis schema.")
    }
  }
  override def copy(extra: ParamMap): LuceneTextAnalyzerTransformer = defaultCopy(extra)
  def outputDataType: DataType = new ArrayType(StringType, true)

  @transient private var analyzer: Option[LuceneTextAnalyzer] = None
  private def analyze(colName: String, str: String): Seq[String] = {
    if ($(prefixTokensWithInputCol)) {
      val inputStream = analyzer.get.tokenStream(colName, str)
      val charTermAttr = inputStream.addAttribute(classOf[CharTermAttribute])
      inputStream.reset()
      val outputBuilder = Seq.newBuilder[String]
      val tokenBuilder = new StringBuilder(colName + LuceneTextAnalyzerTransformer.OutputTokenSeparator)
      val prefixLength = tokenBuilder.length
      while (inputStream.incrementToken) {
        tokenBuilder.setLength(prefixLength)
        tokenBuilder.appendAll(charTermAttr.buffer(), 0, charTermAttr.length())
        outputBuilder += tokenBuilder.toString
      }
      inputStream.end()
      inputStream.close()
      outputBuilder.result()
    } else {
      analyzer.get.analyze(colName, str)
    }
  }
}

object LuceneTextAnalyzerTransformer extends MLReadable[LuceneTextAnalyzerTransformer] {
  /** Used to separate the input column name and the token when prefixTokensWithInputCol = true */
  val OutputTokenSeparator = "="

  override def read: MLReader[LuceneTextAnalyzerTransformer] = new TransformerParamsReader
}

