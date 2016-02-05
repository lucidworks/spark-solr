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

import com.lucidworks.spark.util.Utils
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.{Analyzer, DelegatingAnalyzerWrapper}
import org.apache.lucene.analysis.custom.CustomAnalyzer
import org.apache.spark.Logging
import org.apache.spark.ml.HasInputColsTransformer
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCol}
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._
import org.apache.lucene.util.{Version => LuceneVersion}
import org.json4s.jackson.JsonMethods.parse

import java.io.{PrintWriter, StringWriter}
import java.util.regex.Pattern

import scala.collection.mutable
import scala.util.control.Breaks._
import scala.collection.JavaConversions._
import scala.util.control.NonFatal


/**
  * :: Experimental ::
  * Specify a schema as a JSON string to build a custom Lucene analyzer, which
  * transforms the input column(s) into a sequence of tokens in the output column.
  */
@Experimental
class LuceneAnalyzer(override val uid: String)
  extends HasInputColsTransformer with Logging {
  def this() = this(Identifiable.randomUID("LuceneAnalyzer"))

  /** @group setParam */
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)
  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)
  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCols, Array(value))

  val analysisSchema: Param[String] = new Param(this, "analysisSchema",
    "JSON analysis schema: Analyzers (named analysis pipelines: charFilters, tokenizer, filters)"
      + " and input column -> analyzer mappings",
    validateAnalysisSchema _)
  /** @group setParam */
  def setAnalysisSchema(value: String): this.type = set(analysisSchema, value)
  /** @group getParam */
  def getAnalysisSchema: String = $(analysisSchema)
  setDefault(analysisSchema -> s"""
                                  |{
                                  |  "schemaType": "${LuceneAnalyzerSchema.SchemaType_V1}",
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
                                  |  "inputColumns": [{
                                  |    "regex": ".+",
                                  |    "analyzer": "StdTok_LowerCase"
                                  |  }]
                                  |}""".stripMargin)
  @transient var analyzerInitFailure: Option[String] = None
  def validateAnalysisSchema(analysisSchema: String): Boolean = {
    analyzer = None
    analyzerInitFailure = None
    try {
      analyzer = Some(new SchemaAnalyzer(analysisSchema))
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
    $(inputCols).foreach { colName =>
      schema(colName).dataType match {
        case StringType | ArrayType(StringType, _) =>
        case other => throw new IllegalArgumentException(
          s"Input column $colName : data type $other is not supported.")
      }
    }
    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${$(outputCol)} already exists.")
    }
    StructType(schema.fields :+ new StructField($(outputCol), outputDataType, nullable = false))
  }
  override def transform(dataset: DataFrame): DataFrame = {
    val schema = dataset.schema
    val existingInputCols = $(inputCols).filter(schema.fieldNames.contains(_))
    val outputSchema = transformSchema(schema)
    val analysisFunc = udf { row: Row =>
      if (analyzer == null || (analyzer.isEmpty && analyzerInitFailure.isEmpty)) {
        validateAnalysisSchema($(analysisSchema)) // make sure analyzer has been instantiated
      }
      val seqBuilder = Seq.newBuilder[String]
      val colNameIter = existingInputCols.iterator
      row.toSeq foreach { column: Any =>
        val field = colNameIter.next()
        column match {
          case value: String => seqBuilder ++= analyze(field, value)
          case values: Seq[String @unchecked] =>
            values.foreach { value => seqBuilder ++= analyze(field, value) }
        }
      }
      seqBuilder.result()
    }
    val args = existingInputCols.map(dataset(_))
    val metadata = outputSchema($(outputCol)).metadata
    dataset.select(col("*"), analysisFunc(struct(args: _*)).as($(outputCol), metadata))
  }
  override def validateParams(): Unit = {
    super.validateParams()
    if (analyzer.exists(_.isValid)) {
      buildReferencedAnalyzers()
    }
    require(analyzer.exists(_.isValid),
      analyzer.map(_.invalidMessages).getOrElse(analyzerInitFailure.get))
  }
  private def buildReferencedAnalyzers(): Unit = {
    $(inputCols) foreach { inputCol =>
      require(analyzer.get.luceneAnalyzerSchema.getAnalyzer(inputCol).isDefined,
        s"Input column '$inputCol': no matching inputColumn name or regex in analysis schema.")
    }
  }
  override def copy(extra: ParamMap): LuceneAnalyzer = defaultCopy(extra)
  def outputDataType: DataType = new ArrayType(StringType, true)

  @transient private var analyzer: Option[SchemaAnalyzer] = None
  private def analyze(colName: String, str: String): Seq[String] = {
    val stream = analyzer.get.tokenStream(colName, str)
    val charTermAttr = stream.addAttribute(classOf[CharTermAttribute])
    stream.reset()
    val builder = Seq.newBuilder[String]
    while (stream.incrementToken()) {
      builder += charTermAttr.toString
    }
    stream.end()
    stream.close()
    builder.result()
  }
}

private case class AnalyzerConfig(name: String,
                                  charFilters: Option[List[Map[String, String]]],
                                  tokenizer: Map[String, String],
                                  filters: Option[List[Map[String, String]]])
private case class InputColumnConfig(regex: Option[String],
                                     name: Option[String],
                                     analyzer: String) {
  val pattern: Pattern = regex.map(_.r.pattern).orNull
  val columnId: String = name.getOrElse(regex.get)
}
private case class SchemaConfig(schemaType: String,
                                defaultLuceneMatchVersion: Option[String],
                                analyzers: List[AnalyzerConfig],
                                inputColumns: List[InputColumnConfig]) {
  val namedAnalyzerConfigs: Map[String, AnalyzerConfig] = analyzers.map(a => a.name -> a).toMap
  val namedInputColumns: Map[String, InputColumnConfig]
  = inputColumns.filter(c => c.name.isDefined).map(c => c.name.get -> c).toMap
}
private object LuceneAnalyzerSchema {
  val SchemaType_V1 = "LuceneAnalyzerSchema.v1"
}
private class LuceneAnalyzerSchema(val analysisSchema: String) {
  implicit val formats = org.json4s.DefaultFormats
  val schemaConfig = parse(analysisSchema).extract[SchemaConfig]
  val analyzers = mutable.Map[String, Analyzer]()

  // Validate the analysis schema
  var isValid : Boolean = true
  var invalidMessages : StringBuilder = new StringBuilder()
  if (schemaConfig.schemaType != LuceneAnalyzerSchema.SchemaType_V1) {
    isValid = false
    invalidMessages.append(s"""Unknown schemaType "${schemaConfig.schemaType}".""")
  }
  try {
    schemaConfig.defaultLuceneMatchVersion.foreach { version =>
      if ( ! LuceneVersion.parseLeniently(version).onOrAfter(LuceneVersion.LUCENE_4_0_0)) {
        isValid = false
        invalidMessages.append(
          s"""defaultLuceneMatchVersion "${schemaConfig.defaultLuceneMatchVersion}"""")
          .append(" is not on or after ").append(LuceneVersion.LUCENE_4_0_0)
      }
    }
  } catch {
    case NonFatal(e) => isValid = false
      invalidMessages.append(e.getMessage).append("\n")
  }
  schemaConfig.inputColumns.foreach { inputColumn: InputColumnConfig =>
    if (inputColumn.name.isDefined) {
      if (inputColumn.regex.isDefined) {
        isValid = false
        invalidMessages.append("""Both "name" and "regex" keys are defined in an inputColumn,"""
          + " but only one may be.\n")
      }
    } else if (inputColumn.regex.isEmpty) {
      isValid = false
      invalidMessages.append("""Neither "name" nor "regex" key is defined in an inputColumn,""").
        append(" but one must be.\n")
    }
    if (schemaConfig.namedAnalyzerConfigs.get(inputColumn.analyzer).isEmpty) {
      def badAnalyzerMessage(suffix: String): Unit = {
        invalidMessages.append(s"""inputColumn "${inputColumn.columnId}": """)
          .append(s""" analyzer "${inputColumn.analyzer}" """).append(suffix)
      }
      try { // Attempt to interpret the analyzer as a fully qualified class name
        Utils.classForName(inputColumn.analyzer).asInstanceOf[Class[_ <: Analyzer]]
      } catch {
        case _: ClassNotFoundException => isValid = false
          badAnalyzerMessage("not found.\n")
        case _: ClassCastException => isValid = false
          badAnalyzerMessage("is not a subclass of org.apache.lucene.analysis.Analyzer")
      }
    }
  }
  def getAnalyzer(fieldName: String): Option[Analyzer] = {
    var analyzer: Option[Analyzer] = None
    if (isValid) {
      var inputColumnConfig = schemaConfig.namedInputColumns.get(fieldName)
      if (inputColumnConfig.isEmpty) {
        breakable {
          schemaConfig.inputColumns.filter(c => c.regex.isDefined).foreach { column =>
            if (column.pattern matcher fieldName matches()) {
              inputColumnConfig = Some(column)
              break
            }
          }
        }
      }
      if (inputColumnConfig.isDefined) {
        val analyzerConfig = schemaConfig.namedAnalyzerConfigs.get(inputColumnConfig.get.analyzer)
        if (analyzerConfig.isDefined) {
          analyzer = analyzers.get(analyzerConfig.get.name)
          if (analyzer.isEmpty) try {
            analyzer = Some(buildAnalyzer(analyzerConfig.get))
            analyzers.put(analyzerConfig.get.name, analyzer.get)
          } catch {
            case NonFatal(e) => isValid = false
              val writer = new StringWriter
              writer.write(s"Exception initializing analyzer '${analyzerConfig.get.name}': ")
              e.printStackTrace(new PrintWriter(writer))
              invalidMessages.append(writer.toString).append("\n")
          }
        } else {
          try {
            val clazz = Utils.classForName(inputColumnConfig.get.analyzer)
            analyzer = Some(clazz.newInstance.asInstanceOf[Analyzer])
            schemaConfig.defaultLuceneMatchVersion foreach { version =>
              analyzer.get.setVersion(LuceneVersion.parseLeniently(version))
            }
          } catch {
            case NonFatal(e) => isValid = false
              val writer = new StringWriter
              writer.write(s"Exception initializing analyzer '${inputColumnConfig.get.analyzer}': ")
              e.printStackTrace(new PrintWriter(writer))
              invalidMessages.append(writer.toString).append("\n")
          }
        }
      }
    }
    analyzer
  }
  private def buildAnalyzer(analyzerConfig: AnalyzerConfig): Analyzer = {
    var builder = CustomAnalyzer.builder()
    if (schemaConfig.defaultLuceneMatchVersion.isDefined) {
      builder = builder.withDefaultMatchVersion(
        LuceneVersion.parseLeniently(schemaConfig.defaultLuceneMatchVersion.get))
    }
    // Builder methods' param maps must be mutable to enable put("luceneMatchVersion", ...)
    if (analyzerConfig.charFilters.isDefined) {
      for (charFilter <- analyzerConfig.charFilters.get) {
        val charFilterNoType = mutable.Map[String, String]() ++ (charFilter - "type")
        builder = builder.addCharFilter(charFilter("type"), charFilterNoType)
      }
    }
    val tokenizerNoType = mutable.Map[String, String]() ++ (analyzerConfig.tokenizer - "type")
    builder = builder.withTokenizer(analyzerConfig.tokenizer("type"), tokenizerNoType)
    if (analyzerConfig.filters.isDefined) {
      for (filter <- analyzerConfig.filters.get) {
        val filterNoType = mutable.Map[String, String]() ++ (filter - "type")
        builder = builder.addTokenFilter(filter("type"), filterNoType)
      }
    }
    builder.build()
  }
}

private class SchemaAnalyzer (analysisSchema: String)
  extends DelegatingAnalyzerWrapper(Analyzer.PER_FIELD_REUSE_STRATEGY) {

  val luceneAnalyzerSchema = new LuceneAnalyzerSchema(analysisSchema)
  def isValid: Boolean = luceneAnalyzerSchema.isValid
  def invalidMessages: String = luceneAnalyzerSchema.invalidMessages.result()
  val analyzerCache = mutable.Map.empty[String, Analyzer]

  override protected def getWrappedAnalyzer(fieldName: String): Analyzer = {
    analyzerCache.synchronized {
      var analyzer = analyzerCache.get(fieldName)
      if (analyzer.isEmpty) {
        if (isValid) {
          analyzer = luceneAnalyzerSchema.getAnalyzer(fieldName)
          if (! isValid) {
            throw new IllegalArgumentException(invalidMessages)
          }
        } else {
          throw new IllegalArgumentException(invalidMessages)
        }
        analyzerCache.put(fieldName, analyzer.get)
      }
      analyzer.get
    }
  }
}
