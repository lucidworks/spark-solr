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

import com.lucidworks.spark.ml.param.{StringStringMapParam, StringStringMapArrayParam}

import collection.JavaConversions._
import org.apache.lucene.analysis.custom.CustomAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}
import org.apache.lucene.util.{Version => LuceneVersion}


/**
 * :: Experimental ::
 * Specify CharFilters, Tokenizer, and Filters to build a custom Lucene analyzer, which
 * transforms the input column string into a sequence of tokens in the output column.
 */
@Experimental
class LuceneAnalyzer(override val uid: String)
  extends UnaryTransformer[String, Seq[String], LuceneAnalyzer] {
  def this() = this(Identifiable.randomUID("LuceneAnalyzer"))
  /**
   * Default Lucene match version, >= LUCENE_4_0_0
   * Used by each analysis component when "luceneMatchVersion" is not specified explicitly for it.
   * Default: LuceneVersion.LATEST
   * @group param
   */
  val defaultLuceneMatchVersion: Param[String] = new Param(this, "defaultLuceneMatchVersion",
    "Default Lucene version compatibility, applied to analysis components with specs that don't"
      + " contain a luceneMatchVersion arg, must be 4.0 or later",
    (version: String) => try {
      LuceneVersion.parseLeniently(version).onOrAfter(LuceneVersion.LUCENE_4_0_0)
      true
    } catch { case e: Throwable => false })
  /** @group setParam */
  def setDefaultLuceneMatchVersion(value: String): this.type = set(defaultLuceneMatchVersion, value)
  /** @group getParam */
  def getDefaultLuceneMatchVersion: String = $(defaultLuceneMatchVersion)
  setDefault(defaultLuceneMatchVersion -> LuceneVersion.LATEST.toString)

  val charFilters : StringStringMapArrayParam = new StringStringMapArrayParam(this, "charFilters",
  "Array of CharFilter config maps: "
    + "(CharFilter type (SPI name) and optional arg key/value pairs)",
  { a => a.map(m => m.contains("type")).foldLeft(true)({ _ & _ }) }) // Each must have "type"
  /** @group setParam */
  def setCharFilters(value: Array[Map[String,String]]): this.type = set(charFilters, value)
  /** @group getParam */
  def getCharFilters: Array[Map[String,String]] = $(charFilters)
  setDefault(charFilters -> Array.empty[Map[String,String]])

  val tokenizer : StringStringMapParam = new StringStringMapParam(this, "tokenizer",
  "Tokenizer config map: Tokenizer type (SPI name) and optional arg key/value pairs",
  { _.contains("type") }) // Must have "type"
  /** @group setParam */
  def setTokenizer(value: Map[String,String]): this.type = set(tokenizer, value)
  /** @group getParam */
  def getTokenizer: Map[String,String] = $(tokenizer)
  setDefault(tokenizer -> Map("type" -> "standard")) // StandardTokenizer is the default

  val filters : StringStringMapArrayParam
  = new StringStringMapArrayParam(this, "filters",
  "Array of Token Filter config maps: "
    + " (Filter type (SPI name) and optional arg key/value pairs)",
  { a => a.map(m => m.contains("type")).foldLeft(true)({ _ & _ }) }) // Each must have "type"
  /** @group setParam */
  def setFilters(value: Array[Map[String,String]]): this.type = set(filters, value)
  /** @group getParam */
  def getFilters: Array[Map[String,String]] = $(filters)
  setDefault(filters -> Array.empty[Map[String,String]])

  override def validateParams(): Unit = {
    super.validateParams()
    buildAnalyzer() // side-effect: building the analyzer will validate all params
  }
  private var analyzer: CustomAnalyzer = null
  def buildAnalyzer(): Unit = {
    var builder = CustomAnalyzer.builder()
      .withDefaultMatchVersion(LuceneVersion.parseLeniently($(defaultLuceneMatchVersion)))
    // Need to pass mutable Maps to the builder methods to enable put("luceneMatchVersion", ...)
    for (charFilter <- $(charFilters)) {
      val charFilterNoType = collection.mutable.Map() ++ (charFilter - "type")
      builder = builder.addCharFilter(charFilter("type"), charFilterNoType)
    }
    val tokenizerNoType = collection.mutable.Map[String,String]() ++ ($(tokenizer) - "type")
    builder = builder.withTokenizer($(tokenizer)("type"), tokenizerNoType)
    for (filter <- $(filters)) {
      val filterNoType = collection.mutable.Map() ++ (filter - "type")
      builder = builder.addTokenFilter(filter("type"), filterNoType)
    }
    analyzer = builder.build()
    builtCharFilters = $(charFilters)
    builtTokenizer = $(tokenizer)
    builtFilters = $(filters)
  }
  private var builtCharFilters: Array[Map[String,String]] = null
  private var builtTokenizer: Map[String,String] = null
  private var builtFilters: Array[Map[String,String]] = null
  def shouldBuildAnalyzer: Boolean = {
    analyzer == null                                 ||
    ! (builtCharFilters sameElements $(charFilters)) ||
    builtTokenizer != $(tokenizer)                   ||
    ! (builtFilters sameElements $(filters))
  }

  override protected def createTransformFunc: String => Seq[String] = { str =>
    if (shouldBuildAnalyzer) buildAnalyzer()
    val stream = analyzer.tokenStream($(inputCol), str)
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
  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }
  override protected def outputDataType: DataType = new ArrayType(StringType, true)
  override def copy(extra: ParamMap): LuceneAnalyzer = defaultCopy(extra)
}
