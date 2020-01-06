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

package com.lucidworks.spark.analysis

import java.io.{Reader, StringReader}

import com.lucidworks.spark.SparkSolrFunSuite
import org.json4s.jackson.JsonMethods._
import org.json4s.JValue

class LuceneTextAnalyzerSuite extends SparkSolrFunSuite {
  import LuceneTextAnalyzerSuite._

  test("StandardTokenizer") {
    val analyzer1 = new LuceneTextAnalyzer(stdTokLowerSchema)
    assertExpectedTokens(analyzer1, "Test for tokenization.", Array("test", "for", "tokenization"))
    assertExpectedTokens(analyzer1, "Te,st. punct", Array("te", "st", "punct"))

    val stdTokMax10Schema =
      """{ "analyzers": [{ "name": "StdTok_max10",
        |                  "tokenizer": { "type": "standard", "maxTokenLength": "10" } }],
        |  "fields": [{ "name": "rawText", "analyzer": "StdTok_max10" }] }""".stripMargin

    val analyzer2 = new LuceneTextAnalyzer(stdTokMax10Schema)
    assertExpectedTokens(analyzer2, "rawText", "我是中国人。 １２３４ Ｔｅｓｔｓ ",
        Array("我", "是", "中", "国", "人", "１２３４", "Ｔｅｓｔｓ"))
    assertExpectedTokens(analyzer2, "rawText", "some-dashed-phrase", Array("some", "dashed", "phrase"))

    val stdTokMax3Schema = """
                             |{
                             |  "defaultLuceneMatchVersion": "7.0.0",
                             |  "analyzers": [{
                             |    "name": "StdTok_max3",
                             |    "tokenizer": {
                             |      "type": "standard",
                             |      "maxTokenLength": "3"
                             |    }
                             |  }],
                             |  "fields": [{
                             |    "regex": ".+",
                             |    "analyzer": "StdTok_max3"
                             |  }]
                             |}""".stripMargin
    val analyzer3 = new LuceneTextAnalyzer(stdTokMax3Schema)
    assertExpectedTokens(analyzer3, "Test for tokenization.",
      Array("Tes", "t", "for", "tok", "eni", "zat", "ion"))
    assertExpectedTokens(analyzer3, "Te,st.  punct", Array("Te", "st", "pun", "ct"))
  }

  test("CharFilters") {
    val schema1 = """
                    |{
                    |  "analyzers": [{
                    |    "name": "strip_alpha_std_tok",
                    |    "charFilters":[{
                    |      "type": "patternreplace",
                    |      "pattern": "[A-Za-z]+",
                    |      "replacement": ""
                    |    }],
                    |    "tokenizer": {
                    |      "type": "standard"
                    |    }
                    |  }],
                    |  "fields": [{
                    |    "regex": ".+",
                    |    "analyzer": "strip_alpha_std_tok"
                    |  }]
                    |}""".stripMargin
    val analyzer1 = new LuceneTextAnalyzer(schema1)
    assertExpectedTokens(analyzer1, "Test for 9983, tokenization.", Array("9983"))
    assertExpectedTokens(analyzer1, "Te,st. punct", Array[String]())

    val schema2 = """
                    |{
                    |  "analyzers": [{
                    |    "name": "htmlstrip_drop_removeme_std_tok",
                    |    "charFilters":[{
                    |        "type": "htmlstrip"
                    |      }, {
                    |        "type": "patternreplace",
                    |        "pattern": "removeme",
                    |        "replacement": ""
                    |    }],
                    |    "tokenizer": {
                    |      "type": "standard"
                    |    }
                    |  }],
                    |  "fields": [{
                    |    "name": "rawText",
                    |    "analyzer": "htmlstrip_drop_removeme_std_tok"
                    |  }]
                    |}""".stripMargin
    val analyzer2 = new LuceneTextAnalyzer(schema2)
    assertExpectedTokens(analyzer2, "rawText",
      "<html><body>remove<b>me</b> but leave<div>the&nbsp;rest.</div></body></html>",
      Array("but", "leave", "the", "rest"))
  }

  test("TokenFilters") {
    val schema = """
                   |{
                   |  "analyzers": [{
                   |    "name": "std_tok_possessive_stop_lower",
                   |    "tokenizer": {
                   |      "type": "standard"
                   |    },
                   |    "filters":[{
                   |        "type": "englishpossessive"
                   |      }, {
                   |        "type": "stop",
                   |        "ignoreCase": "true",
                   |        "format": "snowball",
                   |        "words": "org/apache/lucene/analysis/snowball/english_stop.txt"
                   |      }, {
                   |        "type": "lowercase"
                   |    }]
                   |  }],
                   |  "fields": [{
                   |    "regex": ".+",
                   |    "analyzer": "std_tok_possessive_stop_lower"
                   |  }]
                   |}""".stripMargin
    val analyzer = new LuceneTextAnalyzer(schema)
    assertExpectedTokens(analyzer, "rawText", "Harold's not around.", Array("harold", "around"))
    assertExpectedTokens(analyzer, "The dog's nose KNOWS!", Array("dog", "nose", "knows"))
  }

  test("UAX29URLEmailTokenizer") {
    val schema = """
                   |{
                   |  "analyzers": [{
                   |    "name": "uax29urlemail_2000",
                   |    "tokenizer": {
                   |      "type": "uax29urlemail",
                   |      "maxTokenLength": "2000"
                   |    }
                   |  }],
                   |  "fields": [{
                   |    "regex": ".+",
                   |    "analyzer": "uax29urlemail_2000"
                   |  }]
                   |}""".stripMargin
    val analyzer = new LuceneTextAnalyzer(schema)

    assertExpectedTokens(analyzer, "Click on https://www.google.com/#q=spark+lucene",
      Array("Click", "on", "https://www.google.com/#q=spark+lucene"))
    assertExpectedTokens(analyzer, "Email caffeine@coffee.biz for tips on staying@alert",
      Array("Email", "caffeine@coffee.biz", "for", "tips", "on", "staying", "alert"))
  }

  test("PrebuiltAnalyzer") {
    val schema = """{"fields":[{"regex":".+","analyzer":"org.apache.lucene.analysis.core.WhitespaceAnalyzer"}]}"""
    val analyzer = new LuceneTextAnalyzer(schema)
    assertExpectedTokens(analyzer, "Test for tokenization.", Array("Test", "for", "tokenization."))
    assertExpectedTokens(analyzer, "Te,st. punct", Array("Te,st.", "punct"))
  }

  test("MultivaluedInput") {
    val analyzer = new LuceneTextAnalyzer(stdTokLowerSchema)
    assertExpectedTokens(analyzer, Array("Harold's not around.", "The dog's nose KNOWS!"),
        Array("harold's", "not", "around", "the", "dog's", "nose", "knows"))
  }

  test("MultipleFields") {
    val analyzer1 = new LuceneTextAnalyzer(stdTokLowerSchema)

    assertExpectedTokens(analyzer1,
      Map("one" -> "Harold's not around.",          "two" -> "The dog's nose KNOWS!"),
      Map("one"-> Seq("harold's", "not", "around"), "two" -> Seq("the", "dog's", "nose", "knows")))

    val schema = """
                   |{
                   |  "analyzers": [{
                   |      "name": "std_tok_lower",
                   |      "tokenizer": { "type": "standard" },
                   |      "filters":[{ "type": "lowercase" }]
                   |    }, {
                   |      "name": "std_tok",
                   |      "tokenizer": { "type": "standard" }
                   |    }, {
                   |      "name": "htmlstrip_std_tok_lower",
                   |      "charFilters": [{ "type": "htmlstrip" }],
                   |      "tokenizer": { "type": "standard" },
                   |      "filters": [{ "type": "lowercase" }]
                   |  }],
                   |  "fields": [{
                   |      "name": "rawText1",
                   |      "analyzer": "std_tok_lower"
                   |    }, {
                   |      "name": "rawText2",
                   |      "analyzer": "std_tok"
                   |    }, {
                   |      "regex": ".+",
                   |      "analyzer": "htmlstrip_std_tok_lower"
                   |  }]
                   |}""".stripMargin
    val analyzer2 = new LuceneTextAnalyzer(schema)
    assertExpectedTokens(analyzer2,
      Map("rawText1"->"Harold's NOT around.",           "rawText2"->"The dog's nose KNOWS!"),
      Map("rawText1"->Seq("harold's", "not", "around"), "rawText2"->Seq("The", "dog's", "nose", "KNOWS")))

    assertExpectedTokensMV(analyzer2,
      Map("rawText1"->Seq("Harold's NOT around."), "rawText2"->Seq("The dog's nose KNOWS!", "Good, fine, great...")),
      Map("rawText1"->Seq("harold's", "not", "around"),
          "rawText2"->Seq("The", "dog's", "nose", "KNOWS", "Good", "fine", "great")))

    assertExpectedTokensMV(analyzer2,
      Map("rawText1"->Seq("Harold's NOT around.", "Anymore, I mean."),
          "rawText2"->Seq("The dog's nose KNOWS!", "Good, fine, great...")),
      Map("rawText1"->Seq("harold's", "not", "around", "anymore", "i", "mean"),
          "rawText2"->Seq("The", "dog's", "nose", "KNOWS", "Good", "fine", "great")))

    assertExpectedTokens(analyzer2,
      Map("rawText1"->"Harold's NOT around.",
          "rawText2"->"The dog's nose KNOWS!",
          "rawText3"->"<html><body>Content</body></html>"),
      Map("rawText1"->Seq("harold's", "not", "around"),
          "rawText2"->Seq("The", "dog's", "nose", "KNOWS"),
          "rawText3"->Seq("content")))
  }

  test("MissingValues") {
    val analyzer = new LuceneTextAnalyzer(stdTokLowerSchema)
    assertExpectedTokens(analyzer, null.asInstanceOf[String], Array[String]())
    assertExpectedTokens(analyzer, "", Array[String]())
    assertExpectedTokens(analyzer, Array(null, "Harold's not around.", null, "The dog's nose KNOWS!", ""),
      Array("harold's", "not", "around", "the", "dog's", "nose", "knows"))
    assertExpectedTokens(analyzer,
      Map("rawText1"->"", "rawText2"->"The dog's nose KNOWS!", "rawText3"->null),
      Map("rawText1"->Seq(), "rawText2"->Seq("the", "dog's", "nose", "knows"), "rawText3"->Seq()))
  }

  test("analyze Reader") {
    val analyzer1 = new LuceneTextAnalyzer(stdTokLowerSchema)
    assertExpectedTokens(analyzer1, new StringReader("Test for tokenization."), Array("test", "for", "tokenization"))
    assertExpectedTokens(analyzer1, new StringReader("Te,st. punct"), Array("te", "st", "punct"))

    val stdTokMax3Schema = """
                             |{
                             |  "defaultLuceneMatchVersion": "7.0.0",
                             |  "analyzers": [{
                             |    "name": "StdTok_max3",
                             |    "tokenizer": {
                             |      "type": "standard",
                             |      "maxTokenLength": "3"
                             |    }
                             |  }],
                             |  "fields": [{
                             |    "regex": ".+",
                             |    "analyzer": "StdTok_max3"
                             |  }]
                             |}""".stripMargin
    val analyzer2 = new LuceneTextAnalyzer(stdTokMax3Schema)
    assertExpectedTokens(analyzer2, new StringReader("Test for tokenization."),
      Array("Tes", "t", "for", "tok", "eni", "zat", "ion"))
    assertExpectedTokens(analyzer2, new StringReader("Te,st.  punct"), Array("Te", "st", "pun", "ct"))
  }

  test("Pre-analyzed JSON") {
    val text = "Test for tokenization."
    def getParsedJson(text: Option[String]) = {
      parse(s"""{"v":"1", ${if (text.isDefined) s""""str":"${text.get}",""" else ""}
               |"tokens":[{"t":"test",         "s":0, "e":4,  "i":1},
               |          {"t":"for",          "s":5, "e":8,  "i":1},
               |          {"t":"tokenization", "s":9, "e":21, "i":1}]}""".stripMargin)
    }
    val analyzer = new LuceneTextAnalyzer(stdTokLowerSchema)
    assertExpectedJson(analyzer, "dummy", text, stored = true, getParsedJson(Some(text)))
    assertExpectedJson(analyzer, "dummy", text, stored = false, getParsedJson(None))
    assertExpectedJson(analyzer, "dummy", new StringReader(text), stored = true, getParsedJson(Some(text)))
    assertExpectedJson(analyzer, "dummy", new StringReader(text), stored = false, getParsedJson(None))
  }
}
object LuceneTextAnalyzerSuite extends SparkSolrFunSuite {
  val stdTokLowerSchema =
    """{ "analyzers": [{ "name": "StdTokLower",
      |                  "tokenizer": { "type": "standard" },
      |                  "filters": [{ "type": "lowercase" }] }],
      |  "fields": [{ "regex": ".+", "analyzer": "StdTokLower" }] }""".stripMargin

  def assertExpectedTokens(analyzer: LuceneTextAnalyzer, in: String, expected: Array[String]): Unit = {
    assertExpectedTokens(analyzer, "dummy", in, expected)
  }
  def assertExpectedTokens(analyzer: LuceneTextAnalyzer, field: String,
                           in: String, expected: Array[String]): Unit = {
    val output = analyzer.analyze(field, in)
    assert(output === expected)
  }
  def assertExpectedTokens(analyzer: LuceneTextAnalyzer, reader: Reader, expected: Array[String]): Unit = {
    assertExpectedTokens(analyzer, "dummy", reader, expected)
  }
  def assertExpectedTokens(analyzer: LuceneTextAnalyzer, field: String,
                           reader: Reader, expected: Array[String]): Unit = {
    val output = analyzer.analyze(field, reader)
    assert(output === expected)
  }
  def assertExpectedTokens(analyzer: LuceneTextAnalyzer, in: Array[String], expected: Array[String]): Unit = {
    assertExpectedTokens(analyzer, "dummy", in, expected)
  }
  def assertExpectedTokens(analyzer: LuceneTextAnalyzer, field: String,
                           in: Array[String], expected: Array[String]): Unit = {
    val output = analyzer.analyzeMV(field, in)
    assert(output === expected)
  }
  def assertExpectedTokens(analyzer: LuceneTextAnalyzer, fieldValues: Map[String,String],
                           expected: Map[String,Seq[String]]): Unit = {
    val output = analyzer.analyze(fieldValues)
    assert(output === expected)
  }
  def assertExpectedTokensMV(analyzer: LuceneTextAnalyzer, fieldValues: Map[String,Seq[String]],
                             expected: Map[String,Seq[String]]): Unit = {
    val output = analyzer.analyzeMV(fieldValues)
    assert(output === expected)
  }
  def assertExpectedJson
  (analyzer: LuceneTextAnalyzer, field: String, in: String, stored: Boolean, expected: JValue): Unit = {
    val output = parse(analyzer.toPreAnalyzedJson(field, in, stored))
    assert(output === expected)
  }
  def assertExpectedJson
  (analyzer: LuceneTextAnalyzer, field: String, reader: Reader, stored: Boolean, expected: JValue): Unit = {
    val output = parse(analyzer.toPreAnalyzedJson(field, reader, stored))
    assert(output === expected)
  }
}
