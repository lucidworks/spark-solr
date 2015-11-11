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

import com.lucidworks.spark.SparkSolrFunSuite
import org.apache.spark.ml.feature.TokenizerTestData
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{Row, DataFrame}

class LuceneAnalyzerSuite extends SparkSolrFunSuite with MLlibTestSparkContext {
  import com.lucidworks.spark.ml.feature.LuceneAnalyzerSuite._

  test("params") {
    ParamsSuite.checkParams(new LuceneAnalyzer)
  }

  test("StandardTokenizer") {
    val analyzer0 = new LuceneAnalyzer()
      .setInputCol("rawText")
      .setOutputCol("tokens")
    val dataset0 = sqlContext.createDataFrame(Seq(
      TokenizerTestData("Test for tokenization.", Array("Test", "for", "tokenization")),
      TokenizerTestData("Te,st. punct", Array("Te", "st", "punct"))
    ))
    testLuceneAnalyzer(analyzer0, dataset0)

    val dataset1 = sqlContext.createDataFrame(Seq(
      TokenizerTestData("我是中国人。 １２３４ Ｔｅｓｔｓ ",
        Array("我", "是", "中", "国", "人", "１２３４", "Ｔｅｓｔｓ")),
      TokenizerTestData("some-dashed-phrase", Array("some", "dashed", "phrase"))
    ))
    analyzer0.setTokenizer(Map("type" -> "standard", "maxTokenLength" -> "10"))
    testLuceneAnalyzer(analyzer0, dataset1)

    val analyzer2 = new LuceneAnalyzer()
      .setDefaultLuceneMatchVersion("4.10.4")
      .setTokenizer(Map("type" -> "standard", "maxTokenLength" -> "3"))
      .setInputCol("rawText")
      .setOutputCol("tokens")
    val dataset2 = sqlContext.createDataFrame(Seq(
      TokenizerTestData("Test for tokenization.",
        Array("Tes", "t", "for", "tok", "eni", "zat", "ion")),
      TokenizerTestData("Te,st.  punct", Array("Te", "st", "pun", "ct"))
    ))
    testLuceneAnalyzer(analyzer2, dataset2)
  }

  test("CharFilters") {
    val analyzer0 = new LuceneAnalyzer()
      .setCharFilters(Array(Map("type" -> "patternreplace",
        "pattern" -> "[A-Za-z]+", "replacement" -> "")))
      .setTokenizer(Map("type" -> "standard"))
      .setInputCol("rawText")
      .setOutputCol("tokens")
    val dataset0 = sqlContext.createDataFrame(Seq(
      TokenizerTestData("Test for 9983, tokenization.", Array("9983")),
      TokenizerTestData("Te,st. punct", Array())
    ))
    testLuceneAnalyzer(analyzer0, dataset0)

    analyzer0.setCharFilters(Array(Map("type" -> "htmlstrip"),
      Map("type" -> "patternreplace", "pattern" -> "removeme", "replacement" -> "")))
    val dataset1 = sqlContext.createDataFrame(Seq(
      TokenizerTestData("<html><body>remove<b>me</b> but leave<div>the&nbsp;rest.</div></body></html>",
        Array("but", "leave", "the", "rest"))
    ))
    testLuceneAnalyzer(analyzer0, dataset1)
  }

  test("TokenFilters") {
    val analyzer0 = new LuceneAnalyzer()
      .setTokenizer(Map("type" -> "standard"))
      .setFilters(Array(Map("type" -> "englishpossessive"),
        Map("type" -> "stop", "ignoreCase" -> "true", "format" -> "snowball",
          "words" -> "org/apache/lucene/analysis/snowball/english_stop.txt"),
        Map("type" -> "lowercase")))
      .setInputCol("rawText")
      .setOutputCol("tokens")
    val dataset0 = sqlContext.createDataFrame(Seq(
      TokenizerTestData("Harold's not around.", Array("harold", "around")),
      TokenizerTestData("The dog's nose KNOWS!", Array("dog", "nose", "knows"))
    ))
    testLuceneAnalyzer(analyzer0, dataset0)
  }

  test("UAX29URLEmailTokenizer") {
    val analyzer0 = new LuceneAnalyzer()
      .setTokenizer(Map("type" -> "uax29urlemail", "maxTokenLength" -> "2000"))
      .setInputCol("rawText")
      .setOutputCol("tokens")
    val dataset0 = sqlContext.createDataFrame(Seq(
      TokenizerTestData("Click on https://www.google.com/#q=spark+lucene",
        Array("Click", "on", "https://www.google.com/#q=spark+lucene")),
      TokenizerTestData("Email caffeine@coffee.biz for tips on staying@alert",
        Array("Email", "caffeine@coffee.biz", "for", "tips", "on", "staying", "alert"))
    ))
    testLuceneAnalyzer(analyzer0, dataset0)
  }
}

object LuceneAnalyzerSuite extends SparkSolrFunSuite {

  def testLuceneAnalyzer(t: LuceneAnalyzer, dataset: DataFrame): Unit = {
    t.transform(dataset)
      .select("tokens", "wantedTokens")
      .collect()
      .foreach { case Row(tokens, wantedTokens) =>
        assert(tokens === wantedTokens)
      }
  }
}
