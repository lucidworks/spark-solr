package com.lucidworks.spark.ml.feature;

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

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.TokenizerTestData;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import scala.collection.JavaConversions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JavaLuceneAnalyzerTest {
  private transient JavaSparkContext jsc;
  private transient SQLContext jsql;

  @Before
  public void setupJavaSparkContext() {
    SparkConf conf = new SparkConf()
        .setMaster("local")
        .setAppName("JavaLuceneAnalyzerTest")
        .set("spark.default.parallelism", "1");
    jsc = new JavaSparkContext(conf);
    jsql = new SQLContext(jsc);
  }

  @After
  public void stopSparkContext() {
    jsc.stop();
    jsc = null;
    jsql = null;
  }

  @Test
  public void testStandardTokenizer() {
    LuceneAnalyzer analyzer0 = new LuceneAnalyzer()
        .setInputCol("rawText")
        .setOutputCol("tokens"); // Default: StandardTokenizer

    assertExpectedTokens(analyzer0, Lists.newArrayList(
        new TokenizerTestData("Test for tokenization.", new String[]{"Test", "for", "tokenization"}),
        new TokenizerTestData("Te,st. punct", new String[]{"Te", "st", "punct"})));

    assertExpectedTokens(analyzer0, Lists.newArrayList(
        new TokenizerTestData("Test for tokenization.", new String[]{"Test", "for", "tokenization"}),
        new TokenizerTestData("Te,st. punct", new String[]{"Te", "st", "punct"})));

    Map<String,String> tokenizerSpecification = new HashMap<>();
    tokenizerSpecification.put("type", "standard");
    tokenizerSpecification.put("maxTokenLength", "10");
    analyzer0.setTokenizer(asScalaImmutableMap(tokenizerSpecification));
    assertExpectedTokens(analyzer0, Lists.newArrayList(
        new TokenizerTestData("我是中国人。 １２３４ Ｔｅｓｔｓ ",
            new String[]{"我", "是", "中", "国", "人", "１２３４", "Ｔｅｓｔｓ"}),
        new TokenizerTestData("some-dashed-phrase", new String[]{"some", "dashed", "phrase"})));

    tokenizerSpecification.clear();
    tokenizerSpecification.put("type", "standard");
    tokenizerSpecification.put("maxTokenLength", "3");
    LuceneAnalyzer analyzer1 = new LuceneAnalyzer()
        .setDefaultLuceneMatchVersion("4.10.4")
        .setTokenizer(asScalaImmutableMap(tokenizerSpecification))
        .setInputCol("rawText")
        .setOutputCol("tokens");
    assertExpectedTokens(analyzer1, Lists.newArrayList(
        new TokenizerTestData("Test for tokenization.",
            new String[]{"Tes", "t", "for", "tok", "eni", "zat", "ion"}),
        new TokenizerTestData("Te,st.  punct", new String[]{"Te", "st", "pun", "ct"})));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCharFilters() {
    Map<String,String> charFilterSpecification0 = new HashMap<>();
    charFilterSpecification0.put("type", "patternreplace");
    charFilterSpecification0.put("pattern", "[A-Za-z]+");
    charFilterSpecification0.put("replacement", "");

    Map<String,String> tokenizerSpecification = new HashMap<>();
    tokenizerSpecification.put("type", "standard");

    LuceneAnalyzer analyzer = new LuceneAnalyzer()
        .setCharFilters(new scala.collection.immutable.Map[]
            { asScalaImmutableMap(charFilterSpecification0) })
        .setTokenizer(asScalaImmutableMap(tokenizerSpecification))
        .setInputCol("rawText")
        .setOutputCol("tokens");

    assertExpectedTokens(analyzer, Lists.newArrayList(
        new TokenizerTestData("Test for 9983, tokenization.", new String[]{"9983"}),
        new TokenizerTestData("Te,st. punct", new String[]{})));

    Map<String,String> charFilterSpecification1 = new HashMap<>();
    charFilterSpecification1.put("type", "htmlstrip");

    Map<String,String> charFilterSpecification2 = new HashMap<>();
    charFilterSpecification2.put("type", "patternreplace");
    charFilterSpecification2.put("pattern", "removeme");
    charFilterSpecification2.put("replacement", "");

    analyzer.setCharFilters(new scala.collection.immutable.Map[]
        {asScalaImmutableMap(charFilterSpecification1),
            asScalaImmutableMap(charFilterSpecification2)});

    assertExpectedTokens(analyzer, Lists.newArrayList(
        new TokenizerTestData("<html><body>remove<b>me</b> but leave<div>the&nbsp;rest.</div></body></html>",
            new String[]{"but", "leave", "the", "rest"})));
  }

  @Test
  public void testTokenFilters() {
    Map<String,String> tokenizerSpecification = new HashMap<>();
    tokenizerSpecification.put("type", "standard");

    Map<String,String> filterSpecification1 = new HashMap<>();
    filterSpecification1.put("type", "englishpossessive");

    Map<String,String> filterSpecification2 = new HashMap<>();
    filterSpecification2.put("type", "stop");
    filterSpecification2.put("ignoreCase", "true");
    filterSpecification2.put("format", "snowball");
    filterSpecification2.put("words", "org/apache/lucene/analysis/snowball/english_stop.txt");

    Map<String,String> filterSpecification3 = new HashMap<>();
    filterSpecification3.put("type", "lowercase");

    @SuppressWarnings("unchecked")
    LuceneAnalyzer analyzer = new LuceneAnalyzer()
        .setTokenizer(asScalaImmutableMap(tokenizerSpecification))
        .setFilters(new scala.collection.immutable.Map[] {
            asScalaImmutableMap(filterSpecification1),
            asScalaImmutableMap(filterSpecification2),
            asScalaImmutableMap(filterSpecification3)})
        .setInputCol("rawText")
        .setOutputCol("tokens");
    assertExpectedTokens(analyzer, Lists.newArrayList(
        new TokenizerTestData("Harold's not around.", new String[]{"harold", "around"}),
        new TokenizerTestData("The dog's nose KNOWS!", new String[]{"dog", "nose", "knows"})));
  }

  @Test
  public void testUAX29URLEmailTokenizer() {
    Map<String,String> tokenizerSpecification = new HashMap<>();
    tokenizerSpecification.put("type", "uax29urlemail");
    tokenizerSpecification.put("maxTokenLength", "2000");
    LuceneAnalyzer analyzer = new LuceneAnalyzer()
        .setTokenizer(asScalaImmutableMap(tokenizerSpecification))
        .setInputCol("rawText")
        .setOutputCol("tokens");
    assertExpectedTokens(analyzer, Lists.newArrayList(
        new TokenizerTestData("Click on https://www.google.com/#q=spark+lucene",
            new String[]{"Click", "on", "https://www.google.com/#q=spark+lucene"}),
        new TokenizerTestData("Email caffeine@coffee.biz for tips on staying@alert",
            new String[]{"Email", "caffeine@coffee.biz", "for", "tips", "on", "staying", "alert"})));
  }

  private void assertExpectedTokens(LuceneAnalyzer analyzer, List<TokenizerTestData> testData) {
    JavaRDD<TokenizerTestData> rdd = jsc.parallelize(testData);
    Row[] pairs = analyzer.transform(jsql.createDataFrame(rdd, TokenizerTestData.class))
        .select("tokens", "wantedTokens")
        .collect();
    for (Row r : pairs) {
      Assert.assertEquals(r.get(0), r.get(1));
    }
  }

  private scala.collection.immutable.Map<String,String> asScalaImmutableMap(Map<String,String> map) {
    return scala.collection.immutable.Map$.MODULE$.<String,String>empty()
        .$plus$plus(JavaConversions.mapAsScalaMap(map));
  }
}
