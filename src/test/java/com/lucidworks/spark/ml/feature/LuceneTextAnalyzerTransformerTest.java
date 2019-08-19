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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.TokenizerTestData;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class LuceneTextAnalyzerTransformerTest {
  private transient JavaSparkContext jsc;
  private transient SparkSession sparkSession;

  @Before
  public void setupJavaSparkContext() {
    SparkConf conf = new SparkConf()
        .setMaster("local")
        .setAppName("JavaLuceneAnalyzerTest")
        .set("spark.default.parallelism", "1");
    sparkSession = SparkSession.builder().config(conf).getOrCreate();
    jsc = new JavaSparkContext(sparkSession.sparkContext());
  }

  @After
  public void stopSparkContext() {
    jsc.stop();
    jsc = null;
  }

  @Test
  public void testStandardTokenizer() {
    LuceneTextAnalyzerTransformer analyzer1 = new LuceneTextAnalyzerTransformer()
        .setInputCol("rawText")
        .setOutputCol("tokens"); // Default analysis schema: StandardTokenizer + LowerCaseFilter

    assertExpectedTokens(analyzer1, Arrays.asList(
        new TokenizerTestData("Test for tokenization.", new String[]{"test", "for", "tokenization"}),
        new TokenizerTestData("Te,st. punct", new String[]{"te", "st", "punct"})));

    assertExpectedTokens(analyzer1, Arrays.asList(
        new TokenizerTestData("Test for tokenization.", new String[]{"test", "for", "tokenization"}),
        new TokenizerTestData("Te,st. punct", new String[]{"te", "st", "punct"})));

    String analysisSchema1 = json("{\n" +
        "'analyzers': [{\n" +
        "  'name': 'StdTok_max10',\n" +
        "  'tokenizer': {\n" +
        "    'type': 'standard',\n" +
        "    'maxTokenLength': '10'\n" +
        "  }\n" +
        "}],\n" +
        "'fields': [{\n" +
        "  'regex': '.+',\n" +
        "  'analyzer': 'StdTok_max10'\n" +
        "}]}\n");
    analyzer1.setAnalysisSchema(analysisSchema1);
    assertExpectedTokens(analyzer1, Arrays.asList(
        new TokenizerTestData("我是中国人。 １２３４ Ｔｅｓｔｓ ",
            new String[]{"我", "是", "中", "国", "人", "１２３４", "Ｔｅｓｔｓ"}),
        new TokenizerTestData("some-dashed-phrase", new String[]{"some", "dashed", "phrase"})));

    String analysisSchema2 = json("{\n" +
        "'defaultLuceneMatchVersion': '7.0.0',\n" +
        "'analyzers': [{\n" +
        "  'name': 'StdTok_max3',\n" +
        "  'tokenizer': {\n" +
        "    'type': 'standard',\n" +
        "    'maxTokenLength': '3'\n" +
        "  }\n" +
        "}],\n" +
        "'fields': [{\n" +
        "  'regex': '.+',\n" +
        "  'analyzer': 'StdTok_max3'\n" +
        "}]}\n");
    LuceneTextAnalyzerTransformer analyzer2 = new LuceneTextAnalyzerTransformer()
        .setAnalysisSchema(analysisSchema2)
        .setInputCol("rawText")
        .setOutputCol("tokens");
    assertExpectedTokens(analyzer2, Arrays.asList(
        new TokenizerTestData("Test for tokenization.",
            new String[]{"Tes", "t", "for", "tok", "eni", "zat", "ion"}),
        new TokenizerTestData("Te,st.  punct", new String[]{"Te", "st", "pun", "ct"})));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCharFilters() {
    String analysisSchema1 = json("{\n" +
        "'analyzers': [{\n" +
        "  'name': 'strip_alpha_std_tok',\n" +
        "  'charFilters': [{\n" +
        "    'type': 'patternreplace',\n" +
        "    'pattern': '[A-Za-z]+',\n" +
        "    'replacement': ''\n" +
        "  }],\n" +
        "  'tokenizer': {\n" +
        "    'type': 'standard'\n" +
        "  }\n" +
        "}],\n" +
        "'fields': [{\n" +
        "  'regex': '.+',\n" +
        "  'analyzer': 'strip_alpha_std_tok'\n" +
        "}]}\n");
    LuceneTextAnalyzerTransformer analyzer = new LuceneTextAnalyzerTransformer()
        .setAnalysisSchema(analysisSchema1)
        .setInputCol("rawText")
        .setOutputCol("tokens");

    assertExpectedTokens(analyzer, Arrays.asList(
        new TokenizerTestData("Test for 9983, tokenization.", new String[]{"9983"}),
        new TokenizerTestData("Te,st. punct", new String[]{})));

    String analysisSchema2 = json("{\n" +
        "'analyzers': [{\n" +
        "  'name': 'htmlstrip_drop_removeme_std_tok',\n" +
        "  'charFilters': [{\n" +
        "      'type': 'htmlstrip'\n" +
        "    }, {\n" +
        "      'type': 'patternreplace',\n" +
        "      'pattern': 'removeme',\n" +
        "      'replacement': ''\n" +
        "  }],\n" +
        "  'tokenizer': {\n" +
        "    'type': 'standard'\n" +
        "  }\n" +
        "}],\n" +
        "'fields': [{\n" +
        "  'name': 'rawText',\n" +
        "  'analyzer': 'htmlstrip_drop_removeme_std_tok'\n" +
        "}]}\n");
    analyzer.setAnalysisSchema(analysisSchema2);

    assertExpectedTokens(analyzer, Collections.singletonList(
        new TokenizerTestData("<html><body>remove<b>me</b> but leave<div>the&nbsp;rest.</div></body></html>",
            new String[]{"but", "leave", "the", "rest"})));
  }

  @Test
  public void testTokenFilters() {
    String analysisSchema = json("{\n" +
        "'analyzers': [{\n" +
        "  'name': 'std_tok_possessive_stop_lower',\n" +
        "  'tokenizer': {\n" +
        "    'type': 'standard'\n" +
        "  },\n" +
        "  'filters': [{\n" +
        "      'type': 'englishpossessive'\n" +
        "    }, {\n" +
        "      'type': 'stop',\n" +
        "      'ignoreCase': 'true',\n" +
        "      'format': 'snowball',\n" +
        "      'words': 'org/apache/lucene/analysis/snowball/english_stop.txt'\n" +
        "    }, {\n" +
        "      'type': 'lowercase'\n" +
        "  }]\n" +
        "}],\n" +
        "'fields': [{\n" +
        "  'name': 'rawText',\n" +
        "  'analyzer': 'std_tok_possessive_stop_lower'\n" +
        "}]}\n");
    LuceneTextAnalyzerTransformer analyzer = new LuceneTextAnalyzerTransformer()
        .setAnalysisSchema(analysisSchema)
        .setInputCol("rawText")
        .setOutputCol("tokens");
    assertExpectedTokens(analyzer, Arrays.asList(
        new TokenizerTestData("Harold's not around.", new String[]{"harold", "around"}),
        new TokenizerTestData("The dog's nose KNOWS!", new String[]{"dog", "nose", "knows"})));
  }

  @Test
  public void testUAX29URLEmailTokenizer() {
    String analysisSchema = json("{\n" +
        "'analyzers': [{\n" +
        "  'name': 'uax29urlemail_2000',\n" +
        "  'tokenizer': {\n" +
        "    'type': 'uax29urlemail',\n" +
        "    'maxTokenLength': '2000'\n" +
        "  }\n" +
        "}],\n" +
        "'fields': [{\n" +
        "  'regex': '.+',\n" +
        "  'analyzer': 'uax29urlemail_2000'\n" +
        "}]}\n");
    LuceneTextAnalyzerTransformer analyzer = new LuceneTextAnalyzerTransformer()
        .setAnalysisSchema(analysisSchema)
        .setInputCol("rawText")
        .setOutputCol("tokens");
    assertExpectedTokens(analyzer, Arrays.asList(
        new TokenizerTestData("Click on https://www.google.com/#q=spark+lucene",
            new String[]{"Click", "on", "https://www.google.com/#q=spark+lucene"}),
        new TokenizerTestData("Email caffeine@coffee.biz for tips on staying@alert",
            new String[]{"Email", "caffeine@coffee.biz", "for", "tips", "on", "staying", "alert"})));
  }

  @Test
  public void testPrebuiltAnalyzer() {
    String analyzerConfig = json("{\n" +
        "'fields': [{\n" +
        "  'regex': '.+',\n" +
        "  'analyzer': 'org.apache.lucene.analysis.core.WhitespaceAnalyzer'\n" +
        "}]}\n");
    LuceneTextAnalyzerTransformer analyzer = new LuceneTextAnalyzerTransformer()
        .setAnalysisSchema(analyzerConfig)
        .setInputCol("rawText")
        .setOutputCol("tokens");

    assertExpectedTokens(analyzer, Arrays.asList(
        new TokenizerTestData("Test for tokenization.", new String[] { "Test", "for", "tokenization." }),
        new TokenizerTestData("Te,st. punct", new String[] { "Te,st.", "punct" })));
  }

  @Test
  public void testMultivaluedInputCol() {
    LuceneTextAnalyzerTransformer analyzer = new LuceneTextAnalyzerTransformer()
        .setInputCols(new String[]{"rawText"})
        .setOutputCol("tokens");
    assertExpectedTokens(analyzer, Collections.singletonList(
        new MV_TokenizerTestData(new String[] {"Harold's not around.", "The dog's nose KNOWS!"},
            new String[]{"harold's", "not", "around", "the", "dog's", "nose", "knows"})));
  }

  @Test
  public void testMultipleInputCols() {
    LuceneTextAnalyzerTransformer analyzer1 = new LuceneTextAnalyzerTransformer()
        .setInputCols(new String[] {"rawText1", "rawText2"})
        .setOutputCol("tokens");
    assertExpectedTokens(analyzer1, Collections.singletonList(
        new SV_SV_TokenizerTestData("Harold's not around.", "The dog's nose KNOWS!",
            new String[] {"harold's", "not", "around", "the", "dog's", "nose", "knows"})));

    String analysisSchema = json("{\n" +
          "'analyzers': [{\n" +
          "    'name': 'std_tok_lower',\n" +
          "    'tokenizer': { 'type': 'standard' },\n" +
          "    'filters':[{ 'type': 'lowercase' }]\n" +
          "  }, {\n" +
          "    'name': 'std_tok',\n" +
          "    'tokenizer': { 'type': 'standard' }\n" +
          "  }, {\n" +
          "    'name': 'htmlstrip_std_tok_lower',\n" +
          "    'charFilters': [{ 'type': 'htmlstrip' }],\n" +
          "    'tokenizer': { 'type': 'standard' },\n" +
          "    'filters': [{ 'type': 'lowercase' }]\n" +
          "}],\n" +
          "'fields': [{\n" +
          "    'name': 'rawText1',\n" +
          "    'analyzer': 'std_tok_lower'\n" +
          "  }, {\n" +
          "    'name': 'rawText2',\n" +
          "    'analyzer': 'std_tok'\n" +
          "  }, {\n" +
          "    'regex': '.+',\n" +
          "    'analyzer': 'htmlstrip_std_tok_lower'\n" +
          "}]}\n");
    LuceneTextAnalyzerTransformer analyzer2 = new LuceneTextAnalyzerTransformer()
        .setAnalysisSchema(analysisSchema)
        .setInputCols(new String[] {"rawText1", "rawText2"})
        .setOutputCol("tokens");
    assertExpectedTokens(analyzer2, Collections.singletonList(
        new SV_SV_TokenizerTestData("Harold's NOT around.", "The dog's nose KNOWS!",
            new String[]{"harold's", "not", "around", "The", "dog's", "nose", "KNOWS"})));

    assertExpectedTokens(analyzer2, Collections.singletonList(
        new SV_MV_TokenizerTestData("Harold's NOT around.", new String[] {"The dog's nose KNOWS!", "Good, fine, great..."},
            new String[] {"harold's", "not", "around", "The", "dog's", "nose", "KNOWS", "Good", "fine", "great"})));

    assertExpectedTokens(analyzer2, Collections.singletonList(
        new MV_MV_TokenizerTestData(new String[] {"Harold's NOT around.", "Anymore, I mean."},
            new String[] {"The dog's nose KNOWS!", "Good, fine, great..."},
            new String[] {"harold's", "not", "around", "anymore", "i", "mean",
                "The", "dog's", "nose", "KNOWS", "Good", "fine", "great"})));

    analyzer2.setInputCols(new String[] {"rawText1", "rawText2", "rawText3"});
    assertExpectedTokens(analyzer2, Collections.singletonList(
        new SV_SV_SV_TokenizerTestData(
            "Harold's NOT around.", "The dog's nose KNOWS!", "<html><body>Content</body></html>",
            new String[]{"harold's", "not", "around", "The", "dog's", "nose", "KNOWS", "content"})));
  }

  @Test
  public void testPrefixTokensWithInputCol() {
    String[] rawText1 = new String[] { "Harold's NOT around.", "Anymore, I mean." };
    String[] tokens1 = new String[] { "harold's", "not", "around", "anymore", "i", "mean" };

    String[] rawText2 = new String[] { "The dog's nose KNOWS!", "Good, fine, great..." };
    String[] tokens2 = new String[] { "the", "dog's", "nose", "knows", "good", "fine", "great" };

    List<String> tokenList = new ArrayList<>();
    List<String> prefixedTokenList = new ArrayList<>();
    for (String token : tokens1) {
      tokenList.add(token);
      prefixedTokenList.add("rawText1=" + token);
    }
    for (String token : tokens2) {
      tokenList.add(token);
      prefixedTokenList.add("rawText2=" + token);
    }
    String[] tokens = tokenList.toArray(new String[tokenList.size()]);
    String[] prefixedTokens = prefixedTokenList.toArray(new String[prefixedTokenList.size()]);

    // First transform without token prefixes
    LuceneTextAnalyzerTransformer analyzer = new LuceneTextAnalyzerTransformer()
        .setInputCols(new String[] {"rawText1", "rawText2"})
        .setOutputCol("tokens");
    assertExpectedTokens(analyzer, Collections.singletonList(
        new MV_MV_TokenizerTestData(rawText1, rawText2, tokens)));

    // Then transform with token prefixes
    analyzer.setPrefixTokensWithInputCol(true);
    assertExpectedTokens(analyzer, Collections.singletonList(
        new MV_MV_TokenizerTestData(rawText1, rawText2, prefixedTokens)));
  }

  @Test
  public void testMissingValues() {
    LuceneTextAnalyzerTransformer analyzer = new LuceneTextAnalyzerTransformer()
        .setInputCol("rawText")
        .setOutputCol("tokens");
    assertExpectedTokens(analyzer, Arrays.asList(new TokenizerTestData(null, new String[]{})));
    assertExpectedTokens(analyzer, Arrays.asList(new TokenizerTestData("", new String[]{})));
    assertExpectedTokens(analyzer, Collections.singletonList(
        new MV_TokenizerTestData(new String[] {null, "Harold's not around.", null, "The dog's nose KNOWS!", ""},
            new String[]{"harold's", "not", "around", "the", "dog's", "nose", "knows"})));

    analyzer.setInputCols(new String[] {"rawText1", "rawText2", "rawText3"});
    assertExpectedTokens(analyzer, Collections.singletonList(
        new SV_SV_SV_TokenizerTestData(
            "", "The dog's nose KNOWS!", null,
            new String[]{"the", "dog's", "nose", "knows"})));
  }

  private <T> void assertExpectedTokens(LuceneTextAnalyzerTransformer analyzer, List<T> testData) {
    JavaRDD<T> rdd = jsc.parallelize(testData);
    List<Row> pairs = analyzer.transform(sparkSession.createDataFrame(rdd, testData.get(0).getClass()))
        .select("wantedTokens", "tokens")
        .collectAsList();
    for (Row r : pairs) {
      Assert.assertEquals(r.get(0), r.get(1));
    }
  }

  private String json(String singleQuoted) {
    return singleQuoted.replaceAll("'", "\"");
  }
}
