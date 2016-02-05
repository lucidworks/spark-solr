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
import org.apache.spark.sql.SQLContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


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
    LuceneAnalyzer analyzer1 = new LuceneAnalyzer()
        .setInputCol("rawText")
        .setOutputCol("tokens"); // Default analysis schema: StandardTokenizer + LowerCaseFilter

    assertExpectedTokens(analyzer1, Arrays.asList(
        new TokenizerTestData("Test for tokenization.", new String[]{"test", "for", "tokenization"}),
        new TokenizerTestData("Te,st. punct", new String[]{"te", "st", "punct"})));

    assertExpectedTokens(analyzer1, Arrays.asList(
        new TokenizerTestData("Test for tokenization.", new String[]{"test", "for", "tokenization"}),
        new TokenizerTestData("Te,st. punct", new String[]{"te", "st", "punct"})));

    String analysisSchema1 = json("{\n" +
        "'schemaType': 'LuceneAnalyzerSchema.v1',\n" +
        "'analyzers': [{\n" +
        "  'name': 'StdTok_max10',\n" +
        "  'tokenizer': {\n" +
        "    'type': 'standard',\n" +
        "    'maxTokenLength': '10'\n" +
        "  }\n" +
        "}],\n" +
        "'inputColumns': [{\n" +
        "  'regex': '.+',\n" +
        "  'analyzer': 'StdTok_max10'\n" +
        "}]}\n");
    analyzer1.setAnalysisSchema(analysisSchema1);
    assertExpectedTokens(analyzer1, Arrays.asList(
        new TokenizerTestData("我是中国人。 １２３４ Ｔｅｓｔｓ ",
            new String[]{"我", "是", "中", "国", "人", "１２３４", "Ｔｅｓｔｓ"}),
        new TokenizerTestData("some-dashed-phrase", new String[]{"some", "dashed", "phrase"})));

    String analysisSchema2 = json("{\n" +
        "'schemaType': 'LuceneAnalyzerSchema.v1',\n" +
        "'defaultLuceneMatchVersion': '4.10.4',\n" +
        "'analyzers': [{\n" +
        "  'name': 'StdTok_max3',\n" +
        "  'tokenizer': {\n" +
        "    'type': 'standard',\n" +
        "    'maxTokenLength': '3'\n" +
        "  }\n" +
        "}],\n" +
        "'inputColumns': [{\n" +
        "  'regex': '.+',\n" +
        "  'analyzer': 'StdTok_max3'\n" +
        "}]}\n");
    LuceneAnalyzer analyzer2 = new LuceneAnalyzer()
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
        "'schemaType': 'LuceneAnalyzerSchema.v1',\n" +
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
        "'inputColumns': [{\n" +
        "  'regex': '.+',\n" +
        "  'analyzer': 'strip_alpha_std_tok'\n" +
        "}]}\n");
    LuceneAnalyzer analyzer = new LuceneAnalyzer()
        .setAnalysisSchema(analysisSchema1)
        .setInputCol("rawText")
        .setOutputCol("tokens");

    assertExpectedTokens(analyzer, Arrays.asList(
        new TokenizerTestData("Test for 9983, tokenization.", new String[]{"9983"}),
        new TokenizerTestData("Te,st. punct", new String[]{})));

    String analysisSchema2 = json("{\n" +
        "'schemaType': 'LuceneAnalyzerSchema.v1',\n" +
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
        "'inputColumns': [{\n" +
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
        "'schemaType': 'LuceneAnalyzerSchema.v1',\n" +
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
        "'inputColumns': [{\n" +
        "  'name': 'rawText',\n" +
        "  'analyzer': 'std_tok_possessive_stop_lower'\n" +
        "}]}\n");
    LuceneAnalyzer analyzer = new LuceneAnalyzer()
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
        "'schemaType': 'LuceneAnalyzerSchema.v1',\n" +
        "'analyzers': [{\n" +
        "  'name': 'uax29urlemail_2000',\n" +
        "  'tokenizer': {\n" +
        "    'type': 'uax29urlemail',\n" +
        "    'maxTokenLength': '2000'\n" +
        "  }\n" +
        "}],\n" +
        "'inputColumns': [{\n" +
        "  'regex': '.+',\n" +
        "  'analyzer': 'uax29urlemail_2000'\n" +
        "}]}\n");
    LuceneAnalyzer analyzer = new LuceneAnalyzer()
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
  public void testMultivaluedInputCol() {
    LuceneAnalyzer analyzer = new LuceneAnalyzer()
        .setInputCols(new String[]{"rawText"})
        .setOutputCol("tokens");
    assertExpectedTokens(analyzer, Collections.singletonList(
        new MV_TokenizerTestData(new String[] {"Harold's not around.", "The dog's nose KNOWS!"},
            new String[]{"harold's", "not", "around", "the", "dog's", "nose", "knows"})));
  }

  @Test
  public void testMultipleInputCols() {
    LuceneAnalyzer analyzer1 = new LuceneAnalyzer()
        .setInputCols(new String[] {"rawText1", "rawText2"})
        .setOutputCol("tokens");
    assertExpectedTokens(analyzer1, Collections.singletonList(
        new SV_SV_TokenizerTestData("Harold's not around.", "The dog's nose KNOWS!",
            new String[] {"harold's", "not", "around", "the", "dog's", "nose", "knows"})));

    String analysisSchema = json("{\n" +
          "'schemaType': 'LuceneAnalyzerSchema.v1',\n" +
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
          "'inputColumns': [{\n" +
          "    'name': 'rawText1',\n" +
          "    'analyzer': 'std_tok_lower'\n" +
          "  }, {\n" +
          "    'name': 'rawText2',\n" +
          "    'analyzer': 'std_tok'\n" +
          "  }, {\n" +
          "    'regex': '.+',\n" +
          "    'analyzer': 'htmlstrip_std_tok_lower'\n" +
          "}]}\n");
    LuceneAnalyzer analyzer2 = new LuceneAnalyzer()
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

  private <T> void assertExpectedTokens(LuceneAnalyzer analyzer, List<T> testData) {
    JavaRDD<T> rdd = jsc.parallelize(testData);
    Row[] pairs = analyzer.transform(jsql.createDataFrame(rdd, testData.get(0).getClass()))
        .select("tokens", "wantedTokens")
        .collect();
    for (Row r : pairs) {
      Assert.assertEquals(r.get(0), r.get(1));
    }
  }

  private String json(String singleQuoted) {
    return singleQuoted.replaceAll("'", "\"");
  }
}
