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

package com.lucidworks.spark.analysis;

import junit.framework.Assert;
import org.junit.Test;

import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LuceneTextAnalyzerTest {
  public final String stdTokLowerSchema = json(
      "{'analyzers': [{'name': 'StdTokLower',\n" +
          "                'tokenizer': {'type': 'standard'}, 'filters': [{'type': 'lowercase'}] }],\n" +
          " 'fields': [{'regex': '.+', 'analyzer': 'StdTokLower' }] }\n");
  @Test
  public void testStandardTokenizer() {
    LuceneTextAnalyzer analyzer1 = new LuceneTextAnalyzer(stdTokLowerSchema);

    assertExpectedTokens(analyzer1, "Test for tokenization.", Arrays.asList("test", "for", "tokenization"));
    assertExpectedTokens(analyzer1, "Te,st. punct", Arrays.asList("te", "st", "punct"));

    String stdTokMax10Schema = json(
        "{'analyzers': [{'name': 'StdTok_max10',\n" +
        "                'tokenizer': {'type': 'standard', 'maxTokenLength': '10'}\n}],\n" +
        "'fields': [{'regex': '.+', 'analyzer': 'StdTok_max10'}] }\n");
    LuceneTextAnalyzer analyzer2 = new LuceneTextAnalyzer(stdTokMax10Schema);

    assertExpectedTokens(analyzer2, "我是中国人。 １２３４ Ｔｅｓｔｓ ",
        Arrays.asList("我", "是", "中", "国", "人", "１２３４", "Ｔｅｓｔｓ"));
    assertExpectedTokens(analyzer2, "some-dashed-phrase", Arrays.asList("some", "dashed", "phrase"));

    String stdTokMax3Schema = json(
        "{'defaultLuceneMatchVersion': '7.0.0',\n" +
        " 'analyzers': [{'name': 'StdTok_max3',\n" +
        "                'tokenizer': {'type': 'standard', 'maxTokenLength': '3'} }],\n" +
        "'fields': [{'regex': '.+', 'analyzer': 'StdTok_max3'}] }\n");
    LuceneTextAnalyzer analyzer3 = new LuceneTextAnalyzer(stdTokMax3Schema);

    assertExpectedTokens(analyzer3, "Test for tokenization.",
        Arrays.asList("Tes", "t", "for", "tok", "eni", "zat", "ion"));
    assertExpectedTokens(analyzer3, "Te,st.  punct", Arrays.asList("Te", "st", "pun", "ct"));
  }

  @Test
  public void testCharFilters() {
    String schema1 = json("{\n" +
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
    LuceneTextAnalyzer analyzer1 = new LuceneTextAnalyzer(schema1);

    assertExpectedTokens(analyzer1, "Test for 9983, tokenization.", Collections.singletonList("9983"));
    assertExpectedTokens(analyzer1, "Te,st. punct", Collections.<String>emptyList());

    String schema2 = json("{\n" +
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
    LuceneTextAnalyzer analyzer2 = new LuceneTextAnalyzer(schema2);

    assertExpectedTokens(analyzer2, "rawText",
        "<html><body>remove<b>me</b> but leave<div>the&nbsp;rest.</div></body></html>",
        Arrays.asList("but", "leave", "the", "rest"));
  }

  @Test
  public void testTokenFilters() {
    String schema = json("{\n" +
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
    LuceneTextAnalyzer analyzer = new LuceneTextAnalyzer(schema);
    assertExpectedTokens(analyzer, "rawText", "Harold's not around.", Arrays.asList("harold", "around"));
    assertExpectedTokens(analyzer, "rawText", "The dog's nose KNOWS!", Arrays.asList("dog", "nose", "knows"));
  }

  @Test
  public void testUAX29URLEmailTokenizer() {
    String schema = json("{\n" +
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
    LuceneTextAnalyzer analyzer = new LuceneTextAnalyzer(schema);
    assertExpectedTokens(analyzer, "Click on https://www.google.com/#q=spark+lucene",
        Arrays.asList("Click", "on", "https://www.google.com/#q=spark+lucene"));
    assertExpectedTokens(analyzer, "Email caffeine@coffee.biz for tips on staying@alert",
        Arrays.asList("Email", "caffeine@coffee.biz", "for", "tips", "on", "staying", "alert"));
  }

  @Test
  public void testPrebuiltAnalyzer() {
    String schema = json("{\n" +
        "'fields': [{\n" +
        "  'regex': '.+',\n" +
        "  'analyzer': 'org.apache.lucene.analysis.core.WhitespaceAnalyzer'\n" +
        "}]}\n");
    LuceneTextAnalyzer analyzer = new LuceneTextAnalyzer(schema);

    assertExpectedTokens(analyzer, "Test for tokenization.", Arrays.asList("Test", "for", "tokenization."));
    assertExpectedTokens(analyzer, "Te,st. punct", Arrays.asList("Te,st.", "punct"));
  }

  @Test
  public void testMultivaluedInput() {
    LuceneTextAnalyzer analyzer = new LuceneTextAnalyzer(stdTokLowerSchema);
    assertExpectedTokens(analyzer, Arrays.asList("Harold's not around.", "The dog's nose KNOWS!"),
            Arrays.asList("harold's", "not", "around", "the", "dog's", "nose", "knows"));
  }

  @Test
  public void testMultipleFields() {
    LuceneTextAnalyzer analyzer1 = new LuceneTextAnalyzer(stdTokLowerSchema);

    Map<String,String> fieldValues = new HashMap<>();
    Map<String,List<String>> expected = new HashMap<>();
    fieldValues.put("one", "Harold's not around.");
    expected.put("one", Arrays.asList("harold's", "not", "around"));
    fieldValues.put("two", "The dog's nose KNOWS!");
    expected.put("two", Arrays.asList("the", "dog's", "nose", "knows"));

    assertExpectedTokens(analyzer1, fieldValues, expected);

    String schema = json("{\n" +
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
    LuceneTextAnalyzer analyzer2 = new LuceneTextAnalyzer(schema);

    fieldValues.clear();
    expected.clear();
    fieldValues.put("rawText1", "Harold's NOT around.");
    expected.put("rawText1", Arrays.asList("harold's", "not", "around"));
    fieldValues.put("rawText2", "The dog's nose KNOWS!");
    expected.put("rawText2", Arrays.asList("The", "dog's", "nose", "KNOWS"));

    assertExpectedTokens(analyzer2, fieldValues, expected);

    Map<String,List<String>> fieldValuesMV = new HashMap<>();
    expected.clear();
    fieldValuesMV.put("rawText1", Collections.singletonList("Harold's NOT around."));
    expected.put("rawText1", Arrays.asList("harold's", "not", "around"));
    fieldValuesMV.put("rawText2", Arrays.asList("The dog's nose KNOWS!", "Good, fine, great..."));
    expected.put("rawText2", Arrays.asList("The", "dog's", "nose", "KNOWS", "Good", "fine", "great"));

    assertExpectedTokensMV(analyzer2, fieldValuesMV, expected);

    fieldValuesMV.clear();
    expected.clear();
    fieldValuesMV.put("rawText1", Arrays.asList("Harold's NOT around.", "Anymore, I mean."));
    expected.put("rawText1", Arrays.asList("harold's", "not", "around", "anymore", "i", "mean"));
    fieldValuesMV.put("rawText2", Arrays.asList("The dog's nose KNOWS!", "Good, fine, great..."));
    expected.put("rawText2", Arrays.asList("The", "dog's", "nose", "KNOWS", "Good", "fine", "great"));

    assertExpectedTokensMV(analyzer2, fieldValuesMV, expected);

    fieldValues.clear();
    expected.clear();
    fieldValues.put("rawText1", "Harold's not around.");
    expected.put("rawText1", Arrays.asList("harold's", "not", "around"));
    fieldValues.put("rawText2", "The dog's nose KNOWS!");
    expected.put("rawText2", Arrays.asList("The", "dog's", "nose", "KNOWS"));
    fieldValues.put("rawText3", "<html><body>Content</body></html>");
    expected.put("rawText3", Collections.singletonList("content"));

    assertExpectedTokens(analyzer2, fieldValues, expected);
  }

  @Test
  public void testMissingValues() {
    LuceneTextAnalyzer analyzer = new LuceneTextAnalyzer(stdTokLowerSchema);
    assertExpectedTokens(analyzer, (String) null, Collections.<String>emptyList());
    assertExpectedTokens(analyzer, "", Collections.<String>emptyList());
    assertExpectedTokens(analyzer, Arrays.asList(null, "Harold's not around.", null, "The dog's nose KNOWS!", ""),
        Arrays.asList("harold's", "not", "around", "the", "dog's", "nose", "knows"));

    Map<String,String> fieldValues = new HashMap<>();
    Map<String,List<String>> expected = new HashMap<>();
    fieldValues.put("one", "");
    expected.put("one", Collections.<String>emptyList());
    fieldValues.put("two", "The dog's nose KNOWS!");
    expected.put("two", Arrays.asList("the", "dog's", "nose", "knows"));
    fieldValues.put("three", null);
    expected.put("three", Collections.<String>emptyList());

    assertExpectedTokens(analyzer, fieldValues, expected);
  }

  @Test
  public void testAnalyzeReader() {
    LuceneTextAnalyzer analyzer1 = new LuceneTextAnalyzer(stdTokLowerSchema);

    assertExpectedTokens(analyzer1, new StringReader("Test for tokenization."), Arrays.asList("test", "for", "tokenization"));
    assertExpectedTokens(analyzer1, new StringReader("Te,st. punct"), Arrays.asList("te", "st", "punct"));

    String stdTokMax3Schema = json(
        "{'defaultLuceneMatchVersion': '7.0.0',\n" +
            " 'analyzers': [{'name': 'StdTok_max3',\n" +
            "                'tokenizer': {'type': 'standard', 'maxTokenLength': '3'} }],\n" +
            "'fields': [{'regex': '.+', 'analyzer': 'StdTok_max3'}] }\n");
    LuceneTextAnalyzer analyzer2 = new LuceneTextAnalyzer(stdTokMax3Schema);

    assertExpectedTokens(analyzer2, new StringReader("Test for tokenization."),
        Arrays.asList("Tes", "t", "for", "tok", "eni", "zat", "ion"));
    assertExpectedTokens(analyzer2, new StringReader("Te,st.  punct"), Arrays.asList("Te", "st", "pun", "ct"));
  }

  @Test
  public void testPreAnalyzedJson() {
    String text = "Test for tokenization.";
    // TODO: compare parsed JSON rather than strings; direct string comparison is brittle, e.g. key ordering in JSON objects is not guaranteed
    String jsonWithStringVal = json(
        "{'v':'1','str':'" + text + "','tokens':["
            + "{'t':'test','s':0,'e':4,'i':1},"
            + "{'t':'for','s':5,'e':8,'i':1},"
            + "{'t':'tokenization','s':9,'e':21,'i':1}]}");
    String jsonNoStringVal = json(
        "{'v':'1','tokens':["
            + "{'t':'test','s':0,'e':4,'i':1},"
            + "{'t':'for','s':5,'e':8,'i':1},"
            + "{'t':'tokenization','s':9,'e':21,'i':1}]}");
    LuceneTextAnalyzer analyzer = new LuceneTextAnalyzer(stdTokLowerSchema);
    assertExpectedJson(analyzer, "dummy", text, true, jsonWithStringVal);
    assertExpectedJson(analyzer, "dummy", text, false, jsonNoStringVal);
    assertExpectedJson(analyzer, "dummy", new StringReader(text), true, jsonWithStringVal);
    assertExpectedJson(analyzer, "dummy", new StringReader(text), false, jsonNoStringVal);
  }

  private static void assertExpectedTokens(LuceneTextAnalyzer analyzer, String in, List<String> expected) {
    assertExpectedTokens(analyzer, "dummy", in, expected);
  }

  private static void assertExpectedTokens(LuceneTextAnalyzer analyzer, String field, String in, List<String> expected) {
    List<String> output = analyzer.analyzeJava(field, in);
    Assert.assertEquals(expected, output);
  }

  private static void assertExpectedTokens(LuceneTextAnalyzer analyzer, Reader reader, List<String> expected) {
    assertExpectedTokens(analyzer, "dummy", reader, expected);
  }

  private static void assertExpectedTokens(LuceneTextAnalyzer analyzer, String field, Reader reader, List<String> expected) {
    List<String> output = analyzer.analyzeJava(field, reader);
    Assert.assertEquals(expected, output);
  }

  private static void assertExpectedTokens(LuceneTextAnalyzer analyzer, List<String> in, List<String> expected) {
    assertExpectedTokens(analyzer, "dummy", in, expected);
  }

  private static void assertExpectedTokens(LuceneTextAnalyzer analyzer, String field, List<String> in, List<String> expected) {
    List<String> output = analyzer.analyzeMVJava(field, in);
    Assert.assertEquals(expected, output);
  }

  private static void assertExpectedTokens
      (LuceneTextAnalyzer analyzer, Map<String,String> fieldValues, Map<String,List<String>> expected) {
    Map<String,List<String>> output = analyzer.analyzeJava(fieldValues);
    Assert.assertEquals(expected, output);
  }

  private static void assertExpectedTokensMV // different name because type erasure
      (LuceneTextAnalyzer analyzer, Map<String,List<String>> fieldValues, Map<String,List<String>> expected) {
    Map<String,List<String>> output = analyzer.analyzeMVJava(fieldValues);
    Assert.assertEquals(expected, output);
  }

  private static void assertExpectedJson
      (LuceneTextAnalyzer analyzer, String field, String in, boolean stored, String expected) {
    // TODO: compare parsed JSON rather than strings; direct string comparison is brittle, e.g. key ordering in JSON objects is not guaranteed
    String output = analyzer.toPreAnalyzedJson(field, in, stored);
    Assert.assertEquals(expected, output);
  }

  private static void assertExpectedJson
      (LuceneTextAnalyzer analyzer, String field, Reader reader, boolean stored, String expected) {
    // TODO: compare parsed JSON rather than strings; direct string comparison is brittle, e.g. key ordering in JSON objects is not guaranteed
    String output = analyzer.toPreAnalyzedJson(field, reader, stored);
    Assert.assertEquals(expected, output);
  }

  private String json(String singleQuoted) {
    return singleQuoted.replaceAll("'", "\"");
  }
}
