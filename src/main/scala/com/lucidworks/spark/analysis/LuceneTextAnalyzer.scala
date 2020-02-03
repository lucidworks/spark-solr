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

import java.io.{PrintWriter, Reader, StringWriter}
import java.util.regex.Pattern

import com.lucidworks.spark.util.Utils
import org.apache.commons.io.IOUtils
import org.apache.lucene.analysis.custom.CustomAnalyzer
import org.apache.lucene.analysis.tokenattributes.{CharTermAttribute, OffsetAttribute, PositionIncrementAttribute}
import org.apache.lucene.analysis.{Analyzer, DelegatingAnalyzerWrapper, TokenStream}
import org.apache.lucene.util.{Version => LuceneVersion}
import org.apache.solr.schema.JsonPreAnalyzedParser
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import scala.collection.JavaConversions._
import scala.collection.immutable
import scala.collection.mutable
import scala.util.control.Breaks._
import scala.util.control.NonFatal

/**
  * This class allows simple access to custom Lucene text processing pipelines, a.k.a. text analyzers,
  * which are specified via a JSON schema that hosts named analyzer specifications and mappings from
  * field name(s) to analyzer(s).
  *
  * Here's an example schema with descriptions inline as comments:
  * {{{
  * {
  *   "defaultLuceneMatchVersion": "7.0.0" // Optional.  Supplied to analysis components
  *                                         //     that don't explicitly specify "luceneMatchVersion".
  *   "analyzers": [              // Optional.  If not included, all field mappings must be
  *     {                         //     to fully qualified class names of Lucene Analyzer subclasses.
  *       "name": "html",         // Required.  Mappings in the "fields" array below refer to this name.
  *       "charFilters":[{        // Optional.
  *         "type": "htmlstrip"   // Required. "htmlstrip" is the SPI name for HTMLStripCharFilter
  *       }],
  *       "tokenizer": {          // Required.  Only one allowed.
  *         "type": "standard"    // Required. "standard" is the SPI name for StandardTokenizer
  *       },
  *       "filters": [{           // Optional.
  *           "type": "stop",     // Required.  "stop" is the SPI name for StopFilter
  *           "ignoreCase": "true",  // Component-specific params
  *           "format": "snowball",
  *           "words": "org/apache/lucene/analysis/snowball/english_stop.txt"
  *         }, {
  *           "type": "lowercase" // Required. "lowercase" is the SPI name for LowerCaseFilter
  *       }]
  *     },
  *     { "name": "stdtok", "tokenizer": { "type": "standard" } }
  *   ],
  *   "fields": [{                // Required.  To lookup an analyzer for a field, first the "name"
  *                               //     mappings are consulted, and then the "regex" mappings are
  *                               //     tested, in the order specified.
  *       "name": "keywords",     // Either "name" or "regex" is required.  "name" matches the field name exactly.
  *       "analyzer": "org.apache.lucene.analysis.core.KeywordAnalyzer" // FQCN of an Analyzer subclass
  *     }, {
  *       "regex": ".*html.*"     // Either "name" or "regex" is required.  "regex" must match the whole field name.
  *       "analyzer": "html"      // Reference to the named analyzer specified in the "analyzers" section.
  *     }, {
  *       "regex": ".+",          // Either "name" or "regex" is required.  "regex" must match the whole field name.
  *       "analyzer": "stdtok"    // Reference to the named analyzer specified in the "analyzers" section.
  *   }]
  * }
  * }}}
  */
class LuceneTextAnalyzer(analysisSchema: String) extends Serializable {
  @transient private lazy val analyzerSchema = new AnalyzerSchema(analysisSchema)
  @transient private lazy val analyzerCache = mutable.Map.empty[String, Analyzer]
  def isValid: Boolean = analyzerSchema.isValid
  def invalidMessages: String = analyzerSchema.invalidMessages.result()
  /** Returns the analyzer mapped to the given field in the configured analysis schema, if any. */
  def getFieldAnalyzer(field: String): Option[Analyzer] = analyzerSchema.getAnalyzer(field)

  def analyze(field: String, o: Any): Seq[String] = {
    o match {
      case s: String => analyze(field, s)
      case as: mutable.WrappedArray[String] @unchecked => analyzeMV(field, as)
      case a: Any => analyze(field, a.toString)
      case _ => Seq.empty[String]
    }
  }

  def analyzeJava(field: String, o: Any): java.util.List[String] = {
    seqAsJavaList(analyze(field, o))
  }

  /** Looks up the analyzer mapped to the given field from the configured analysis schema,
    * uses it to perform analysis on the given string, returning the produced token sequence.
    */
  def analyze(field: String, str: String): Seq[String] = {
    if ( ! isValid) throw new IllegalArgumentException(invalidMessages)
    if (str == null) return Seq.empty[String]
    analyze(tokenStream(field, str))
  }
  /** Looks up the analyzer mapped to the given field from the configured analysis schema,
    * uses it to perform analysis on the given reader, returning the produced token sequence.
    */
  def analyze(field: String, reader: Reader): Seq[String] = {
    if ( ! isValid) throw new IllegalArgumentException(invalidMessages)
    analyze(tokenStream(field, reader))
  }
  /** For each of the field->value pairs in fieldValues, looks up the analyzer mapped
    * to the field from the configured analysis schema, and uses it to perform analysis on the
    * value.  Returns a map from the fields to the produced token sequences.
    */
  def analyze(fieldValues: immutable.Map[String,String]): immutable.Map[String,Seq[String]] = {
    val builder = immutable.Map.newBuilder[String,Seq[String]]
    for ((field, value) <- fieldValues) builder += field -> analyze(field, value)
    builder.result()
  }
  /** Looks up the analyzer mapped to the given field from the configured analysis schema,
    * uses it to perform analysis on each of the given values, and returns the flattened
    * concatenation of the produced token sequence.
    */
  def analyzeMV(field: String, values: Seq[String]): Seq[String] = {
    if (values == null) return Seq.empty[String]
    val seqBuilder = Seq.newBuilder[String]
    values foreach { value => seqBuilder ++= analyze(field, value) }
    seqBuilder.result()
  }
  /** For each of the field->multi-value pairs in fieldValues, looks up the analyzer mapped
    * to the field from the configured analysis schema, and uses it to perform analysis on the
    * each of the values.  Returns a map from the fields to the flattened concatenation of the
    * produced token sequences.
    */
  def analyzeMV(fieldValues: immutable.Map[String,Seq[String]]): immutable.Map[String,Seq[String]] = {
    val builder = immutable.Map.newBuilder[String,Seq[String]]
    for ((field, values) <- fieldValues) { builder += field -> analyzeMV(field, values) }
    builder.result()
  }
  /** Java-friendly version: looks up the analyzer mapped to the given field from the configured
    * analysis schema, uses it to perform analysis on the given string, returning the produced
    * token sequence. */
  def analyzeJava(field: String, str: String): java.util.List[String] = {
    seqAsJavaList(analyze(field, str))
  }
  /** Java-friendly version: looks up the analyzer mapped to the given field from the configured
    * analysis schema, uses it to perform analysis on the given reader, returning the produced
    * token sequence. */
  def analyzeJava(field: String, reader: Reader): java.util.List[String] = {
    seqAsJavaList(analyze(field, reader))
  }
  /** Java-friendly version: for each of the field->value pairs in fieldValues, looks up the
    * analyzer mapped to the field from the configured analysis schema, and uses it to perform
    * analysis on the value.  Returns a map from the fields to the produced token sequences.
    */
  def analyzeJava(fieldValues: java.util.Map[String,String]): java.util.Map[String,java.util.List[String]] = {
    val output = new java.util.HashMap[String,java.util.List[String]]()
    for ((field, value) <- fieldValues) output.put(field, analyzeJava(field, value))
    java.util.Collections.unmodifiableMap(output)
  }
  /** Java-friendly version: looks up the analyzer mapped to the given field from the configured
    * analysis schema, uses it to perform analysis on each of the given values, and returns the
    * flattened concatenation of the produced token sequence.
    */
  def analyzeMVJava(field: String, values: java.util.List[String]): java.util.List[String] = {
    if (values == null) return java.util.Collections.emptyList[String]()
    val output = new java.util.ArrayList[String]()
    values foreach { value => output.addAll(analyzeJava(field, value)) }
    output
  }
  /** Java-friendly version: for each of the field->multi-value pairs in fieldValues, looks up the
    * analyzer mapped to the field from the configured analysis schema, and uses it to perform
    * analysis on each of the values.  Returns a map from the fields to the flattened concatenation
    * of the produced token sequences.
    */
  def analyzeMVJava(fieldValues: java.util.Map[String,java.util.List[String]])
  : java.util.Map[String,java.util.List[String]] = {
    val output = new java.util.HashMap[String,java.util.List[String]]()
    for ((field, values) <- fieldValues) output.put(field, analyzeMVJava(field, values))
    java.util.Collections.unmodifiableMap(output)
  }
  def tokenStream(fieldName: String, text: String) = analyzerWrapper.tokenStream(fieldName, text)
  /** Looks up the analyzer mapped to `fieldName` and returns a [[org.apache.lucene.analysis.TokenStream]]
    * for the analyzer to tokenize the contents of `reader`. */
  def tokenStream(fieldName: String, reader: Reader) = analyzerWrapper.tokenStream(fieldName, reader)
  /** Looks up the analyzer mapped to the given field from the configured analysis schema,
    * uses it to perform analysis on the given string, and returns a PreAnalyzedField-compatible
    * JSON string with the following serialized attributes:
    *
    *   - CharTermAttribute (token text)
    *   - OffsetAttribute (start and end character offsets)
    *   - PositionIncrementAttribute (token position relative to the previous token)
    *
    * If stored = true, the original string input value will be included as a value to be stored.
    * (Note that the Solr schema for the destination Solr field must be configured to store the
    * value; if it is not, then the stored value included in the JSON will be ignored by Solr.)
    */
  def toPreAnalyzedJson(field: String, str: String, stored: Boolean): String = {
    toPreAnalyzedJson(tokenStream(field, str), if (stored) Some(str) else None)
  }
  /** Looks up the analyzer mapped to the given field from the configured analysis schema,
    * uses it to perform analysis on the given reader, and returns a PreAnalyzedField-compatible
    * JSON string with the following serialized attributes:
    *
    * - CharTermAttribute (token text),
    * - OffsetAttribute (start and end position)
    * - PositionIncrementAttribute (token position relative to the previous token)
    *
    * If stored = true, the original reader input value, read into a string, will be included as
    * a value to be stored. (Note that the Solr schema for the destination Solr field must be
    * configured to store the value; if it is not, then the stored value included in the JSON
    * will be ignored by Solr.)
    */
  def toPreAnalyzedJson(field: String, reader: Reader, stored: Boolean): String = {
    if (stored)
      toPreAnalyzedJson(field, IOUtils.toString(reader), stored = true)
    else
      toPreAnalyzedJson(tokenStream(field, reader), None)
  }
  private def toPreAnalyzedJson(stream: TokenStream, str: Option[String]): String = {
    // Implementation note: Solr's JsonPreAnalyzedParser.toFormattedString() produces JSON
    // suitable for use with PreAnalyzedField, but there are problems with using it with
    // CustomAnalyzer:
    //
    //   - toFormattedString() will serialize all attributes present on the passed-in token
    //     stream's AttributeSource, which is fixed by CustomAnalyzer at those in the
    //     PackedTokenAttributeImpl, which includes the PositionLengthAttribute and the
    //     TypeAttribute, neither of which are indexed, and so shouldn't be output
    //     (by default anyway) from this method.
    //   - To modify the set of attributes that CustomAnalyzer has in its AttributeSource,
    //     CustomAnalyzer can't be extended because it's final, so CustomAnalyzer's
    //     createComponents() method can't be overridden to pass in an alternate AttributeFactory
    //     to TokenizerFactory.create().  However, a wrapper can be constructed that forwards all
    //     methods except createComponents(), and then have createComponents() do the right thing.
    //   - Once an alternate AttributeFactory is used in an effectively overridden
    //     CustomAnalyzer.createComponents(), this form will be cached for future uses, but we
    //     don't want that, since it might conflict with the analyze*() methods' requirements,
    //     and future versions of toPreAnalyzedJson might allow for customization of attributes
    //     to output (including e.g. PayloadAttribute).  So we would have to either use an
    //     alternate cache, or not cache analyzers used by toPreAnalyzedJson(), both of which
    //     seem overcomplicated.
    //
    // The code below constructs JSON with a fixed set of serialized attributes.

    val termAtt = stream.addAttribute(classOf[CharTermAttribute])
    val offsetAtt = stream.addAttribute(classOf[OffsetAttribute])
    val posIncAtt = stream.addAttribute(classOf[PositionIncrementAttribute])
    var tokens = List.newBuilder[immutable.ListMap[String, Any]]
    val token = immutable.ListMap.newBuilder[String, Any]
    try {
      stream.reset()
      while (stream.incrementToken) {
        token.clear()
        token += (JsonPreAnalyzedParser.TOKEN_KEY -> new String(termAtt.buffer, 0, termAtt.length))
        token += (JsonPreAnalyzedParser.OFFSET_START_KEY -> offsetAtt.startOffset)
        token += (JsonPreAnalyzedParser.OFFSET_END_KEY -> offsetAtt.endOffset)
        token += (JsonPreAnalyzedParser.POSINCR_KEY -> posIncAtt.getPositionIncrement)
        tokens += token.result
      }
      stream.end()
    } finally {
      stream.close()
    }
    val topLevel = immutable.ListMap.newBuilder[String, Any]
    topLevel += (JsonPreAnalyzedParser.VERSION_KEY -> JsonPreAnalyzedParser.VERSION)
    if (str.isDefined) topLevel += (JsonPreAnalyzedParser.STRING_KEY -> str)
    topLevel += (JsonPreAnalyzedParser.TOKENS_KEY -> tokens.result)
    implicit val formats = org.json4s.DefaultFormats // required by Serialization.write()
    Serialization.write(topLevel.result)
  }
  @transient private lazy val analyzerWrapper = new AnalyzerWrapper
  private class AnalyzerWrapper extends DelegatingAnalyzerWrapper(Analyzer.PER_FIELD_REUSE_STRATEGY) {
    override protected def getWrappedAnalyzer(field: String): Analyzer = {
      analyzerCache.synchronized {
        var analyzer = analyzerCache.get(field)
        if (analyzer.isEmpty) {
          if (isValid) analyzer = analyzerSchema.getAnalyzer(field)
          if ( ! isValid) throw new IllegalArgumentException(invalidMessages) // getAnalyzer can make isValid false
          if (analyzer.isEmpty) throw new IllegalArgumentException(s"No analyzer defined for field '$field'")
          analyzerCache.put(field, analyzer.get)
        }
        analyzer.get
      }
    }
  }
  private def analyze(inputStream: TokenStream): Seq[String] = {
    val builder = Seq.newBuilder[String]
    val charTermAttr = inputStream.addAttribute(classOf[CharTermAttribute])
    inputStream.reset()
    while (inputStream.incrementToken) builder += charTermAttr.toString
    inputStream.end()
    inputStream.close()
    builder.result()
  }
}

private class AnalyzerSchema(val analysisSchema: String) {
  implicit val formats = org.json4s.DefaultFormats    // enable extract
  val schemaConfig = parse(analysisSchema).extract[SchemaConfig]
  val analyzers = mutable.Map[String, Analyzer]()
  var isValid: Boolean = true
  var invalidMessages : StringBuilder = new StringBuilder()
  try {
    schemaConfig.defaultLuceneMatchVersion.foreach { version =>
      if ( ! LuceneVersion.parseLeniently(version).onOrAfter(LuceneVersion.LUCENE_7_0_0)) {
        isValid = false
        invalidMessages.append(
          s"""defaultLuceneMatchVersion "${schemaConfig.defaultLuceneMatchVersion}"""")
          .append(" is not on or after ").append(LuceneVersion.LUCENE_7_0_0).append("\n")
      }
    }
  } catch {
    case NonFatal(e) => isValid = false
      invalidMessages.append(e.getMessage).append("\n")
  }
  schemaConfig.fields.foreach { field =>
    if (field.name.isDefined) {
      if (field.regex.isDefined) {
        isValid = false
        invalidMessages.append("""Both "name" and "regex" keys are defined in a field,"""
          + " but only one may be.\n")
      }
    } else if (field.regex.isEmpty) {
      isValid = false
      invalidMessages.append("""Neither "name" nor "regex" key is defined in a field,""").
        append(" but one must be.\n")
    }
    if (schemaConfig.namedAnalyzerConfigs.get(field.analyzer).isEmpty) {
      def badAnalyzerMessage(suffix: String): Unit = {
        invalidMessages.append(s"""field "${field.fieldRef}": """)
          .append(s""" analyzer "${field.analyzer}" """).append(suffix)
      }
      try { // Attempt to interpret the analyzer as a fully qualified class name
        Utils.classForName(field.analyzer).asInstanceOf[Class[_ <: Analyzer]]
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
      var fieldConfig = schemaConfig.namedFields.get(fieldName)
      if (fieldConfig.isEmpty) {
        breakable {
          schemaConfig.fields.filter(c => c.regex.isDefined).foreach { field =>
            if (field.pattern matcher fieldName matches()) {
              fieldConfig = Some(field)
              break
            }
          }
        }
      }
      if (fieldConfig.isDefined) {
        val analyzerConfig = schemaConfig.namedAnalyzerConfigs.get(fieldConfig.get.analyzer)
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
            val clazz = Utils.classForName(fieldConfig.get.analyzer)
            analyzer = Some(clazz.newInstance.asInstanceOf[Analyzer])
            schemaConfig.defaultLuceneMatchVersion foreach { version =>
              analyzer.get.setVersion(LuceneVersion.parseLeniently(version))
            }
          } catch {
            case NonFatal(e) => isValid = false
              val writer = new StringWriter
              writer.write(s"Exception initializing analyzer '${fieldConfig.get.analyzer}': ")
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
private case class AnalyzerConfig(name: String,
                                  charFilters: Option[List[Map[String, String]]],
                                  tokenizer: Map[String, String],
                                  filters: Option[List[Map[String, String]]])
private case class FieldConfig(regex: Option[String], name: Option[String], analyzer: String) {
  val pattern: Pattern = regex.map(_.r.pattern).orNull
  val fieldRef: String = name.getOrElse(regex.get)
}
private case class SchemaConfig(defaultLuceneMatchVersion: Option[String],
                                analyzers: List[AnalyzerConfig],
                                fields: List[FieldConfig]) {
  val namedAnalyzerConfigs: Map[String, AnalyzerConfig] = analyzers.map(a => a.name -> a).toMap
  val namedFields: Map[String, FieldConfig]
    = fields.filter(c => c.name.isDefined).map(c => c.name.get -> c).toMap
}
