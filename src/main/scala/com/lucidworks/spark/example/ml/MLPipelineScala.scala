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

package com.lucidworks.spark.example.ml

import com.lucidworks.spark.SparkApp
import com.lucidworks.spark.ml.feature.LuceneTextAnalyzerTransformer
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.OneVsRest
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.SQLContext

import scala.collection.immutable

object MLPipelineScala {
  val LabelCol = "label"
  val WordsCol = "words"
  val PredictionCol = "prediction"
  val FeaturesCol = "features"
  val PredictedLabelCol = "predictedLabel"
  val DefaultZkHost = "localhost:2181/fusion21"
  val DefaultQuery = "content_txt_en:[* TO *] AND newsgroup_s:[* TO *]"
  val DefaultLabelField = "newsgroup_s"
  val DefaultContentFields = "content_txt_en,Subject_txt_en"
  val DefaultCollection = "ml20news"
  val WhitespaceTokSchema =
    """{ "analyzers": [{ "name": "ws_tok", "tokenizer": { "type": "whitespace" } }],
      |  "fields": [{ "regex": ".+", "analyzer": "ws_tok" }] }""".stripMargin
  val StdTokLowerSchema =
    """{ "analyzers": [{ "name": "std_tok_lower", "tokenizer": { "type": "standard" },
      |                  "filters": [{ "type": "lowercase" }] }],
      |  "fields": [{ "regex": ".+", "analyzer": "std_tok_lower" }] }""".stripMargin
}
class MLPipelineScala extends SparkApp.RDDProcessor {
  import MLPipelineScala._
  def getName = "ml-pipeline-scala"
  def getOptions = Array(
    Option.builder().longOpt("zkHost").hasArg.argName("HOST").required(false).desc(
      s"ZooKeeper connection string. Default: $DefaultZkHost").build(),
    Option.builder().longOpt("query").hasArg.argName("QUERY").required(false).desc(
      s"Query to identify documents in the training set. Default: $DefaultQuery").build(),
    Option.builder().longOpt("labelField").hasArg.argName("FIELD").required(false).desc(
      s"Field in containing the label in Solr training set documents. Default: $DefaultLabelField").build(),
    Option.builder().longOpt("contentFields").hasArg.argName("FIELDS").required(false).desc(
      s"Comma-separated list of text field(s) in Solr training set documents. Default: $DefaultContentFields").build())

  override def run(conf: SparkConf, cli: CommandLine): Int = {
    val jsc = new SparkContext(conf)
    val sqlContext = new SQLContext(jsc)
    val labelField = cli.getOptionValue("labelField", DefaultLabelField)
    val contentFields = cli.getOptionValue("contentFields", DefaultContentFields).split(",").map(_.trim)
    val options = immutable.HashMap(
      "zkhost" -> cli.getOptionValue("zkHost", DefaultZkHost),
      "collection" -> cli.getOptionValue("collection", DefaultCollection),
      "query" -> cli.getOptionValue("query", DefaultQuery),
      "fields" -> s"""id,$labelField,${contentFields.mkString(",")}""")
    val solrData = sqlContext.read.format("solr").options(options).load

    // Configure an ML pipeline, which consists of the following stages:
    // index string labels, analyzer, hashingTF, classifier model, convert predictions to string labels.

    // ML needs labels as numeric (double) indexes ... our training data has string labels, convert using a StringIndexer
    // see: https://spark.apache.org/docs/1.6.0/api/java/index.html?org/apache/spark/ml/feature/StringIndexer.html
    val labelIndexer = new StringIndexer().setInputCol(labelField).setOutputCol(LabelCol).fit(solrData)
    val analyzer = new LuceneTextAnalyzerTransformer().setInputCols(contentFields).setOutputCol(WordsCol)

    // Vectorize!
    val hashingTF = new HashingTF().setInputCol(analyzer.getOutputCol).setOutputCol(FeaturesCol)

    // ML pipelines don't provide stages for all algorithms yet, such as NaiveBayes?
    val lr = new LogisticRegression().setMaxIter(10).setLabelCol(LabelCol)

    // to support 20 newsgroups
    val ovr = new OneVsRest().setClassifier(lr)
    ovr.setLabelCol(LabelCol)
    val labelConverter = new IndexToString().setInputCol(PredictionCol)
      .setOutputCol(PredictedLabelCol).setLabels(labelIndexer.labels)
    val pipeline = new Pipeline().setStages(Array(labelIndexer, analyzer, hashingTF, ovr, labelConverter))
    val Array(trainingData, testData) = solrData.randomSplit(Array(0.7, 0.3))
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol(LabelCol)
      .setPredictionCol(PredictionCol).setMetricName("precision")

    // We use a ParamGridBuilder to construct a grid of parameters to search over,
    // with 3 values for hashingTF.numFeatures, 2 values for lr.regParam, 2 values for
    // analyzer.analysisSchema, and both possibilities for analyzer.prefixTokensWithInputCol.
    // This grid will have 3 x 2 x 2 x 2 = 24 parameter settings for CrossValidator to choose from.
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(1000, 25000, 100000))
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(analyzer.analysisSchema, Array(WhitespaceTokSchema, StdTokLowerSchema))
      .addGrid(analyzer.prefixTokensWithInputCol, Array(false, true)).build

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid).setNumFolds(3)
    val cvModel = cv.fit(trainingData)
    val predictions = cvModel.transform(testData)
    predictions.cache

    val accuracyCrossFold = evaluator.evaluate(predictions)
    System.out.println("Cross-Fold Test Error = " + (1.0 - accuracyCrossFold))

    // TODO: remove - debug
    for (r <- predictions.select("id", labelField, PredictedLabelCol).sample(false, 0.1).collect) {
      System.out.println(s"${r(0)}: actual=${r(1)}, predicted=${r(2)}")
    }

    val metrics = new MulticlassMetrics(
      predictions.select(PredictionCol, LabelCol).map(r => (r.getDouble(0), r.getDouble(1))))
    val confusionMatrix = metrics.confusionMatrix

    // output the Confusion Matrix
    System.out.println(s"""Confusion Matrix
                          |$confusionMatrix\n""".stripMargin)

    // compute the false positive rate per label
    System.out.println(s"""\nF-Measure: ${metrics.fMeasure}
                          |label\tfpr\n""".stripMargin)
    val labels = labelConverter.getLabels
    for (i <- labels.indices)
      System.out.println(s"${labels(i)}\t${metrics.falsePositiveRate(i.toDouble)}")

    0
  }
}
