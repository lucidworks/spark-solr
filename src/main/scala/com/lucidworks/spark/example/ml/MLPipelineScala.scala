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
import org.apache.commons.cli.Option.{builder => OptionBuilder}
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.SparkSession

/** An example of building a spark.ml classification model to predict the newsgroup of
  * articles from the 20 newsgroups data (see [[http://qwone.com/~jason/20Newsgroups/]])
  * hosted in a Solr collection.
  *
  * == Prerequisites ==
  *
  * You must run `mvn -DskipTests package` in the spark-solr project, and you must download
  * a Spark 1.6.1 binary distribution and point the environment variable `$SPARK_HOME`
  * to the unpacked distribution directory.
  *
  * Follow the instructions in the [[NewsgroupsIndexer]] example's scaladoc to populate a Solr
  * collection with articles from the above-linked 20 newsgroup data.
  *
  * == Example invocation ==
  *
  * {{{
  *  $SPARK_HOME/bin/spark-submit --master 'local[2]' --class com.lucidworks.spark.SparkApp \
  *  target/spark-solr-2.0.0-SNAPSHOT-shaded.jar ml-pipeline-scala
  * }}}
  *
  * To see a description of all available options, run the following:
  *
  * {{{
  *  $SPARK_HOME/bin/spark-submit --class com.lucidworks.spark.SparkApp \
  *  target/spark-solr-2.0.0-SNAPSHOT-shaded.jar ml-pipeline-scala --help
  * }}}
  */
class MLPipelineScala extends SparkApp.RDDProcessor {
  import MLPipelineScala._
  def getName = "ml-pipeline-scala"
  def getOptions = Array(
    OptionBuilder().longOpt("query").hasArg.argName("QUERY").required(false).desc(
      s"Query to identify documents in the training set. Default: $DefaultQuery").build(),
    OptionBuilder().longOpt("labelField").hasArg.argName("FIELD").required(false).desc(
      s"Field containing the label in Solr training set documents. Default: $DefaultLabelField").build(),
    OptionBuilder().longOpt("contentFields").hasArg.argName("FIELDS").required(false).desc(
      s"Comma-separated list of text field(s) in Solr training set documents. Default: $DefaultContentFields").build(),
// CrossValidator model export only supports NaiveBayes in Spark 1.6.1, but in Spark 2.0,
// OneVsRest+LogisticRegression will be supported: https://issues.apache.org/jira/browse/SPARK-11892
//    OptionBuilder().longOpt("classifier").hasArg.argName("TYPE").required(false).desc(
//      s"Classifier type: either NaiveBayes or LogisticRegression. Default: $DefaultClassifier").build(),
    OptionBuilder().longOpt("sample").hasArg.argName("FRACTION").required(false).desc(
      s"Fraction (0 to 1) of full dataset to sample from Solr. Default: $DefaultSample").build(),
    OptionBuilder().longOpt("collection").hasArg.argName("NAME").required(false).desc(
      s"Solr source collection. Default: $DefaultCollection").build())

  override def run(conf: SparkConf, cli: CommandLine): Int = {
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val labelField = cli.getOptionValue("labelField", DefaultLabelField)
    val contentFields = cli.getOptionValue("contentFields", DefaultContentFields).split(",").map(_.trim)
    val sampleFraction = cli.getOptionValue("sample", DefaultSample).toDouble

    val options = Map(
      "zkhost" -> cli.getOptionValue("zkHost", DefaultZkHost),
      "collection" -> cli.getOptionValue("collection", DefaultCollection),
      "query" -> cli.getOptionValue("query", DefaultQuery),
      "fields" -> s"""id,$labelField,${contentFields.mkString(",")}""")
    val solrData = sparkSession.read.format("solr").options(options).load
    val sampledSolrData = solrData.sample(withReplacement = false, sampleFraction)

    // Configure an ML pipeline, which consists of the following stages:
    // index string labels, analyzer, hashingTF, classifier model, convert predictions to string labels.

    // ML needs labels as numeric (double) indexes ... our training data has string labels, convert using a StringIndexer
    // see: https://spark.apache.org/docs/1.6.0/api/java/index.html?org/apache/spark/ml/feature/StringIndexer.html
    val labelIndexer = new StringIndexer().setInputCol(labelField).setOutputCol(LabelCol).fit(sampledSolrData)
    val analyzer = new LuceneTextAnalyzerTransformer().setInputCols(contentFields).setOutputCol(WordsCol)

    // Vectorize!
    val hashingTF = new HashingTF().setInputCol(WordsCol).setOutputCol(FeaturesCol)

    val nb =  new NaiveBayes()
    val estimatorStage:PipelineStage = nb
    println(s"Using estimator: $estimatorStage")
    val labelConverter = new IndexToString().setInputCol(PredictionCol)
      .setOutputCol(PredictedLabelCol).setLabels(labelIndexer.labels)
    val pipeline = new Pipeline().setStages(Array(labelIndexer, analyzer, hashingTF, estimatorStage, labelConverter))
    val Array(trainingData, testData) = sampledSolrData.randomSplit(Array(0.7, 0.3))
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol(LabelCol)
      .setPredictionCol(PredictionCol).setMetricName("precision")

    // We use a ParamGridBuilder to construct a grid of parameters to search over,
    // with 3 values for hashingTF.numFeatures, 2 values for lr.regParam, 2 values for
    // analyzer.analysisSchema, and both possibilities for analyzer.prefixTokensWithInputCol.
    // This grid will have 3 x 2 x 2 x 2 = 24 parameter settings for CrossValidator to choose from.
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(1000, 5000))
      .addGrid(analyzer.analysisSchema, Array(WhitespaceTokSchema, StdTokLowerSchema))
      .addGrid(analyzer.prefixTokensWithInputCol)
      .addGrid(nb.smoothing, Array(1.0, 0.5)).build

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid).setNumFolds(3)
    val cvModel = cv.fit(trainingData)

    // save it to disk
    cvModel.write.overwrite.save("ml-pipeline-model")

    // read it off disk
    val loadedCvModel = CrossValidatorModel.load("ml-pipeline-model")

    val predictions = loadedCvModel.transform(testData)
    predictions.cache()

    val accuracyCrossFold = evaluator.evaluate(predictions)
    println(s"Cross-Fold Test Error = ${1.0 - accuracyCrossFold}")

    // TODO: remove - debug
    for (r <- predictions.select("id", labelField, PredictedLabelCol).sample(false, 0.1).collect) {
      println(s"${r(0)}: actual=${r(1)}, predicted=${r(2)}")
    }

    val metrics = new MulticlassMetrics(
      predictions
        .select(PredictionCol, LabelCol).rdd
        .map(r => (r.getDouble(0), r.getDouble(1))))

    // output the Confusion Matrix
    println(s"""Confusion Matrix
                          |${metrics.confusionMatrix}\n""".stripMargin)

    // compute the false positive rate per label
    println(s"""\nF-Measure: ${metrics.fMeasure}
                          |label\tfpr\n""".stripMargin)
    val labels = labelConverter.getLabels
    for (i <- labels.indices)
      println(s"${labels(i)}\t${metrics.falsePositiveRate(i.toDouble)}")

    0
  }
}
object MLPipelineScala {
  val LabelCol = "label"
  val WordsCol = "words"
  val PredictionCol = "prediction"
  val FeaturesCol = "features"
  val PredictedLabelCol = "predictedLabel"
  val DefaultZkHost = "localhost:9983"
  val DefaultQuery = "content_txt_en:[* TO *] AND newsgroup_s:[* TO *]"
  val DefaultLabelField = "newsgroup_s"
  val DefaultContentFields = "content_txt_en,Subject_txt_en"
  val DefaultCollection = "ml20news"
  val DefaultSample = "1.0"
  val WhitespaceTokSchema =
    """{ "analyzers": [{ "name": "ws_tok", "tokenizer": { "type": "whitespace" } }],
      |  "fields": [{ "regex": ".+", "analyzer": "ws_tok" }] }""".stripMargin
  val StdTokLowerSchema =
    """{ "analyzers": [{ "name": "std_tok_lower", "tokenizer": { "type": "standard" },
      |                  "filters": [{ "type": "lowercase" }] }],
      |  "fields": [{ "regex": ".+", "analyzer": "std_tok_lower" }] }""".stripMargin
}
