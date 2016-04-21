package com.lucidworks.spark.example.ml

import com.lucidworks.spark.SparkApp
import com.lucidworks.spark.analysis.LuceneTextAnalyzer
import org.apache.commons.cli.{CommandLine, Option}
import org.apache.spark.mllib.classification.{SVMWithSGD, SVMModel}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
import org.apache.spark.api.java.function.Function
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.feature.StandardScalerModel
import scala.collection.JavaConverters._
import scala.collection
import com.lucidworks.spark.fusion.FusionMLModelSupport

import scala.collection.immutable

/**
 * A SVM example for text classification. This class has the following feature transformations
 * LuceneAnalyzer
 * HashingTF
 * Normalizer
 * StandardScaler
 * The SVM Model is trained after the above transformations and is tested for the given test data (using same transformations).
 */

object SVMExample {
  val DEFAULT_NUM_FEATURES = "1000000"
  val DEFAULT_NUM_ITERATIONS = "200"
  val DefaultZkHost = "localhost:9983"
  val DefaultCollection = "twitter_sentiment"
}

class SVMExample extends SparkApp.RDDProcessor  {

  import SVMExample._
  def getName = "mllib-svm-scala"
  def getOptions = Array(
    Option.builder().longOpt("indexTrainingData").hasArg.required(false).desc(
      s"Path to training data to index").build(),
    Option.builder().longOpt("indexTestData").hasArg.required(false).desc(
      s"Path to test data to index").build(),
    Option.builder().longOpt("sample").hasArg.required(false).desc(
      s"Fraction (0 to 1) of full dataset to sample from Solr, default is 1").build(),
    Option.builder().longOpt("numFeatures").hasArg.required(false).desc(
      s"Number of features; default is $DEFAULT_NUM_FEATURES").build(),
    Option.builder().longOpt("numIterations").hasArg.required(false).desc(
      s"Number of iterations; default is $DEFAULT_NUM_ITERATIONS").build(),
    Option.builder().longOpt("modelOutput").hasArg.required(false).desc(
      s"Model output path; default is mllib-svm-sentiment").build(),
    Option.builder().longOpt("fusionHostAndPort").hasArg.required(false).desc(
      s"Fusion host and port; Example localhost:8764").build(),
    Option.builder().longOpt("fusionUser").hasArg.required(false).desc(
      s"Fusion user name").build(),
    Option.builder().longOpt("fusionPassword").hasArg.required(false).desc(
      s"Fusion password").build(),
    Option.builder().longOpt("fusionRealm").hasArg.required(false).desc(
      s"Fusion Realm").build()
  )

  override def run(conf: SparkConf, cli: CommandLine): Int = {
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val csvSchema = StructType(StructField("polarity",StringType, true) ::
      StructField("id",StringType, true) ::
      StructField("date",StringType, true) ::
      StructField("query",StringType, true) ::
      StructField("username",StringType, true) ::
      StructField("tweet_txt",StringType, true) :: Nil)

    val writeoptions = immutable.HashMap(
      "zkhost" -> cli.getOptionValue("zkHost", DefaultZkHost),
      "collection" -> cli.getOptionValue("collection", DefaultCollection),
      "soft_commit_secs" -> "10")

    val indexTrainingData = cli.getOptionValue("indexTrainingData")
    if (indexTrainingData != null) {
      var csvDF = sqlContext.read.format("com.databricks.spark.csv").schema(csvSchema).option("header", "false").load(indexTrainingData)
      csvDF = csvDF.repartition(4)

      csvDF.write.format("solr").options(writeoptions).mode(SaveMode.Overwrite).save()
    }

    val indexTestData = cli.getOptionValue("indexTestData");
    if (indexTestData != null) {
      var csvDF = sqlContext.read.format("com.databricks.spark.csv").schema(csvSchema).option("header", "false").load(indexTestData)
      csvDF = csvDF.withColumnRenamed("polarity", "test_polarity")

      csvDF.write.format("solr").options(writeoptions).mode(SaveMode.Overwrite).save()
    }

    val contentFields = "tweet_txt"

    var trainoptions = immutable.HashMap(
      "zkhost" -> cli.getOptionValue("zkHost", DefaultZkHost),
      "collection" -> cli.getOptionValue("collection", DefaultCollection),
      "query" -> "+polarity:(0 OR 4) +tweet_txt:[* TO *]",
      "fields" -> "id,polarity,tweet_txt",
      "rows" -> "10000",
      "splits" -> "true",
      "split_field" -> "_version_",
      "splits_per_shard" -> "8")

    val sampleFraction = cli.getOptionValue("sample", "1.0").toDouble
    var trainingDataFromSolr = sqlContext.read.format("solr").options(trainoptions).load()
    trainingDataFromSolr = trainingDataFromSolr.sample(false, sampleFraction)

    val inputCols = contentFields.split(" ").map(_.trim)

    val stdTokLowerSchema = "{ \"analyzers\": [{ \"name\": \"std_tok_lower\", \"tokenizer\": { \"type\": \"standard\" },\n" +
      "                \"filters\": [{ \"type\": \"lowercase\" }]}],\n" +
      "  \"fields\": [{ \"regex\": \".+\", \"analyzer\": \"std_tok_lower\" }]}\n"

    val numFeatures = cli.getOptionValue("numFeatures", DEFAULT_NUM_FEATURES).toInt
    val numIterations = cli.getOptionValue("numIterations", DEFAULT_NUM_ITERATIONS).toInt

    def RowtoLab(row: Row, numFeatures: Int, inputCols: Array[String], stdTokLowerSchema: String ): LabeledPoint = {
      var textAnalyzer: LuceneTextAnalyzer = new LuceneTextAnalyzer(stdTokLowerSchema)
      var hashingTF = new HashingTF(numFeatures)
      var normalizer = new Normalizer()
      val polarity = row.getString(row.fieldIndex("polarity"))
      var fields = new java.util.HashMap[String, String]()
      for(i <- 0 until inputCols.length){
        val value = row.getString(row.fieldIndex(inputCols(i)))
        if (value != null) {
          fields.put(inputCols(i), value)
        }
      }
      val analyzedFields = textAnalyzer.analyzeJava(fields)
      var terms = new java.util.LinkedList[String]()
      analyzedFields.values().asScala.toList.foreach(v => terms.addAll(v))
      val sentimentLabel = if (("0" == polarity)) 0.toDouble else 1.toDouble
      new LabeledPoint(sentimentLabel, normalizer.transform(hashingTF.transform(terms)))
    }

    var trainingData = trainingDataFromSolr.rdd.map(row => RowtoLab(row, numFeatures, inputCols, stdTokLowerSchema))
    var standardScaler = new StandardScaler().fit(trainingData.map(x => x.features))
    var trainRDD = trainingData.map(x => new LabeledPoint(x.label, standardScaler.transform(x.features)))
    trainRDD = trainRDD.persist(StorageLevel.MEMORY_ONLY_SER)
    val model = SVMWithSGD.train(trainRDD, numIterations)

    var testoptions = immutable.HashMap("zkhost" -> cli.getOptionValue("zkHost", DefaultZkHost),
      "collection" -> cli.getOptionValue("collection", DefaultCollection),
      "query" -> "+test_polarity:[* TO *] +tweet_txt:[* TO *]",
      "fields" -> "id,test_polarity,tweet_txt")

    var testDataFromSolr = sqlContext.read.format("solr").options(testoptions).load()
    testDataFromSolr = testDataFromSolr.withColumnRenamed("test_polarity", "polarity")
    testDataFromSolr.show
    val testVectors = testDataFromSolr.rdd.map(row => RowtoLab(row, numFeatures, inputCols, stdTokLowerSchema)).map(x => new LabeledPoint(x.label, standardScaler.transform(x.features)))

    val scoreAndLabels =  testVectors.map(p => {
      val score = model.predict(p.features)
      println(">> model predicted: " + score + ", actual: " + p.label)
      new (Double, Double)(score, p.label)
    })

    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC
    println("Area under ROC = " + auROC)

    if (cli.getOptionValue("fusionHostAndPort") != null) {
      var metadata = new java.util.HashMap[String, String]()
      metadata.put("numFeatures", "1000000")
      metadata.put("featureFields", "tweet_txt")
      metadata.put("analyzerJson", stdTokLowerSchema)
      metadata.put("normalizer", "Y")
      metadata.put("standardscaler", "Y")
      metadata.put("mean", standardScaler.mean.toString)
      metadata.put("std", standardScaler.std.toString)

      if (cli.getOptionValue("fusionUser") != null && cli.getOptionValue("fusionPassword") != null) {
        FusionMLModelSupport.saveModelInFusion(cli.getOptionValue("fusionHostAndPort"), cli.getOptionValue("fusionUser"), cli.getOptionValue("fusionPassword"),
          cli.getOptionValue("fusionRealm", "native"), sc, cli.getOptionValue("modelOutput", "mllib-svm-sentiment"), model, metadata)
      } else {
        FusionMLModelSupport.saveModelInLocalFusion(sc, cli.getOptionValue("modelOutput", "mllib-svm-sentiment"), model, metadata)
      }
    }
    else {
      model.save(sc, cli.getOptionValue("modelOutput", "mllib-svm-sentiment"))
    }

    return 0
  }

}
