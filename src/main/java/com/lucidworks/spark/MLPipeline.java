package com.lucidworks.spark;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.OneVsRest;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;

public class MLPipeline implements SparkApp.RDDProcessor {

  @Override
  public String getName() {
    return "ml-pipeline";
  }

  @Override
  public Option[] getOptions() {
    return new Option[]{
        OptionBuilder
            .withArgName("QUERY")
            .hasArg()
            .isRequired(false)
            .withDescription("Query to identify documents in the training set")
            .create("query"),
        OptionBuilder
            .withArgName("FIELD")
            .hasArg()
            .isRequired(false)
            .withDescription("Field in Solr containing the label for each document in the training set")
            .create("labelField"),
        OptionBuilder
            .withArgName("FIELD")
            .hasArg()
            .isRequired(false)
            .withDescription("Field in Solr containing the text content for each document in the training set")
            .create("contentField")
    };
  }

  @Override
  public int run(SparkConf conf, CommandLine cli) throws Exception {

    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    String zkHost = cli.getOptionValue("zkHost", "localhost:2181/fusion21");
    String collection = cli.getOptionValue("collection", "ml20news");
    String queryStr = cli.getOptionValue("query", "content_txt_en:[* TO *] AND newsgroup_s:[* TO *]");
    final String labelField = cli.getOptionValue("labelField", "newsgroup_s");
    final String contentField = cli.getOptionValue("contentField", "content_txt_en");

    Map<String, String> options = new HashMap<String, String>();
    options.put("zkhost", zkHost);
    options.put("collection", collection);
    options.put("query", queryStr);
    options.put("fields", "id,"+labelField+","+contentField);

    DataFrame solrData = sqlContext.read().format("solr").options(options).load();

    // Configure an ML pipeline, which consists of the following stages:
    // index string labels, tokenizer, hashingTF, classifier model, convert predictions to string labels.

    // ML needs labels as numeric (double) indexes ... our training data has string labels, convert using a StringIndexer
    // see: https://spark.apache.org/docs/1.5.1/api/java/index.html?org/apache/spark/ml/feature/StringIndexer.html
    StringIndexerModel labelIndexer = new StringIndexer()
        .setInputCol(labelField)
        .setOutputCol("label")
        .fit(solrData);

    // tokenize text from Solr into words using built-in
    // TODO: provide option to use any Lucene Analyzer / Solr FieldType here
    Tokenizer tokenizer = new Tokenizer()
        .setInputCol(contentField)
        .setOutputCol("words");

    // Vectorize!
    HashingTF hashingTF = new HashingTF()
        .setInputCol(tokenizer.getOutputCol())
        .setOutputCol("features");

    // ML pipelines don't provide stages for all algorithms yet, such as NaiveBayes?

    LogisticRegression lr = new LogisticRegression()
        .setMaxIter(10);

    // to support 20 newsgroups
    OneVsRest ovr = new OneVsRest().setClassifier(lr);
    ovr.setLabelCol("label");

    IndexToString labelConverter = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("predictedLabel")
        .setLabels(labelIndexer.labels());

    Pipeline pipeline = new Pipeline()
        .setStages(new PipelineStage[]{labelIndexer, tokenizer, hashingTF, ovr, labelConverter});

    DataFrame[] splits = solrData.randomSplit(new double[] {0.7, 0.3});
    DataFrame trainingData = splits[0];
    DataFrame testData = splits[1];

    MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("precision");

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
    // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
    ParamMap[] paramGrid = new ParamGridBuilder()
        .addGrid(hashingTF.numFeatures(), new int[]{100, 1000, 10000})
        .addGrid(lr.regParam(), new double[]{0.1, 0.01})
        .build();

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    CrossValidator cv = new CrossValidator()
        .setEstimator(pipeline)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(paramGrid)
        .setNumFolds(3); // Use 3+ in practice

    CrossValidatorModel cvModel = cv.fit(trainingData);
    DataFrame predictions = cvModel.transform(testData);
    predictions.cache();

    double accuracyCrossFold = evaluator.evaluate(predictions);
    System.out.println("Cross-Fold Test Error = " + (1.0 - accuracyCrossFold));

    // TODO: remove - debug
    for (Row r : predictions.select("id", labelField, "predictedLabel").sample(false, 0.1).collect()) {
      System.out.println(r.get(0) + ": actual="+r.get(1)+", predicted=" + r.get(2));
    }

    MulticlassMetrics metrics = new MulticlassMetrics(predictions.select("prediction", "label"));
    Matrix confusionMatrix = metrics.confusionMatrix();

    // output the Confusion Matrix
    System.out.println("Confusion Matrix");
    System.out.println(confusionMatrix);

    // compute the false positive rate per label
    System.out.println();
    System.out.println("F-Measure: "+metrics.fMeasure());
    System.out.println("label\tfpr\n");

    String[] labels = labelConverter.getLabels();
    for (int i = 0; i < labels.length; i++) {
      System.out.print(labels[i]);
      System.out.print("\t");
      System.out.print(metrics.falsePositiveRate((double)i));
      System.out.println();
    }

    return 0;
  }
}
