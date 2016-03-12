package com.lucidworks.spark.example.ml;

import com.lucidworks.spark.SparkApp;
import com.lucidworks.spark.ml.feature.LuceneTextAnalyzerTransformer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.OneVsRest;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.collection.JavaConversions$;

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
            .withArgName("FIELDS")
            .hasArg()
            .isRequired(false)
            .withDescription("Comma-separated list of field(s) in Solr containing the text content for each document in the training set")
            .create("contentFields")
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
    final String contentFields = cli.getOptionValue("contentFields", "content_txt_en,Subject_txt_en");

    Map<String, String> options = new HashMap<>();
    options.put("zkhost", zkHost);
    options.put("collection", collection);
    options.put("query", queryStr);
    options.put("fields", "id," + labelField + "," + contentFields);

    DataFrame solrData = sqlContext.read().format("solr").options(options).load();

    // Configure an ML pipeline, which consists of the following stages:
    // index string labels, analyzer, hashingTF, classifier model, convert predictions to string labels.

    // ML needs labels as numeric (double) indexes ... our training data has string labels, convert using a StringIndexer
    // see: https://spark.apache.org/docs/1.6.0/api/java/index.html?org/apache/spark/ml/feature/StringIndexer.html
    StringIndexerModel labelIndexer = new StringIndexer()
        .setInputCol(labelField)
        .setOutputCol("label")
        .fit(solrData);

    String[] inputCols = contentFields.split(",");
    for (int i = 0 ; i < inputCols.length ; ++i) {
      inputCols[i] = inputCols[i].trim();
    }
    LuceneTextAnalyzerTransformer analyzer = new LuceneTextAnalyzerTransformer()
        .setInputCols(inputCols)
        .setOutputCol("words");
    String whitespaceTokSchema = json(
        "{ 'analyzers': [{ 'name': 'ws_tok', 'tokenizer': { 'type': 'whitespace' }}],\n" +
        "'fields': [{ 'regex': '.+', 'analyzer': 'ws_tok' }]}\n");
    String stdTokLowerSchema = json(
        "{ 'analyzers': [{ 'name': 'std_tok_lower', 'tokenizer': { 'type': 'standard' },\n" +
        "                'filters': [{ 'type': 'lowercase' }]}],\n" +
        "  'fields': [{ 'regex': '.+', 'analyzer': 'std_tok_lower' }]}\n");
    List<String> analysisSchemas = Arrays.asList(whitespaceTokSchema, stdTokLowerSchema);

    // Vectorize!
    HashingTF hashingTF = new HashingTF()
        .setInputCol(analyzer.getOutputCol())
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
        .setStages(new PipelineStage[]{labelIndexer, analyzer, hashingTF, ovr, labelConverter});

    DataFrame[] splits = solrData.randomSplit(new double[] {0.7, 0.3});
    DataFrame trainingData = splits[0];
    DataFrame testData = splits[1];

    MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("precision");

    // We use a ParamGridBuilder to construct a grid of parameters to search over,
    // with 3 values for hashingTF.numFeatures, 2 values for lr.regParam, 2 values for
    // analyzer.analysisSchema, and both possibilities for analyzer.prefixTokensWithInputCol.
    // This grid will have 3 x 2 x 2 x 2 = 24 parameter settings for CrossValidator to choose from.
    ParamMap[] paramGrid = new ParamGridBuilder()
        .addGrid(hashingTF.numFeatures(), new int[]{1000, 10000, 20000})
        .addGrid(lr.regParam(), new double[]{0.1, 0.01})
        .addGrid(analyzer.analysisSchema(), JavaConversions$.MODULE$.asScalaIterable(analysisSchemas))
        .addGrid(analyzer.prefixTokensWithInputCol())
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
    System.out.println("Best model params: " + Arrays.toString(cvModel.bestModel().params()));
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

  private String json(String singleQuoted) {
    return singleQuoted.replaceAll("'", "\"");
  }
}
