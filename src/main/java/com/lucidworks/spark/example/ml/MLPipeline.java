package com.lucidworks.spark.example.ml;

import com.lucidworks.spark.SparkApp;
import com.lucidworks.spark.ml.feature.LuceneTextAnalyzerTransformer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.OneVsRest;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConversions$;

public class MLPipeline implements SparkApp.RDDProcessor {

  @Override
  public String getName() {
    return "ml-pipeline";
  }

  @Override
  public Option[] getOptions() {
    return new Option[] {
      Option.builder()
            .hasArg()
            .required(false)
            .desc("Query to identify documents in the training set")
            .longOpt("query")
            .build(),
      Option.builder()
            .hasArg()
            .required(false)
            .desc("Field in Solr containing the label for each document in the training set")
            .longOpt("labelField")
            .build(),
      Option.builder()
            .hasArg()
            .required(false)
            .desc("Comma-separated list of field(s) in Solr containing the text content for each document in the training set")
            .longOpt("contentFields")
            .build(),
      Option.builder()
            .hasArg()
            .required(false)
            .desc("Classifier type: either NaiveBayes or LogisticRegression")
            .longOpt("classifier")
            .build(),
      Option.builder()
            .hasArg()
            .required(false)
            .desc("Fraction (0 to 1) of full dataset to sample from Solr, default is 1")
            .longOpt("sample")
            .build()
    };
  }

  @Override
  public int run(SparkConf conf, CommandLine cli) throws Exception {

    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

    String zkHost = cli.getOptionValue("zkHost", "localhost:9983");
    String collection = cli.getOptionValue("collection", "ml20news");
    String queryStr = cli.getOptionValue("query", "content_txt:[* TO *] AND newsgroup_s:[* TO *]");
    final String labelField = cli.getOptionValue("labelField", "newsgroup_s");
    final String contentFields = cli.getOptionValue("contentFields", "content_txt,subject");

    Map<String, String> options = new HashMap<>();
    options.put("zkhost", zkHost);
    options.put("collection", collection);
    options.put("query", queryStr);
    options.put("fields", "id," + labelField + "," + contentFields);

    double sampleFraction = Double.parseDouble(cli.getOptionValue("sample", "1.0"));
    Dataset solrData = sparkSession.read().format("solr").options(options).load();
    solrData = solrData.sample(false, sampleFraction);

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

    String whitespaceTokSchema = json(
            "{ 'analyzers': [{ 'name': 'ws_tok', 'tokenizer': { 'type': 'whitespace' }}],\n" +
                    "'fields': [{ 'regex': '.+', 'analyzer': 'ws_tok' }]}\n");
    String stdTokLowerSchema = json(
            "{ 'analyzers': [{ 'name': 'std_tok_lower', 'tokenizer': { 'type': 'standard' },\n" +
                    "                'filters': [{ 'type': 'lowercase' }]}],\n" +
                    "  'fields': [{ 'regex': '.+', 'analyzer': 'std_tok_lower' }]}\n");
    List<String> analysisSchemas = Arrays.asList(whitespaceTokSchema, stdTokLowerSchema);

    LuceneTextAnalyzerTransformer analyzer = new LuceneTextAnalyzerTransformer()
            .setInputCols(inputCols)
            .setOutputCol("words");

    // Vectorize!
    HashingTF hashingTF = new HashingTF()
        .setInputCol("words")
        .setOutputCol("features");

    // ML pipelines don't provide stages for all algorithms yet, such as NaiveBayes?
    PipelineStage estimatorStage = null;

    if ("NaiveBayes".equals(cli.getOptionValue("classifier", "LogisticRegression"))) {
      NaiveBayes nb = new NaiveBayes();
      estimatorStage = nb;
    } else {
      LogisticRegression lr = new LogisticRegression().setMaxIter(10);

      // to support 20 newsgroups
      OneVsRest ovr = new OneVsRest().setClassifier(lr);
      ovr.setLabelCol("label");

      estimatorStage = ovr;
    }

    System.out.println("Using estimator: "+estimatorStage);

    IndexToString labelConverter = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("predictedLabel")
        .setLabels(labelIndexer.labels());

    Pipeline pipeline = new Pipeline()
        .setStages(new PipelineStage[]{labelIndexer, analyzer, hashingTF, estimatorStage, labelConverter});

    Dataset[] splits = solrData.randomSplit(new double[] {0.7, 0.3});
    Dataset trainingData = splits[0];
    Dataset testData = splits[1];

    MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("precision");

    // We use a ParamGridBuilder to construct a grid of parameters to search over,
    // with 3 values for hashingTF.numFeatures, 2 values for lr.regParam, 2 values for
    // analyzer.analysisSchema, and both possibilities for analyzer.prefixTokensWithInputCol.
    // This grid will have 3 x 2 x 2 x 2 = 24 parameter settings for CrossValidator to choose from.
    ParamGridBuilder paramGridBuilder = new ParamGridBuilder()
        .addGrid(hashingTF.numFeatures(), new int[]{1000, 5000})
        .addGrid(analyzer.analysisSchema(), JavaConversions$.MODULE$.collectionAsScalaIterable(analysisSchemas))
        .addGrid(analyzer.prefixTokensWithInputCol());

    if (estimatorStage instanceof LogisticRegression) {
      LogisticRegression lr = (LogisticRegression)estimatorStage;
      paramGridBuilder.addGrid(lr.regParam(), new double[]{0.1, 0.01});
    } else if (estimatorStage instanceof NaiveBayes) {
      NaiveBayes nb = (NaiveBayes)estimatorStage;
      paramGridBuilder.addGrid(nb.smoothing(), new double[]{1.0, 0.5});
    }

    ParamMap[] paramGrid = paramGridBuilder.build();

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

    // save it to disk
    cvModel.write().overwrite().save("ml-pipeline-model");

    // read it off disk
    cvModel = CrossValidatorModel.load("ml-pipeline-model");

    Dataset predictions = cvModel.transform(testData);
    predictions.cache();

    double accuracyCrossFold = evaluator.evaluate(predictions);
    System.out.println("Cross-Fold Test Error = " + (1.0 - accuracyCrossFold));

   // TODO: remove - debug
    for (Object o : predictions.select("id", labelField, "predictedLabel").sample(false, 0.1).toDF().collectAsList()) {
      System.out.println(((Row) o).get(0) + ": actual=" + ((Row) o).get(1) + ", predicted=" + ((Row) o).get(2));
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
