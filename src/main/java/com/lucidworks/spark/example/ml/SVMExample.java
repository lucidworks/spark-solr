package com.lucidworks.spark.example.ml;

import com.lucidworks.spark.SparkApp;
import com.lucidworks.spark.analysis.LuceneTextAnalyzer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import org.apache.spark.mllib.feature.Normalizer;
import org.apache.spark.mllib.feature.StandardScaler;
import org.apache.spark.mllib.feature.StandardScalerModel;

import java.io.Serializable;
import java.util.*;

public class SVMExample implements SparkApp.RDDProcessor {

  private static final String DEFAULT_NUM_FEATURES = "1000000";
  private static final String DEFAULT_NUM_ITERATIONS = "200";

  class RowToLabeledPoint implements Serializable, Function<Row, LabeledPoint> {

    private String[] inputCols;
    private String textAnalyzerJson;
    private HashingTF hashingTF;
    private Normalizer normalizer =  new Normalizer();
    private transient LuceneTextAnalyzer textAnalyzer;

    RowToLabeledPoint(String[] inputCols, int numFeatures, String textAnalyzerJson) {
      this.inputCols = inputCols;
      this.hashingTF = new HashingTF(numFeatures);
      this.textAnalyzerJson = textAnalyzerJson;
    }

    public LabeledPoint call(Row row) throws Exception {
      String polarity = row.getString(row.fieldIndex("polarity"));

      Map<String,String> fields = new HashMap<String, String>();
      for (String col : inputCols) {
        String val = row.getString(row.fieldIndex(col));
        if (val != null) {
          fields.put(col, val);
        }
      }

      // eval it lazily to maintain serializability of this Function
      if (textAnalyzer == null) {
        textAnalyzer = new LuceneTextAnalyzer(textAnalyzerJson);
      }

      Map<String,List<String>> analyzedFields = textAnalyzer.analyzeJava(fields);
      List<String> terms = new LinkedList<>();
      for (List<String> termsPerField : analyzedFields.values()) {
        terms.addAll(termsPerField);
      }

      double sentimentLabel = "0".equals(polarity) ? 0d : 1d;
      return new LabeledPoint(sentimentLabel, normalizer.transform(hashingTF.transform(terms)));
    }
  };

  public String getName() {
    return "mllib-svm";
  }

  public Option[] getOptions() {
    return new Option[] {
            Option.builder()
                    .hasArg()
                    .required(false)
                    .desc("Path to training data to index")
                    .longOpt("indexTrainingData")
                    .build(),
            Option.builder()
                    .hasArg()
                    .required(false)
                    .desc("Path to test data to index")
                    .longOpt("indexTestData")
                    .build(),
            Option.builder()
                    .hasArg()
                    .required(false)
                    .desc("Fraction (0 to 1) of full dataset to sample from Solr, default is 1")
                    .longOpt("sample")
                    .build(),
            Option.builder()
                    .hasArg()
                    .required(false)
                    .desc("Number of features; default is " + DEFAULT_NUM_FEATURES)
                    .longOpt("numFeatures")
                    .build(),
            Option.builder()
                    .hasArg()
                    .required(false)
                    .desc("Number of iterations; default is "+DEFAULT_NUM_ITERATIONS)
                    .longOpt("numIterations")
                    .build(),
            Option.builder()
                    .hasArg()
                    .required(false)
                    .desc("Model output path; default is mllib-svm-sentiment")
                    .longOpt("modelOutput")
                    .build()
    };
  }

  public int run(SparkConf conf, CommandLine cli) throws Exception {
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    String zkHost = cli.getOptionValue("zkHost", "localhost:9983");
    String collection = cli.getOptionValue("collection", "twitter_sentiment");

    StructType csvSchema = new StructType(new StructField[] {
            new StructField("polarity", DataTypes.StringType, true, Metadata.empty()),
            new StructField("id", DataTypes.StringType, true, Metadata.empty()),
            new StructField("date", DataTypes.StringType, true, Metadata.empty()),
            new StructField("query", DataTypes.StringType, true, Metadata.empty()),
            new StructField("username", DataTypes.StringType, true, Metadata.empty()),
            new StructField("tweet_txt", DataTypes.StringType, true, Metadata.empty())
    });

    // note: get training data from:
    // http://help.sentiment140.com/for-students
    // extract the zip locally and point to it with command-line opts
    //
    // this is just for example purposes as the Area Under ROC from this model is 0.65, which is only slightly
    // better than random :-(
    String indexTrainingData = cli.getOptionValue("indexTrainingData");
    if (indexTrainingData != null) {

      DataFrame csvDF = sqlContext.read()
              .format("com.databricks.spark.csv")
              .schema(csvSchema)
              .option("header", "false")
              .load(indexTrainingData);
      csvDF = csvDF.repartition(4);
      Map<String, String> options = new HashMap<>();
      options.put("zkhost", zkHost);
      options.put("collection", collection);
      options.put("soft_commit_secs", "10");
      csvDF.write().format("solr").options(options).mode(SaveMode.Overwrite).save();
    }

    String indexTestData = cli.getOptionValue("indexTestData");
    if (indexTestData != null) {
      DataFrame csvDF = sqlContext.read()
              .format("com.databricks.spark.csv")
              .schema(csvSchema)
              .option("header", "false")
              .load(indexTestData);
      csvDF = csvDF.withColumnRenamed("polarity","test_polarity");
      Map<String, String> options = new HashMap<>();
      options.put("zkhost", zkHost);
      options.put("collection", collection);
      options.put("soft_commit_secs", "10");
      csvDF.write().format("solr").options(options).mode(SaveMode.Overwrite).save();
    }

    final String contentFields = "tweet_txt";

    Map<String, String> options = new HashMap<>();
    options.put("zkhost", zkHost);
    options.put("collection", collection);
    options.put("query", "+polarity:(0 OR 4) +tweet_txt:[* TO *]");
    options.put("fields", "id,polarity,tweet_txt");
    options.put("rows", "10000");
    options.put("splits", "true");
    options.put("split_field", "_version_");
    options.put("splits_per_shard", "8");

    double sampleFraction = Double.parseDouble(cli.getOptionValue("sample", "1.0"));
    DataFrame trainingDataFromSolr = sqlContext.read().format("solr").options(options).load();
    trainingDataFromSolr = trainingDataFromSolr.sample(false, sampleFraction);
    //trainingDataFromSolr.show();

    final String[] inputCols = contentFields.split(",");
    for (int i = 0 ; i < inputCols.length ; ++i) {
      inputCols[i] = inputCols[i].trim();
    }

    // use the standard tokenizer
    String stdTokLowerSchema = json(
            "{ 'analyzers': [{ 'name': 'std_tok_lower', 'tokenizer': { 'type': 'standard' },\n" +
                    "                'filters': [{ 'type': 'lowercase' }]}],\n" +
                    "  'fields': [{ 'regex': '.+', 'analyzer': 'std_tok_lower' }]}\n");

    final int numFeatures = Integer.parseInt(cli.getOptionValue("numFeatures", DEFAULT_NUM_FEATURES));
    final Function<Row, LabeledPoint> mapFunc = new RowToLabeledPoint(inputCols, numFeatures, stdTokLowerSchema);
    final int numIterations = Integer.parseInt(cli.getOptionValue("numIterations", DEFAULT_NUM_ITERATIONS));
    JavaRDD<LabeledPoint> trainingData = trainingDataFromSolr.javaRDD().map(mapFunc);
    final StandardScalerModel standardScaler = new StandardScaler().fit(trainingData.map(new Function<LabeledPoint, org.apache.spark.mllib.linalg.Vector>() {
      public Vector call(LabeledPoint labpt) {
        return labpt.features();
      }
    }).rdd());
    RDD<LabeledPoint> trainRDD = trainingData.map(new Function<LabeledPoint, LabeledPoint>() {
      public LabeledPoint call(LabeledPoint lp) {
        return new LabeledPoint(lp.label(), standardScaler.transform(lp.features()));
      }
    }).rdd();

    trainRDD = trainRDD.persist(StorageLevel.MEMORY_ONLY_SER());
    final SVMModel model = SVMWithSGD.train(trainRDD, numIterations);

    // Clear the default threshold.
    //model.clearThreshold();

    options = new HashMap<>();
    options.put("zkhost", zkHost);
    options.put("collection", collection);

    // find docs that don't have polarity
    options.put("query", "+test_polarity:[* TO *] +tweet_txt:[* TO *]");
    options.put("fields", "id,test_polarity,tweet_txt");
    DataFrame testDataFromSolr = sqlContext.read().format("solr").options(options).load();
    testDataFromSolr = testDataFromSolr.withColumnRenamed("test_polarity", "polarity");
    testDataFromSolr.show();

    JavaRDD<LabeledPoint> testVectors = testDataFromSolr.javaRDD().map(mapFunc).map(new Function<LabeledPoint, LabeledPoint>() {
      public LabeledPoint call(LabeledPoint lp) {
        return new LabeledPoint(lp.label(), standardScaler.transform(lp.features()));
      }
    });

    // Compute raw scores on the test set.
    JavaRDD<Tuple2<Object, Object>> scoreAndLabels = testVectors.map(
      new Function<LabeledPoint, Tuple2<Object, Object>>() {
        public Tuple2<Object, Object> call(LabeledPoint p) {
          Double score = model.predict(p.features());
          System.out.println(">> model predicted: "+score+", actual: "+p.label());
          return new Tuple2<Object, Object>(score, p.label());
        }
      }
    );

    // Get evaluation metrics.
    BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
    double auROC = metrics.areaUnderROC();
    System.out.println("Area under ROC = " + auROC);

    // Save and load model
    model.save(jsc.sc(), cli.getOptionValue("modelOutput", "mllib-svm-sentiment"));
    return 0;
  }

  private String json(String singleQuoted) {
    return singleQuoted.replaceAll("'", "\"");
  }

}
