package com.lucidworks.spark.fusion;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class FusionMLModelSupportTest {

  @Ignore
  @Test
  public void test() throws Exception {

    SparkConf sparkConf = new SparkConf().setAppName(getClass().getSimpleName());
    sparkConf.setMaster("local");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    String fusionHostAndPort = "localhost:8764";
    String fusionUser = "admin";
    String fusionPassword = "password123";
    String fusionRealm= "native";
    SparkContext sc = jsc.sc();
    String modelId = "my-spark-mllib-model";

    SVMModel svmModel = SVMModel.load(sc, "test-mllib-model");

    String stdAnalyzerJson =
            ("{ 'analyzers': [{ 'name': 'std_tok_lower', 'tokenizer': { 'type': 'standard' },\n" +
            "'filters': [{ 'type': 'lowercase' }]}],\n" +
            "  'fields': [{ 'regex': '.+', 'analyzer': 'std_tok_lower' }]}\n").replaceAll("'", "\"").replaceAll("\\s+"," ");

    Map<String,String> metadata = new HashMap<>();
    metadata.put("numFeatures", "1000000");
    metadata.put("featureFields", "tweet_txt");
    metadata.put("analyzerJson", stdAnalyzerJson);

    FusionMLModelSupport.saveModelInFusion(fusionHostAndPort,
            fusionUser, fusionPassword, fusionRealm, sc, modelId, svmModel, metadata);
  }
}
