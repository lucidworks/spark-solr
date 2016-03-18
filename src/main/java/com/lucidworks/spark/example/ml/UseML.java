package com.lucidworks.spark.example.ml;

import com.lucidworks.spark.SparkApp;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UseML implements SparkApp.RDDProcessor {

  @Override
  public String getName() {
    return "use-ml";
  }

  @Override
  public Option[] getOptions() {
    return new Option[0];
  }

  @Override
  public int run(SparkConf conf, CommandLine cli) throws Exception {

    long startMs = System.currentTimeMillis();

    conf.set("spark.ui.enabled", "false");

    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    long diffMs = (System.currentTimeMillis() - startMs);
    System.out.println(">> took "+diffMs+" ms to create SQLContext");

    Map<String, String> options = new HashMap<>();
    options.put("zkhost", "localhost:9983");
    options.put("collection", "ml20news");
    options.put("query", "content_txt:[* TO *]");
    options.put("fields", "content_txt");

    DataFrame solrData = sqlContext.read().format("solr").options(options).load();
    DataFrame sample = solrData.sample(false, 0.1d, 5150).select("content_txt");
    List<Row> rows = sample.collectAsList();
    System.out.println(">> loaded "+rows.size()+" docs to classify");

    StructType schema = sample.schema();

    CrossValidatorModel cvModel =  CrossValidatorModel.load("ml-pipeline-model");
    PipelineModel bestModel = (PipelineModel) cvModel.bestModel();

    int r=0;
    startMs = System.currentTimeMillis();
    for (Row next : rows) {
      Row oneRow = RowFactory.create(next.getString(0));
      DataFrame oneRowDF = sqlContext.createDataFrame(Collections.<Row>singletonList(oneRow), schema);
      DataFrame scored = bestModel.transform(oneRowDF);
      Row scoredRow = scored.collect()[0];
      String predictedLabel = scoredRow.getString(scoredRow.fieldIndex("predictedLabel"));

      // an acutal app would save the predictedLabel
      //System.out.println(">> for row["+r+"], model returned "+scoredRows.length+" rows, "+scoredRows[0]);

      r++;
    }
    diffMs = (System.currentTimeMillis() - startMs);
    System.out.println(">> took "+diffMs+" ms to score "+rows.size()+" docs");

    return 0;
  }
}
