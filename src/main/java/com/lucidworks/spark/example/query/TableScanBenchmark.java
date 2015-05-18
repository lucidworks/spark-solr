package com.lucidworks.spark.example.query;

import com.lucidworks.spark.SolrRDD;
import com.lucidworks.spark.SparkApp;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.*;

/**
 * Example of how to query Solr and process the result set as a Spark RDD
 */
public class TableScanBenchmark implements SparkApp.RDDProcessor {

  public String getName() {
    return "query-solr-benchmark";
  }

  public Option[] getOptions() {
    return new Option[]{
      OptionBuilder
        .withArgName("QUERY")
        .hasArg()
        .isRequired(false)
        .withDescription("URL encoded Solr query to send to Solr, default is *:* (all docs)")
        .create("query"),
      OptionBuilder
        .withArgName("INT")
        .hasArg()
        .isRequired(false)
        .withDescription("Number of rows to fetch at once, default is 1000")
        .create("rows"),
      OptionBuilder
        .withArgName("INT")
        .hasArg()
        .isRequired(false)
        .withDescription("Number of splits per shard, default is 3")
        .create("splitsPerShard"),
      OptionBuilder
        .withArgName("NAME")
        .hasArg()
        .isRequired(false)
        .withDescription("Name of an indexed numeric field (preferably long type) used to split a shard, default is _version_")
        .create("splitField"),
      OptionBuilder
        .withArgName("LIST")
        .hasArg()
        .isRequired(false)
        .withDescription("Comma-delimited list of fields to be returned from the query; default is all fields")
        .create("fields")
    };
  }

  public int run(SparkConf conf, CommandLine cli) throws Exception {

    String zkHost = cli.getOptionValue("zkHost", "localhost:9983");
    String collection = cli.getOptionValue("collection", "collection1");
    String queryStr = cli.getOptionValue("query", "*:*");
    int rows = Integer.parseInt(cli.getOptionValue("rows", "1000"));
    int splitsPerShard = Integer.parseInt(cli.getOptionValue("splitsPerShard", "3"));
    String splitField = cli.getOptionValue("splitField", "_version_");

    JavaSparkContext jsc = new JavaSparkContext(conf);

    // TODO: Would be better to accept a JSON representation of a SolrQuery
    final SolrQuery solrQuery = new SolrQuery(queryStr);

    String fields = cli.getOptionValue("fields");
    if (fields != null)
      solrQuery.setFields(fields.split(","));

    List<SolrQuery.SortClause> sorts = new ArrayList<SolrQuery.SortClause>();
    sorts.add(new SolrQuery.SortClause("id", "asc"));
    solrQuery.setSorts(sorts);
    solrQuery.setRows(rows);

    SolrRDD solrRDD = new SolrRDD(zkHost, collection);

    JavaRDD<SolrDocument> docs = null;

    long startMs = System.currentTimeMillis();

    docs = solrRDD.queryShards(jsc, solrQuery, splitField, splitsPerShard);
    long count = docs.count();

    long tookMs = System.currentTimeMillis() - startMs;
    System.out.println("took " + tookMs + "ms read "+count+" docs using queryShards with splits");

    startMs = System.currentTimeMillis();
    docs = solrRDD.queryShards(jsc, solrQuery);

    count = docs.count();

    tookMs = System.currentTimeMillis() - startMs;
    System.out.println("took " + tookMs + "ms read "+count+" docs using queryShards");

    jsc.stop();

    return 0;
  }
}
