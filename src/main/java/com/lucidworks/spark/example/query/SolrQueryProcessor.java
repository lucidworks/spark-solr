package com.lucidworks.spark.example.query;

import com.lucidworks.spark.SolrRDD;
import com.lucidworks.spark.SparkApp;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import scala.Tuple2;

import org.apache.spark.sql.api.java.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Example of how to query Solr and process the result set as a Spark RDD
 */
public class SolrQueryProcessor implements SparkApp.RDDProcessor {

  private static final class WordCountSorter implements Comparator<Tuple2<String, Integer>>, Serializable {
    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
      Integer lhs = o1._2;
      Integer rhs = o2._2;
      return (lhs == rhs) ? (o1._1.compareTo(o2._1)) : (lhs > rhs ? 1 : -1);
    }
  }

  public String getName() {
    return "query-solr";
  }

  public Option[] getOptions() {
    return new Option[]{
      OptionBuilder
              .withArgName("QUERY")
              .hasArg()
              .isRequired(false)
              .withDescription("URL encoded Solr query to send to Solr")
              .create("query")
    };
  }

  public int run(SparkConf conf, CommandLine cli) throws Exception {

    String zkHost = cli.getOptionValue("zkHost", "localhost:9983");
    String collection = cli.getOptionValue("collection", "collection1");
    String queryStr = cli.getOptionValue("query", "*:*");

    JavaSparkContext jsc = new JavaSparkContext(conf);

    // TODO: Would be better to accept a JSON representation of a SolrQuery
    final SolrQuery solrQuery = new SolrQuery(queryStr);
    solrQuery.setFields("text_t","type_s");

    List<SolrQuery.SortClause> sorts = new ArrayList<SolrQuery.SortClause>();
    sorts.add(new SolrQuery.SortClause("id", "asc"));
    sorts.add(new SolrQuery.SortClause("created_at_tdt", "asc"));
    solrQuery.setSorts(sorts);

    SolrRDD solrRDD = new SolrRDD(zkHost, collection);

    JavaRDD<SolrDocument> solrJavaRDD = solrRDD.queryShards(jsc, solrQuery);

    JavaRDD<String> words = solrJavaRDD.flatMap(new FlatMapFunction<SolrDocument, String>() {
      public Iterable<String> call(SolrDocument doc) {
        Object tweet_s = doc.get("text_t");
        String str = tweet_s != null ? tweet_s.toString() : "";
        str = str.toLowerCase().replaceAll("[.,!?\n]", " ").trim();
        return Arrays.asList(str.split(" "));
      }
    });

    JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
      public Tuple2<String, Integer> call(String s) {
        return new Tuple2<String, Integer>(s, 1);
      }
    });
    JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
      public Integer call(Integer i1, Integer i2) {
        return i1 + i2;
      }
    });

    for (Tuple2<?,?> tuple : counts.top(20, new WordCountSorter()))
      System.out.println(tuple._1() + ": " + tuple._2());


    // Now use schema information in Solr to build a queryable SchemaRDD
    JavaSQLContext sqlContext = new JavaSQLContext(jsc);
    JavaSchemaRDD solrQuerySchemaRDD =
      solrRDD.applySchema(sqlContext, solrQuery, solrJavaRDD, zkHost, collection);

    // Register the SchemaRDD as a table.
    solrQuerySchemaRDD.registerTempTable("tweets");

    // SQL can be run over RDDs that have been registered as tables.
    JavaSchemaRDD results = sqlContext.sql("SELECT COUNT(type_s) FROM tweets WHERE type_s='echo'");

    // The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by ordinal.
    List<Long> count = results.map(new Function<Row, Long>() {
      public Long call(Row row) {
        return row.getLong(0);
      }
    }).collect();

    System.out.println("# of echos : "+count);

    jsc.stop();

    return 0;
  }
}
