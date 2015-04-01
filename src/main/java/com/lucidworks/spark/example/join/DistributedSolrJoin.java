package com.lucidworks.spark.example.join;

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
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;


/**
 * How to join Solr data using Spark.
 */
public class DistributedSolrJoin implements SparkApp.RDDProcessor {

  public String getName() {
    return "join-solr";
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

    // parents
    final SolrQuery solrQuery = new SolrQuery("*:*");
    solrQuery.setFields("id");

    solrQuery.setRows(1000);

    List<SolrQuery.SortClause> sorts = new ArrayList<SolrQuery.SortClause>();
    sorts.add(new SolrQuery.SortClause("id", "asc"));
    sorts.add(new SolrQuery.SortClause("_version_", "asc"));
    solrQuery.setSorts(sorts);

    System.out.println(">> starting to query solr");

    long _startMs = System.currentTimeMillis();

    SolrRDD parentSolrRDD = new SolrRDD(zkHost,collection);

    JavaRDD<SolrDocument> results = parentSolrRDD.queryShards(jsc, solrQuery);
    long count = results.count();
    long _diffMs = (System.currentTimeMillis() - _startMs);

    System.out.println(">> count="+count+"; took: "+_diffMs);

    /*
    JavaPairRDD<String,SolrDocument> parents =
            results.mapToPair(new PairFunction<SolrDocument, String, SolrDocument>() {
              public Tuple2<String, SolrDocument> call(SolrDocument doc) throws Exception {
                return new Tuple2<String, SolrDocument>((String) doc.getFirstValue("id"), doc);
              }
            });

    SolrRDD childSolrRDD = new SolrRDD(zkHost, "child");
    JavaPairRDD<String,SolrDocument> children =
            parentSolrRDD.queryShards(jsc, solrQuery).mapToPair(new PairFunction<SolrDocument, String, SolrDocument>() {
              public Tuple2<String, SolrDocument> call(SolrDocument doc) throws Exception {
                return new Tuple2<String, SolrDocument>((String)doc.getFirstValue("joinkey_s"), doc);
              }
            });

    JavaPairRDD<String,Tuple2<SolrDocument,SolrDocument>> joined = parents.join(children).sample(true, 0.01d);

    joined.foreach(new VoidFunction<Tuple2<String, Tuple2<SolrDocument, SolrDocument>>>() {
      public void call(Tuple2<String, Tuple2<SolrDocument, SolrDocument>> t) throws Exception {
        System.out.println(">> "+t._1+": "+t._2._1.get("id")+" / "+t._2._2.get("id"));
      }
    });

    long count = joined.count();

    long _diffMs = (System.currentTimeMillis() - _startMs);

    System.out.println(">> count="+count+"; took: "+_diffMs);
    */

    /*
    joined.flatMap(new FlatMapFunction<Tuple2<String, Tuple2<SolrDocument, SolrDocument>>, Object>() {
      public Iterable<Object> call(Tuple2<String, Tuple2<SolrDocument, SolrDocument>> stringTuple2Tuple2) throws Exception {
        return null;
      }
    });
    */


    jsc.stop();

    return 0;
  }
}
