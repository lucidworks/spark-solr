package com.lucidworks.spark.example.query;

import com.lucidworks.spark.SparkApp;
import com.lucidworks.spark.rdd.SolrJavaRDD;
import com.lucidworks.spark.rdd.SolrRDD;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;

import java.util.ArrayList;
import java.util.List;

/**
 * Generate a {@code JavaRDD<org.apache.spark.mllib.linalg.Vector>} from term vector information in the Solr index.
 */
public class ReadTermVectors implements SparkApp.RDDProcessor {

  public String getName() {
    return "term-vectors";
  }

  public Option[] getOptions() {
    return new Option[]{
      OptionBuilder
        .withArgName("QUERY")
        .hasArg()
        .isRequired(false)
        .withDescription("URL encoded Solr query to send to Solr; default is *:*")
        .create("query"),
      OptionBuilder
        .withArgName("FIELD")
        .hasArg()
        .isRequired(true)
        .withDescription("Field to generate term vectors from")
        .create("field"),
      OptionBuilder
        .withArgName("NUM")
        .hasArg()
        .isRequired(false)
        .withDescription("Number of features; defaults to 500")
        .create("numFeatures"),
      OptionBuilder
        .withArgName("NUM")
        .hasArg()
        .isRequired(false)
        .withDescription("Number of iterations for K-Means clustering; defaults to 20")
        .create("numIterations"),
      OptionBuilder
        .withArgName("NUM")
        .hasArg()
        .isRequired(false)
        .withDescription("Number of clusters (k) for K-Means clustering; defaults to 5")
        .create("numClusters")

    };
  }

  public int run(SparkConf conf, CommandLine cli) throws Exception {

    String zkHost = cli.getOptionValue("zkHost", "localhost:9983");
    String collection = cli.getOptionValue("collection", "collection1");
    String queryStr = cli.getOptionValue("query", "*:*");
    String field = cli.getOptionValue("field");
    int numFeatures = Integer.parseInt(cli.getOptionValue("numFeatures", "500"));
    int numClusters = Integer.parseInt(cli.getOptionValue("numClusters", "5"));
    int numIterations = Integer.parseInt(cli.getOptionValue("numIterations", "20"));

    JavaSparkContext jsc = new JavaSparkContext(conf);

    final SolrQuery solrQuery = new SolrQuery(queryStr);
    solrQuery.setFields("id");

    // sorts are needed for deep-paging
    List<SolrQuery.SortClause> sorts = new ArrayList<SolrQuery.SortClause>();
    sorts.add(new SolrQuery.SortClause("id", "asc"));
    sorts.add(new SolrQuery.SortClause("created_at_tdt", "asc"));
    solrQuery.setSorts(sorts);

    SolrJavaRDD solrRDD = SolrJavaRDD.get(zkHost, collection, jsc.sc());

    //TODO: Commented out until we implement term vectors in Base RDD
//    // query Solr for term vectors
//    JavaRDD<Vector> termVectorsFromSolr =
//      solrRDD.queryTermVectors(solrQuery, field, numFeatures);
//    termVectorsFromSolr.cache();
//
//    // Cluster the data using KMeans
//    KMeansModel clusters = KMeans.train(termVectorsFromSolr.rdd(), numClusters, numIterations);
//
//    // TODO: do something interesting with the clusters
//
//    // Evaluate clustering by computing Within Set Sum of Squared Errors
//    double WSSSE = clusters.computeCost(termVectorsFromSolr.rdd());
//    System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

    jsc.stop();

    return 0;
  }
}
