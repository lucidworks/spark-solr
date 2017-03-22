package com.lucidworks.spark.rdd;

import com.lucidworks.spark.util.JavaApiHelper;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;

import java.util.Map;

public class SolrStreamJavaRDD extends JavaRDD<Map<?,?>> {

  private final StreamingSolrRDD solrRDD;

  public SolrStreamJavaRDD(StreamingSolrRDD solrRDD) {
    super(solrRDD, solrRDD.elementClassTag());
    this.solrRDD = solrRDD;
  }

  protected SolrStreamJavaRDD wrap(StreamingSolrRDD rdd) {
    return new SolrStreamJavaRDD(rdd);
  }

  public JavaRDD<Map<?,?>> query(String query) {
    return wrap(rdd().query(query));
  }

  public JavaRDD<Map<?,?>> queryShards(String query) {
    return wrap(rdd().query(query));
  }

  public JavaRDD<Map<?,?>> queryShards(SolrQuery solrQuery) {
    return wrap(rdd().query(solrQuery));
  }

  public JavaRDD<Map<?,?>> queryShards(SolrQuery solrQuery, String splitFieldName, int splitsPerShard) {
    return wrap(rdd().query(solrQuery).splitField(splitFieldName).splitsPerShard(splitsPerShard));
  }

  public JavaRDD<Map<?,?>> queryNoSplits(String query) {
    return wrap(rdd().query(query).splitsPerShard(1));
  }

  @Override
  public StreamingSolrRDD rdd() {
    return solrRDD;
  }

  public static SolrStreamJavaRDD get(String zkHost, String collection, SparkContext sc) {
    StreamingSolrRDD solrRDD = StreamingSolrRDD$.MODULE$.apply(zkHost, collection, sc);
    return new SolrStreamJavaRDD(solrRDD);
  }

}
