package com.lucidworks.spark.rdd;

import com.lucidworks.spark.util.JavaApiHelper;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;

public class SolrStreamJavaRDD extends JavaRDD<Tuple> {

  private final StreamingSolrRDD solrRDD;

  public SolrStreamJavaRDD(StreamingSolrRDD solrRDD) {
    super(solrRDD, JavaApiHelper.getClassTag(Tuple.class));
    this.solrRDD = solrRDD;
  }

  protected SolrStreamJavaRDD wrap(StreamingSolrRDD rdd) {
    return new SolrStreamJavaRDD(rdd);
  }

  public JavaRDD<Tuple> query(String query) {
    return wrap((StreamingSolrRDD) rdd().query(query));
  }

  public JavaRDD<Tuple> queryShards(String query) {
    return wrap((StreamingSolrRDD) rdd().query(query));
  }

  public JavaRDD<Tuple> queryShards(SolrQuery solrQuery) {
    return wrap((StreamingSolrRDD) rdd().query(solrQuery));
  }

  public JavaRDD<Tuple> queryShards(SolrQuery solrQuery, String splitFieldName, int splitsPerShard) {
    return wrap((StreamingSolrRDD) ((StreamingSolrRDD)((StreamingSolrRDD) rdd().query(solrQuery)).splitField(splitFieldName)).splitsPerShard(splitsPerShard));
  }

  public JavaRDD<Tuple> queryNoSplits(String query) {
    return wrap((StreamingSolrRDD) ((StreamingSolrRDD) rdd().query(query)).splitsPerShard(1));
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
