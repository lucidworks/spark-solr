package com.lucidworks.spark.rdd;

import com.lucidworks.spark.util.JavaApiHelper;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;

public class SolrJavaRDD extends JavaRDD<SolrDocument> {

  private final SelectSolrRDD solrRDD;

  public SolrJavaRDD(SelectSolrRDD solrRDD) {
    super(solrRDD, solrRDD.elementClassTag());
    this.solrRDD = solrRDD;
  }

  protected SolrJavaRDD wrap(SelectSolrRDD rdd) {
    return new SolrJavaRDD(rdd);
  }

  public JavaRDD<SolrDocument> query(String query) {
    return wrap(rdd().query(query));
  }

  public JavaRDD<SolrDocument> queryShards(String query) {
    return wrap(rdd().query(query));
  }

  public JavaRDD<SolrDocument> queryShards(SolrQuery solrQuery) {
    return wrap(rdd().query(solrQuery));
  }

  public JavaRDD<SolrDocument> queryShards(SolrQuery solrQuery, String splitFieldName, int splitsPerShard) {
    return wrap(rdd().query(solrQuery).splitField(splitFieldName).splitsPerShard(splitsPerShard));
  }

  public JavaRDD<SolrDocument> queryNoSplits(String query) {
    return wrap(rdd().query(query).splitsPerShard(1));
  }

  @Override
  public SelectSolrRDD rdd() {
    return solrRDD;
  }

  public static SolrJavaRDD get(String zkHost, String collection, SparkContext sc) {
    SelectSolrRDD solrRDD = SelectSolrRDD$.MODULE$.apply(zkHost, collection, sc);
    return new SolrJavaRDD(solrRDD);
  }

}
