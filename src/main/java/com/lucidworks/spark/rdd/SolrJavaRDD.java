package com.lucidworks.spark.rdd;

import com.lucidworks.spark.util.JavaApiHelper;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;

//TODO: Add all other methods from SolrRDD here
public class SolrJavaRDD extends JavaRDD<SolrDocument> {

  private final SolrRDD solrRDD;

  public SolrJavaRDD(SolrRDD solrRDD) {
    super(solrRDD, JavaApiHelper.getClassTag(SolrDocument.class));
    this.solrRDD = solrRDD;
  }

  protected SolrJavaRDD wrap(SolrRDD rdd) {
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

  @Override
  public SolrRDD rdd() {
    return solrRDD;
  }

  public static SolrJavaRDD get(String zkHost, String collection, SparkContext sc) {
    SolrRDD solrRDD = SolrRDD$.MODULE$.apply(zkHost, collection, sc);
    return new SolrJavaRDD(solrRDD);
  }

}
