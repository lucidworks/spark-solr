package com.lw.spark.rdd;

import com.lw.spark.query.ShardSplit;
import com.lw.spark.util.SolrQuerySupport;
import com.lw.spark.query.StreamingResultsIterator;
import com.lw.spark.query.TermVectorIterator;
import com.lw.spark.util.SolrSupport;
import com.lw.spark.SolrConf;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.mllib.linalg.Vector;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class SolrRDD implements Serializable{

  private static Logger log = Logger.getLogger(SolrRDD.class);

  private final String collection;
  private final String zkHost;
  private final JavaSparkContext jsc;
  private final SparkConf sparkConf;
  private final SolrConf solrConf;
  private final CloudSolrClient solrClient;
  private final String uniqueKey;

  public SolrRDD(String zkHost, String collection, SparkContext sparkContext) throws SparkException{
    this.collection = collection;
    this.zkHost = zkHost;
    this.jsc = new JavaSparkContext(sparkContext);
    this.sparkConf = sparkContext.getConf();
    this.solrConf = new SolrConf(this.sparkConf);
    this.solrClient = SolrSupport.getSolrClient(zkHost);
    this.uniqueKey = SolrQuerySupport.getUniqueKey(zkHost, collection);
  }

  /**
   *  Get a document by ID using real-time get
   */
  public JavaRDD<SolrDocument> get(final String docId) throws SparkException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("collection", collection);
    params.set("qt", "/get");
    params.set(uniqueKey, docId);

    QueryResponse resp = null;
    try {
      resp = solrClient.query(params);
    } catch (Exception exc) {
      throw new SparkException("Exception while querying Solr", exc);
    }
    SolrDocument doc = (SolrDocument) resp.getResponse().get("doc");
    List<SolrDocument> list = (doc != null) ? Collections.singletonList(doc): Collections.emptyList();
    return jsc.parallelize(list, 1);
  }

  /**
   * Get solr documents by querying shards in parallel
   */
  public JavaRDD<SolrDocument> queryShards(String query) {
    return queryShards(SolrQuerySupport.toQuery(query));
  }

  public JavaRDD<SolrDocument> queryShards(final SolrQuery origQuery) {
    List<String> shards = SolrSupport.buildShardList(solrClient, collection);
    final SolrQuery query = origQuery.getCopy();

    query.set("collection", collection);
    SolrQuerySupport.setQueryDefaultsForShards(query, uniqueKey);

    // Parallelize the requests to shards
    JavaRDD<SolrDocument> docs = jsc.parallelize(shards, shards.size()).flatMap(
      new FlatMapFunction<String, SolrDocument>() {
        @Override
        public Iterable<SolrDocument> call(String shardUrl) throws Exception {
          return new StreamingResultsIterator(SolrSupport.getHttpSolrClient(shardUrl), query, "*");
        }
      }
    );
    return docs;
  }

  public JavaRDD<Vector> queryTV(final SolrQuery origQuery, final String field, final int numFeatures) throws SparkException {
    List<String> shards = SolrSupport.buildShardList(solrClient, collection);
    final SolrQuery query = origQuery.getCopy();

    query.set("collection", collection);
    SolrQuerySupport.setQueryDefaultsForTV(query, field, uniqueKey);

    // Parallelize the requests to shards
    JavaRDD<Vector> docs = jsc.parallelize(shards, shards.size()).flatMap(
      new FlatMapFunction<String, Vector>() {
        @Override
        public Iterable<Vector> call(String shardUrl) throws Exception {
          return new TermVectorIterator(SolrSupport.getHttpSolrClient(shardUrl), query, "*", field, numFeatures);
        }
      }
    );
    return docs;
  }

  public JavaRDD<SolrDocument> queryShards(final SolrQuery origQuery, final String splitFieldName, final int splitsPerShard) {
    // if only doing 1 split per shard, then queryShards does that already
    if (splitFieldName == null || splitsPerShard <= 1)
      return queryShards(origQuery);

    long timerDiffMs = 0L;
    long timerStartMs = 0L;

    // first get a list of replicas to query for this collection
    List<String> shards = SolrSupport.buildShardList(solrClient, collection);

    timerStartMs = System.currentTimeMillis();

    final SolrQuery query = origQuery.getCopy();
    query.set("collection", collection);
    SolrQuerySupport.setQueryDefaultsForShards(query, uniqueKey);

    JavaRDD<ShardSplit> splitsRDD = SolrQuerySupport.splitShard(jsc, query, shards, splitFieldName, splitsPerShard, collection);
    List<ShardSplit> splits = splitsRDD.collect();
    timerDiffMs = (System.currentTimeMillis() - timerStartMs);
    log.info("Collected " + splits.size() + " splits, took " + timerDiffMs + "ms");

    // parallelize the requests to the shards
    JavaRDD<SolrDocument> docs = jsc.parallelize(splits, splits.size()).flatMap(
      new FlatMapFunction<ShardSplit, SolrDocument>() {
        public Iterable<SolrDocument> call(ShardSplit split) throws Exception {
          return new StreamingResultsIterator(SolrSupport.getHttpSolrClient(split.getShardUrl()), split.getSplitQuery(), "*");
        }
      }
    );
    return docs;
  }

  public String getUniqueKey() {
    return this.uniqueKey;
  }

}
