package com.lucidworks.spark.query;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.io.graph.GatherNodesStream;
import org.apache.solr.client.solrj.io.graph.ShortestPathStream;
import org.apache.solr.client.solrj.io.ops.ConcatOperation;
import org.apache.solr.client.solrj.io.ops.DistinctOperation;
import org.apache.solr.client.solrj.io.ops.GroupOperation;
import org.apache.solr.client.solrj.io.ops.ReplaceOperation;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.metrics.*;
import org.apache.solr.common.params.SolrParams;

public class CloudStreamIterator extends TupleStreamIterator {

  private static final Logger log = Logger.getLogger(CloudStreamIterator.class);

  protected String zkHost;
  protected String collection;

  public CloudStreamIterator(String zkHost, String collection, SolrParams solrParams) {
    super(solrParams);
    this.zkHost = zkHost;
    this.collection = collection;
  }

  protected TupleStream openStream() {
    StreamFactory streamFactory = new StreamFactory();

    log.info("Opening CloudSolrStream to "+collection+" using "+zkHost);

    streamFactory = streamFactory.withCollectionZkHost(collection, zkHost).withDefaultZkHost(zkHost)
            .withFunctionName("search", CloudSolrStream.class)
            .withFunctionName("facet", FacetStream.class)
            .withFunctionName("update", UpdateStream.class)
            .withFunctionName("jdbc", JDBCStream.class)
            .withFunctionName("topic", TopicStream.class)

                    // decorator streams
            .withFunctionName("merge", MergeStream.class)
            .withFunctionName("unique", UniqueStream.class)
            .withFunctionName("top", RankStream.class)
            .withFunctionName("group", GroupOperation.class)
            .withFunctionName("reduce", ReducerStream.class)
            .withFunctionName("parallel", ParallelStream.class)
            .withFunctionName("rollup", RollupStream.class)
            .withFunctionName("stats", StatsStream.class)
            .withFunctionName("innerJoin", InnerJoinStream.class)
            .withFunctionName("leftOuterJoin", LeftOuterJoinStream.class)
            .withFunctionName("hashJoin", HashJoinStream.class)
            .withFunctionName("outerHashJoin", OuterHashJoinStream.class)
            .withFunctionName("intersect", IntersectStream.class)
            .withFunctionName("complement", ComplementStream.class)
            .withFunctionName("daemon", DaemonStream.class)
            .withFunctionName("sort", SortStream.class)
            .withFunctionName("select", SelectStream.class)
            .withFunctionName("shortestPath", ShortestPathStream.class)
            .withFunctionName("gatherNodes", GatherNodesStream.class)

                    // metrics
            .withFunctionName("min", MinMetric.class)
            .withFunctionName("max", MaxMetric.class)
            .withFunctionName("avg", MeanMetric.class)
            .withFunctionName("sum", SumMetric.class)
            .withFunctionName("count", CountMetric.class)

                    // tuple manipulation operations
            .withFunctionName("replace", ReplaceOperation.class)
            .withFunctionName("concat", ConcatOperation.class)

                    // stream reduction operations
            .withFunctionName("group", GroupOperation.class)
            .withFunctionName("distinct", DistinctOperation.class);

    String expr = solrParams.get("expr").replaceAll("\\s+", " ");
    log.info("Executing streaming expression "+expr+" against collection "+collection);
    try {
      TupleStream tupleStream = streamFactory.constructStream(expr);
      tupleStream.open();
      return tupleStream;
    } catch (Exception e) {
      log.error("Failed to execute streaming expression "+expr+" due to: "+e, e);
      if (e instanceof RuntimeException) {
        throw (RuntimeException)e;
      } else {
        throw new RuntimeException(e);
      }
    }
  }
}
