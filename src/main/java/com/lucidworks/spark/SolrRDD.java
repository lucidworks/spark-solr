package com.lucidworks.spark;

import java.io.IOException;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class SolrRDD implements Serializable {

  public static Logger log = Logger.getLogger(SolrRDD.class);

  private static final int DEFAULT_PAGE_SIZE = 50;

  /**
   * Iterates over the entire results set of a query (all hits).
   */
  private class PagedResultsIterator implements Iterator<SolrDocument>, Iterable<SolrDocument> {

    private SolrServer solrServer;
    private SolrQuery solrQuery;
    private SolrDocumentList currentPage;
    private int currentPageSize = 0;
    private int iterPos = 0;
    private long totalDocs = 0;
    private long numDocs = 0;
    private String cursorMark = null;
    private boolean closeAfterIterating = false;

    private PagedResultsIterator(SolrServer solrServer, SolrQuery solrQuery) {
      this(solrServer, solrQuery, null);
    }

    private PagedResultsIterator(SolrServer solrServer, SolrQuery solrQuery, String cursorMark) {
      this.solrServer = solrServer;
      this.closeAfterIterating = !(solrServer instanceof CloudSolrServer);
      this.solrQuery = solrQuery;
      this.cursorMark = cursorMark;
      if (solrQuery.getRows() == null)
        solrQuery.setRows(DEFAULT_PAGE_SIZE); // default page size
    }

    public boolean hasNext() {
      if (currentPage == null || iterPos == currentPageSize) {
        try {
          currentPage = fetchNextPage();
          currentPageSize = currentPage.size();
          iterPos = 0;
        } catch (SolrServerException sse) {
          throw new RuntimeException(sse);
        }
      }
      boolean hasNext = (iterPos < currentPageSize);
      if (!hasNext && closeAfterIterating) {
        try {
          solrServer.shutdown();
        } catch (Exception exc) {}
      }
      return hasNext;
    }

    protected int getStartForNextPage() {
      Integer currentStart = solrQuery.getStart();
      return (currentStart != null) ? currentStart + solrQuery.getRows() : 0;
    }

    protected SolrDocumentList fetchNextPage() throws SolrServerException {
      int start = (cursorMark != null) ? 0 : getStartForNextPage();
      QueryResponse resp = querySolr(solrServer, solrQuery, start, cursorMark);
      if (cursorMark != null)
        cursorMark = resp.getNextCursorMark();

      iterPos = 0;
      SolrDocumentList docs = resp.getResults();
      totalDocs = docs.getNumFound();
      return docs;
    }

    public SolrDocument next() {
      if (currentPage == null || iterPos >= currentPageSize)
        throw new NoSuchElementException("No more docs available!");

      ++numDocs;

      return currentPage.get(iterPos++);
    }

    public void remove() {
      throw new UnsupportedOperationException("remove is not supported");
    }

    public Iterator<SolrDocument> iterator() {
      return this;
    }
  }

  // can't serialize CloudSolrServers so we cache them in a static context to reuse by the zkHost
  private static final Map<String,CloudSolrServer> cachedServers = new HashMap<String,CloudSolrServer>();

  protected static CloudSolrServer getSolrServer(String zkHost) {
    CloudSolrServer cloudSolrServer = null;
    synchronized (cachedServers) {
      cloudSolrServer = cachedServers.get(zkHost);
      if (cloudSolrServer == null) {
        cloudSolrServer = new CloudSolrServer(zkHost);
        cloudSolrServer.connect();
        cachedServers.put(zkHost, cloudSolrServer);
      }
    }
    return cloudSolrServer;
  }

  protected String zkHost;
  protected String collection;

  public SolrRDD(String zkHost, String collection) {
    this.zkHost = zkHost;
    this.collection = collection;
  }

  /**
   * Get a document by ID using real-time get
   */
  public JavaRDD<SolrDocument> get(JavaSparkContext jsc, final String docId) throws SolrServerException {
    CloudSolrServer cloudSolrServer = getSolrServer(zkHost);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("collection", collection);
    params.set("qt", "/get");
    params.set("id", docId);
    QueryResponse resp = cloudSolrServer.query(params);
    SolrDocument doc = (SolrDocument)resp.getResponse().get("doc");
    List<SolrDocument> list = (doc != null) ? Arrays.asList(doc) : new ArrayList<SolrDocument>();
    return jsc.parallelize(list, 1);
  }

  public JavaRDD<SolrDocument> query(JavaSparkContext jsc, final SolrQuery query, boolean useDeepPagingCursor) throws SolrServerException {
    if (useDeepPagingCursor)
      return queryDeep(jsc, query);

    query.set("collection", collection);
    CloudSolrServer cloudSolrServer = getSolrServer(zkHost);
    List<SolrDocument> results = new ArrayList<SolrDocument>();
    Iterator<SolrDocument> resultsIter = new PagedResultsIterator(cloudSolrServer, query);
    while (resultsIter.hasNext()) results.add(resultsIter.next());
    return jsc.parallelize(results,1);
  }

  public JavaRDD<SolrDocument> queryShards(JavaSparkContext jsc, final SolrQuery query) throws SolrServerException {
    // first get a list of replicas to query for this collection
    List<String> shards = buildShardList(getSolrServer(zkHost));

    // we'll be directing queries to each shard, so we don't want distributed
    query.set("distrib", false);
    query.set("collection", collection);
    query.setStart(0);
    if (query.getRows() == null)
      query.setRows(DEFAULT_PAGE_SIZE); // default page size

    // parallelize the requests to the shards
    JavaRDD<SolrDocument> docs = jsc.parallelize(shards).flatMap(
      new FlatMapFunction<String, SolrDocument>() {
        public Iterable<SolrDocument> call(String shardUrl) throws Exception {
          return new PagedResultsIterator(new HttpSolrServer(shardUrl), query, "*");
        }
      }
    );
    return docs;
  }

  protected List<String> buildShardList(CloudSolrServer cloudSolrServer) {
    ZkStateReader zkStateReader = cloudSolrServer.getZkStateReader();

    ClusterState clusterState = zkStateReader.getClusterState();
    Set<String> liveNodes = clusterState.getLiveNodes();
    Collection<Slice> slices = clusterState.getSlices(collection);
    if (slices == null)
      throw new IllegalArgumentException("Collection "+collection+" not found!");

    Random random = new Random();
    List<String> shards = new ArrayList<String>();
    for (Slice slice : slices) {
      List<String> replicas = new ArrayList<String>();
      for (Replica r : slice.getReplicas()) {
        ZkCoreNodeProps replicaCoreProps = new ZkCoreNodeProps(r);
        if (liveNodes.contains(replicaCoreProps.getNodeName()))
          replicas.add(replicaCoreProps.getCoreUrl());
      }
      int numReplicas = replicas.size();
      if (numReplicas == 0)
        throw new IllegalStateException("Shard "+slice.getName()+" does not have any active replicas!");

      String replicaUrl = (numReplicas == 1) ? replicas.get(0) : replicas.get(random.nextInt(replicas.size()));
      shards.add(replicaUrl);
    }

    return shards;
  }

  public JavaRDD<SolrDocument> queryDeep(JavaSparkContext jsc, final SolrQuery query) throws SolrServerException {
    List<String> cursors = new ArrayList<String>();

    // stash this for later use when we're actually querying for data
    String fields = query.getFields();

    query.set("collection", collection);
    query.setStart(0);
    query.setFields("id");

    if (query.getRows() == null)
      query.setRows(DEFAULT_PAGE_SIZE); // default page size

    CloudSolrServer cloudSolrServer = getSolrServer(zkHost);
    String nextCursorMark = "*";
    while (true) {
      cursors.add(nextCursorMark);
      query.set("cursorMark", nextCursorMark);
      QueryResponse resp = cloudSolrServer.query(query);
      nextCursorMark = resp.getNextCursorMark();
      if (nextCursorMark == null || resp.getResults().isEmpty())
        break;
    }

    JavaRDD<String> cursorJavaRDD = jsc.parallelize(cursors);

    query.setFields(fields);

    // now we need to execute all the cursors in parallel
    JavaRDD<SolrDocument> docs = cursorJavaRDD.flatMap(
      new FlatMapFunction<String, SolrDocument>() {
        public Iterable<SolrDocument> call(String cursorMark) throws Exception {
          return querySolr(getSolrServer(zkHost), query, 0, cursorMark).getResults();
        }
      }
    );
    return docs;
  }

  protected static QueryResponse querySolr(SolrServer solrServer, SolrQuery solrQuery, int startIndex, String cursorMark) throws SolrServerException {
    QueryResponse resp = null;
    try {
      if (cursorMark != null) {
        solrQuery.setStart(0);
        solrQuery.set("cursorMark", cursorMark);
      } else {
        solrQuery.setStart(startIndex);
      }
      resp = solrServer.query(solrQuery);
    } catch (SolrServerException exc) {

      // re-try once in the event of a communications error with the server
      Throwable rootCause = SolrException.getRootCause(exc);
      boolean wasCommError =
              (rootCause instanceof ConnectException ||
               rootCause instanceof IOException ||
               rootCause instanceof SocketException);
      if (wasCommError) {
        try {
          Thread.sleep(2000L);
        } catch (InterruptedException ie) {
          Thread.interrupted();
        }

        resp = solrServer.query(solrQuery);
      } else {
        throw exc;
      }
    }

    return resp;
  }
}
