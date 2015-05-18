package com.lucidworks.spark.query;

import com.lucidworks.spark.SolrRDD;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * An iterator over a stream of query results from Solr.
 */
public class StreamingResultsIterator extends StreamingResponseCallback implements Iterator<SolrDocument>, Iterable<SolrDocument> {

  protected SolrClient solrServer;
  protected SolrQuery solrQuery;
  protected int currentPageSize = 0;
  protected int iterPos = 0;
  protected long totalDocs = -1;
  protected long numDocs = 0;
  protected String cursorMark = null;
  protected boolean closeAfterIterating = false;
  protected LinkedBlockingDeque<SolrDocument> queue;

  private CountDownLatch docListInfoLatch = new CountDownLatch(1);

  public StreamingResultsIterator(SolrClient solrServer, SolrQuery solrQuery) {
    this(solrServer, solrQuery, null);
  }

  public StreamingResultsIterator(SolrClient solrServer, SolrQuery solrQuery, String cursorMark) {
    this.queue = new LinkedBlockingDeque<SolrDocument>();
    this.solrServer = solrServer;
    this.closeAfterIterating = !(solrServer instanceof CloudSolrClient);
    this.solrQuery = solrQuery;
    this.cursorMark = cursorMark;
    if (solrQuery.getRows() == null)
      solrQuery.setRows(PagedResultsIterator.DEFAULT_PAGE_SIZE); // default page size
  }

  public boolean hasNext() {
    if (totalDocs == 0 || totalDocs == numDocs)
      return false; // done iterating!

    if (totalDocs == -1 || iterPos == currentPageSize) {
      // call out to Solr to get next page
      try {
        fetchNextPage();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    boolean hasNext = (totalDocs > 0 && iterPos < currentPageSize);
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

  protected void fetchNextPage() throws SolrServerException, InterruptedException {
    int start = (cursorMark != null) ? 0 : getStartForNextPage();
    currentPageSize = solrQuery.getRows();
    QueryResponse resp = SolrRDD.querySolr(solrServer, solrQuery, start, cursorMark, this);
    if (cursorMark != null) cursorMark = resp.getNextCursorMark();
    iterPos = 0;
    docListInfoLatch.await();
  }

  public SolrDocument next() {
    if (iterPos >= currentPageSize)
      throw new NoSuchElementException("No more docs available! Please call hasNext before calling next!");

    SolrDocument next = null;
    try {
      next = queue.poll(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new RuntimeException(e);
    }
    ++numDocs;
    ++iterPos;

    return next;
  }

  public void remove() {
    throw new UnsupportedOperationException("remove is not supported");
  }

  public Iterator<SolrDocument> iterator() {
    return this;
  }

  public void streamSolrDocument(SolrDocument doc) {
    queue.offer(doc);
  }

  public void streamDocListInfo(long numFound, long start, Float maxScore) {
    docListInfoLatch.countDown();
    totalDocs = numFound;
    if (currentPageSize > totalDocs)
      currentPageSize = (int)totalDocs;
  }
}