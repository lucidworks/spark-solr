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
  protected boolean usingCursors = false;
  protected String nextCursorMark = null;
  protected String cursorMarkOfCurrentPage = null;
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
    this.usingCursors = (cursorMark != null);
    this.nextCursorMark = cursorMark;
    this.cursorMarkOfCurrentPage = cursorMark;
    if (solrQuery.getRows() == null)
      solrQuery.setRows(PagedResultsIterator.DEFAULT_PAGE_SIZE); // default page size
  }

  public boolean hasNext() {
    if (totalDocs == 0 || (totalDocs != -1 && numDocs >= totalDocs))
      return false; // done iterating!

    boolean hasNext = false;
    if (totalDocs == -1 || iterPos == currentPageSize) {
      // call out to Solr to get next page
      try {
        hasNext = fetchNextPage();
      } catch (Exception e) {
        if (e instanceof RuntimeException) {
          throw (RuntimeException)e;
        } else {
          throw new RuntimeException(e);
        }
      }
    } else {
      hasNext = (totalDocs > 0 && iterPos < currentPageSize);
    }

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

  protected boolean fetchNextPage() throws SolrServerException, InterruptedException {
    int start = usingCursors ? 0 : getStartForNextPage();
    currentPageSize = solrQuery.getRows();
    this.cursorMarkOfCurrentPage = nextCursorMark;
    QueryResponse resp = SolrRDD.querySolr(solrServer, solrQuery, start, cursorMarkOfCurrentPage, this);

    if (usingCursors) {
      nextCursorMark = resp.getNextCursorMark();
    }

    iterPos = 0;
    if (usingCursors) {
      if (nextCursorMark != null) {
        docListInfoLatch.await(); // wait until the callback receives notification from Solr in streamDocListInfo
        return totalDocs > 0;
      } else {
        return false;
      }
    } else {
      docListInfoLatch.await();
      return totalDocs > 0;
    }
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

    if (next == null) {
      throw new RuntimeException("No SolrDocument in queue (waited 60 seconds) while processing cursorMark="+
              cursorMarkOfCurrentPage+", read "+numDocs+" of "+totalDocs+
          " so far. Most likely this means your query's sort criteria is not generating stable results for computing deep-paging cursors, has the index changed? " +
          "If so, try using a filter criteria the bounds the results to non-changing data.");
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
    if (doc != null) {
      queue.offer(doc);
    } else {
      SolrRDD.log.warn("Received null SolrDocument from streamSolrDocument callback while processing cursorMark="+
              cursorMarkOfCurrentPage+", read "+numDocs+" of "+totalDocs+" so far.");
    }
  }

  public void streamDocListInfo(long numFound, long start, Float maxScore) {
    docListInfoLatch.countDown();
    totalDocs = numFound;
    if (currentPageSize > totalDocs)
      currentPageSize = (int)totalDocs;
  }
}