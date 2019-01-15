package com.lucidworks.spark.query;

import com.lucidworks.spark.util.SolrQuerySupport;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * An iterator over a stream of query results from Solr.
 */
public class StreamingResultsIterator extends ResultsIterator<SolrDocument> {

  private static Logger log = LoggerFactory.getLogger(StreamingResultsIterator.class);

  protected SolrClient solrServer;
  protected SolrQuery solrQuery;
  protected int currentPageSize = 0;
  protected int iterPos = 0;
  protected long totalDocs = -1;
  protected long numDocs = 0;
  protected boolean usingCursors = false;
  protected String nextCursorMark = null;
  protected String cursorMarkOfCurrentPage = null;
  protected LinkedBlockingDeque<SolrDocument> queue;
  protected Integer maxSampleDocs = null;
  protected String solrId = null;

  private ResponseCallback responseCallback = new ResponseCallback();
  private CountDownLatch docListInfoLatch = new CountDownLatch(1);

  public StreamingResultsIterator(SolrClient solrServer, SolrQuery solrQuery) {
    this(solrServer, solrQuery, null);
  }

  public StreamingResultsIterator(SolrClient solrServer, SolrQuery solrQuery, String cursorMark) {
    this.queue = new LinkedBlockingDeque<SolrDocument>();
    this.solrServer = solrServer;

    // get some identifier for this solr server
    if (solrServer instanceof HttpSolrClient) {
      HttpSolrClient httpSolrClient = (HttpSolrClient)solrServer;
      solrId = httpSolrClient.getBaseURL();
    } else {
      solrId = solrServer.toString();
    }

    this.solrQuery = solrQuery;
    this.usingCursors = (cursorMark != null);
    this.nextCursorMark = cursorMark;
    this.cursorMarkOfCurrentPage = cursorMark;
    if (solrQuery.getRows() == null)
      solrQuery.setRows(PagedResultsIterator.DEFAULT_PAGE_SIZE); // default page size
  }

  public void setMaxSampleDocs(Integer maxDocs) {
    this.maxSampleDocs = maxDocs;
  }

  public boolean hasNext() {
    if (totalDocs == 0 || (totalDocs != -1 && numDocs >= totalDocs) || (maxSampleDocs != null && maxSampleDocs >= 0 && numDocs >= maxSampleDocs))
      return false; // done iterating!

    boolean hasNext = false;
    if (totalDocs == -1 || iterPos == currentPageSize) {
      // call out to Solr to get next page
      try {
        hasNext = fetchNextPage();
      } catch (Exception e) {
        log.error("Fetch next page ({}) from Solr {} using query [{}], cursorMarks? {}, " +
            "cursorMarkOfCurrentPage={} failed due to: {}", iterPos, solrId, solrQuery, usingCursors, cursorMarkOfCurrentPage, e);
        if (e instanceof RuntimeException) {
          throw (RuntimeException)e;
        } else {
          throw new RuntimeException(e);
        }
      }
    } else {
      hasNext = (totalDocs > 0 && iterPos < currentPageSize);
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

    Option<QueryResponse> resp = SolrQuerySupport.querySolr(solrServer, solrQuery, start, cursorMarkOfCurrentPage, responseCallback);

    if (resp.isDefined()) {
      if (usingCursors) {
        nextCursorMark = resp.get().getNextCursorMark();
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
    } else {
      throw new SolrServerException("No response from "+solrId+" found for query '" + solrQuery + "'");
    }

  }

  public SolrDocument next() {
    if (iterPos >= currentPageSize)
      throw new NoSuchElementException("No more docs available from "+solrId+"! Please call hasNext before calling next!");

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
          " so far from "+solrId+". Most likely this means your query's sort criteria is not generating stable results for computing deep-paging cursors, has the index changed? " +
          "If so, try using a filter criteria the bounds the results to non-changing data.");
    }

    ++numDocs;
    ++iterPos;
    increment();

    return next;
  }

  public void remove() {
    throw new UnsupportedOperationException("remove is not supported");
  }

  public Iterator<SolrDocument> iterator() {
    return this;
  }

  @Override
  public long getNumDocs() {
    return numDocs;
  }

  private class ResponseCallback extends StreamingResponseCallback {
    public void streamSolrDocument(SolrDocument doc) {
      if (doc != null) {
        queue.offer(doc);
      } else {
        log.warn("Received null SolrDocument from {} callback while processing cursorMark={}," +
            " read {} of {} so far.", solrId, cursorMarkOfCurrentPage, numDocs, totalDocs);
      }
    }

    public void streamDocListInfo(long numFound, long start, Float maxScore) {
      docListInfoLatch.countDown();
      totalDocs = numFound;

      // see if they enabled sampling
      if (maxSampleDocs == null) {
        if (numFound > 0) {
          String samplePctParam = solrQuery.get("sample_pct");
          if (samplePctParam != null) {
            float pct = Float.parseFloat(samplePctParam);
            maxSampleDocs = Math.round((float)numFound * pct);
            log.info("Sampling {} ({} of {}) from {}", maxSampleDocs, pct, numFound, solrId);
          } else {
            maxSampleDocs = -1; // no sampling
          }
        } else {
          maxSampleDocs = -1;
        }
      }

      if (currentPageSize > totalDocs)
        currentPageSize = (int)totalDocs;
    }
  }
}
