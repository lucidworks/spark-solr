package com.lucidworks.spark.query;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * An iterator over a stream of tuples from Solr.
 * <p>
 * This iterator is not thread safe. It is intended to be used within the
 * context of a single thread.
 */
public abstract class TupleStreamIterator extends ResultsIterator<Map> {

  private static final Logger log = LoggerFactory.getLogger(TupleStreamIterator.class);

  protected TupleStream stream;
  protected long numDocs = 0;
  protected SolrParams solrParams;
  private Tuple currentTuple = null;
  private long openedAt;
  private boolean isClosed = false;

  public TupleStreamIterator(SolrParams solrParams) {
    this.solrParams = solrParams;
  }

  public synchronized boolean hasNext() {
    if (isClosed) {
      return false;
    }

    if (stream == null) {
      stream = openStream();
      openedAt = System.currentTimeMillis();
    }

    try {
      if (currentTuple == null) {
        currentTuple = fetchNextTuple();
      }
    } catch (IOException e) {
      log.error("Failed to fetch next Tuple for query: " + solrParams.toQueryString(), e);
      throw new RuntimeException(e);
    }

    if (currentTuple == null) {
      try {
        stream.close();
      } catch (IOException e) {
        log.error("Failed to close the SolrStream.", e);
        throw new RuntimeException(e);
      } finally {
        this.isClosed = true;
      }

      long diffMs = System.currentTimeMillis() - openedAt;
      log.debug("Took {} (ms) to read {} from stream", diffMs, numDocs);

      try {
        afterStreamClosed();
      } catch (Exception exc) {
        log.warn("Exception: {}", exc);
      }
    }

    return currentTuple != null;
  }

  protected void afterStreamClosed() throws Exception {
    // no-op - sub-classes can override if needed
  }

  protected Tuple fetchNextTuple() throws IOException {
    Tuple tuple = stream.read();
    if (tuple.EOF)
      return null;

    return tuple;
  }

  protected abstract TupleStream openStream();

  public synchronized Map nextTuple() {
    if (isClosed)
      throw new NoSuchElementException("already closed");

    if (currentTuple == null)
      throw new NoSuchElementException();

    final Tuple tempCurrentTuple = currentTuple;
    currentTuple = null;
    ++numDocs;
    increment();
    return tempCurrentTuple.getMap();
  }

  public synchronized Map next() {
    return nextTuple();
  }

  public void remove() {
    throw new UnsupportedOperationException("remove is not supported");
  }

  public Iterator<Map> iterator() {
    return this;
  }

  public long getNumDocs() {
    return numDocs;
  }
}
