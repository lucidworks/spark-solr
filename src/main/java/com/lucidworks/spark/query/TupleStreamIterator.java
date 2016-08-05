package com.lucidworks.spark.query;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.SolrParams;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator over a stream of tuples from Solr.
 * <p>
 * This iterator is not thread safe. It is intended to be used within the
 * context of a single thread.
 */
public abstract class TupleStreamIterator extends ResultsIterator {

  private static final Logger log = Logger.getLogger(TupleStreamIterator.class);

  protected TupleStream stream;
  protected long numDocs = 0;
  protected SolrParams solrParams;
  private Tuple currentTuple = null;
  private long openedAt;

  public TupleStreamIterator(SolrParams solrParams) {
    this.solrParams = solrParams;
  }

  public synchronized boolean hasNext() {
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
      }

      long diffMs = System.currentTimeMillis() - openedAt;
      log.info("Took "+diffMs+" (ms) to read "+numDocs+" from stream.");

      try {
        afterStreamClosed();
      } catch (Exception exc) {
        log.warn(exc);
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

  public synchronized Tuple nextTuple() {
    if (currentTuple == null)
      throw new NoSuchElementException();

    final Tuple tempCurrentTuple = currentTuple;
    currentTuple = null;
    ++numDocs;
    return tempCurrentTuple;
  }

  public synchronized SolrDocument next() {
    return tuple2doc(nextTuple());
  }

  protected SolrDocument tuple2doc(Tuple tuple) {
    final SolrDocument doc = new SolrDocument();
    for (Object key : tuple.fields.keySet()) {
      doc.setField((String) key, tuple.get(key));
    }
    return doc;
  }

  public void remove() {
    throw new UnsupportedOperationException("remove is not supported");
  }

  public Iterator<SolrDocument> iterator() {
    return this;
  }

  public long getNumDocs() {
    return numDocs;
  }
}
