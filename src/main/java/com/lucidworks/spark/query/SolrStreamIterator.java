package com.lucidworks.spark.query;

import com.lucidworks.spark.util.SolrQuerySupport;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.common.SolrDocument;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * An iterator over a stream of query results from one Solr Core. It is a
 * wrapper over the SolrStream to adapt it to an iterator interface.
 *
 * This iterator is not thread safe. It is intended to be used within the
 * context of a single thread.
 */
public class SolrStreamIterator extends ResultsIterator {

    private static final Logger log = Logger.getLogger(SolrStreamIterator.class);

    private SolrStream stream;
    protected SolrClient solrServer;
    protected SolrQuery solrQuery;
    protected boolean closeAfterIterating;
    protected long numDocs = 0;
    private boolean isOpenStream;
    private SolrDocument currentTuple = null;

    public SolrStreamIterator(String shardUrl, SolrClient solrServer, SolrQuery solrQuery) {
        this.solrServer = solrServer;
        this.closeAfterIterating = !(solrServer instanceof CloudSolrClient);
        this.solrQuery = solrQuery;

        if (solrQuery.getRequestHandler() == null) {
            solrQuery = solrQuery.setRequestHandler("/export");
        }
        solrQuery.setRows(null);
        Map<String, Object> params = getAll(solrQuery, null,
                (String[]) IteratorUtils.toArray(solrQuery.getParameterNamesIterator(), String.class));
        SolrQuerySupport.validateExportHandlerQuery(solrServer, solrQuery);
        this.stream = new SolrStream(shardUrl, params);
        openStream();
    }

    /**
     * Copy all params to the given map or if the given map is null
     * create a new one
     */
    public Map<String, Object> getAll(SolrQuery solrQuery, Map<String, Object> sink, String... params) {
        if(sink == null) sink = new LinkedHashMap<>();
        for (String param : params) {
            String[] v = solrQuery.getParams(param);
            if(v != null && v.length>0 ) {
                if(v.length == 1) {
                    sink.put(param, v[0]);
                } else {
                    sink.put(param,v);
                }
            }
        }
        return sink;
    }

    public synchronized boolean hasNext() {
        try {
            if (currentTuple == null) {
                currentTuple = fetchNextDocument();
            }
        } catch (IOException e) {
            log.error("Failed to fetch next document for query: " + solrQuery.toQueryString(), e);
            throw new RuntimeException(e);
        }

        if (currentTuple == null && closeAfterIterating) {
            // TODO: upstream change to SolrStream to make it AutoCloseable
            try {
                stream.close();
            } catch (IOException e) {
                log.error("Failed to close the SolrStream.", e);
                throw new RuntimeException(e);
            }
            IOUtils.closeQuietly(solrServer);
        }

        return currentTuple != null;
    }

    protected SolrDocument fetchNextDocument() throws IOException {
        Tuple tuple = stream.read();
        if (tuple.EOF)
            return null;

        final SolrDocument doc = new SolrDocument();
        for (Object key : tuple.fields.keySet()) {
            doc.setField((String) key, tuple.get(key));
        }

        return doc;
    }

    private void openStream() {
        try {
            if (isOpenStream)
                return;
            stream.open();
            // stream.setTrace(true);
            isOpenStream = true;
        } catch (IOException e1) {
            throw new RuntimeException(e1);
        }
    }

    public synchronized SolrDocument next() {
        if (currentTuple == null)
            throw new NoSuchElementException();

        final SolrDocument tempCurrentTuple = currentTuple;
        currentTuple = null;
        ++numDocs;
        return tempCurrentTuple;
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
}
