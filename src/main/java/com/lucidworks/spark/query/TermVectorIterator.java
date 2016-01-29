package com.lucidworks.spark.query;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Returns an iterator over TermVectors
 */
public class TermVectorIterator extends PagedResultsIterator<Vector> {

  private String field = null;
  private HashingTF hashingTF = null;

  public TermVectorIterator(SolrClient solrServer, SolrQuery solrQuery, String cursorMark, String field, int numFeatures) {
    super(solrServer, solrQuery, cursorMark);
    this.field = field;
    hashingTF = new HashingTF(numFeatures);
  }

  protected List<Vector> processQueryResponse(QueryResponse resp) {
    NamedList<Object> response = resp.getResponse();

    NamedList<Object> termVectorsNL = (NamedList<Object>)response.get("termVectors");
    if (termVectorsNL == null)
      throw new RuntimeException("No termVectors in response! " +
        "Please check your query to make sure it is requesting term vector information from Solr correctly.");

    List<Vector> termVectors = new ArrayList<>(termVectorsNL.size());
    Iterator<Map.Entry<String, Object>> iter = termVectorsNL.iterator();
    while (iter.hasNext()) {
      Map.Entry<String, Object> next = iter.next();
      String nextKey = next.getKey();
      Object nextValue = next.getValue();
      if (nextValue instanceof NamedList) {
        NamedList nextList = (NamedList) nextValue;
        Object fieldTerms = nextList.get(field);
        if (fieldTerms != null && fieldTerms instanceof NamedList) {
          termVectors.add(SolrTermVector.newInstance(nextKey, hashingTF, (NamedList<Object>) fieldTerms));
        }
      }
    }

    SolrDocumentList docs = resp.getResults();
    totalDocs = docs.getNumFound();

    return termVectors;
  }
}

