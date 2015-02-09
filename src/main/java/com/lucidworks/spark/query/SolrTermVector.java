package com.lucidworks.spark.query;

import org.apache.solr.common.util.NamedList;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vector;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

public class SolrTermVector extends SparseVector implements Vector, Serializable {

  protected String docId;
  protected String[] terms;

  /**
   * Converts doc-level term vector information the Solr QueryResponse into a SolrTermVector.
   */
  public static SolrTermVector newInstance(String docId, HashingTF hashingTF, NamedList<Object> termList) {

    int termCount = termList.size();
    int[] indices = new int[termCount];
    double[] weights = new double[termCount];
    String[] terms = new String[termCount];
    Iterator<Map.Entry<String,Object>> termsIter = termList.iterator();
    int idx = -1;
    while (termsIter.hasNext()) {
      Map.Entry<String,Object> termEntry = termsIter.next();
      double weight = 0d;
      Object termVal = termEntry.getValue();
      if (termVal instanceof NamedList) {
        Object w = ((NamedList)termVal).get("tf-idf");
        if (w != null) {
          weight = (w instanceof Number) ? ((Number)w).doubleValue() : Double.parseDouble(w.toString());
        }
      }

      ++idx;
      String term = termEntry.getKey();
      terms[idx] = term;
      indices[idx] = hashingTF.indexOf(term);
      weights[idx] = weight;
    }

    return new SolrTermVector(docId, terms, hashingTF.numFeatures(), indices, weights);
  }

  protected SolrTermVector(String docId, String[] terms, int size, int[] indices, double[] weights) {
    super(size, indices, weights);
    this.docId = docId;
    this.terms = terms;

  }

  public String getDocumentId() {
    return docId;
  }

  public String toString() {
    return docId + ": " + super.toString();
  }
}
