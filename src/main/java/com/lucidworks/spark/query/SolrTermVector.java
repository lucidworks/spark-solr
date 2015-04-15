package com.lucidworks.spark.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.solr.common.util.NamedList;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vector;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SolrTermVector extends SparseVector implements Vector, Serializable {

  private static final Analyzer stdAnalyzer = new StandardAnalyzer();

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

  public static SolrTermVector newInstance(String docId, HashingTF hashingTF, String rawText) {
    return newInstance(docId, hashingTF, stdAnalyzer, new StringReader(rawText));
  }

  public static SolrTermVector newInstance(String docId, HashingTF hashingTF, Analyzer analyzer, Reader rawTextReader) {
    List<String> termList = new ArrayList<String>();
    TokenStream ts = null;
    try {
      ts = analyzer.tokenStream(null, rawTextReader);
      ts.reset();
      while (ts.incrementToken())
        termList.add(ts.getAttribute(CharTermAttribute.class).toString());
      ts.end();
    } catch (IOException ioExc) {
      throw new RuntimeException(ioExc);
    } finally {
      if (ts != null)
        try {
          ts.close();
        } catch (IOException ignore) {}
    }

    int termCount = termList.size();
    int[] indices = new int[termCount];
    double[] weights = new double[termCount];
    String[] terms = new String[termCount];
    for (int t=0; t < termList.size(); t++) {
      String term = termList.get(t);
      terms[t] = term;
      indices[t] = hashingTF.indexOf(term);
      weights[t] = 1d;
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
