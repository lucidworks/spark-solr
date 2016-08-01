package com.lucidworks.spark.query;

import org.apache.solr.common.SolrDocument;

import java.util.Iterator;

public abstract class ResultsIterator implements Iterator<SolrDocument>, Iterable<SolrDocument> {

  public abstract long getNumDocs();

}
