package com.lucidworks.spark.query;

import com.lucidworks.spark.SparkSolrAccumulator;

import java.util.Iterator;

public abstract class ResultsIterator<E> implements Iterator<E>, Iterable<E> {

  protected SparkSolrAccumulator acc;

  public SparkSolrAccumulator getAccumulator() {
    return this.acc;
  }

  public abstract long getNumDocs();

  public void setAccumulator(SparkSolrAccumulator acc) {
    this.acc = acc;
  }

  protected void increment() {
    if (acc != null) {
      acc.inc();
    }
  }
}
