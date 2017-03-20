package com.lucidworks.spark.query;

import java.util.Iterator;

public abstract class ResultsIterator<E> implements Iterator<E>, Iterable<E> {

  public abstract long getNumDocs();

}
