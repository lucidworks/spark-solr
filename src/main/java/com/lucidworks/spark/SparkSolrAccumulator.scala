package com.lucidworks.spark

import java.lang.Long

import org.apache.spark.util.AccumulatorV2

class SparkSolrAccumulator extends AccumulatorV2[java.lang.Long, java.lang.Long] {
  private var _count = 0L

  override def isZero: Boolean = _count == 0

  override def copy(): SparkSolrAccumulator = {
    val newAcc = new SparkSolrAccumulator
    newAcc._count = this._count
    newAcc
  }

  override def reset(): Unit = {
    _count = 0L
  }

  override def add(v: Long): Unit = {
    _count += v
  }

  def count: Long = _count

  override def merge(other: AccumulatorV2[Long, Long]): Unit = other match {
    case o: SparkSolrAccumulator =>
      _count += o.count
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: Long = _count

  def inc(): Unit = _count += 1
}
