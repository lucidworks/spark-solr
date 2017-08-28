package com.lucidworks.spark

import scala.collection.concurrent.TrieMap

/**
  * Spark made it impossible to lookup an accumulator by name. Holding a global singleton here, so that external
  * clients that use this library can access the accumulators that are created by spark-solr for reading/writing
  * Get rid of this once Spark ties accumulators to the context SPARK-13051
  *
  * Not really happy about the global singleton but I don't see any other way to do it
  */
object SparkSolrAccumulatorContext {

  private val accMapping = TrieMap.empty[String, Long]

  def remove(name: String): Unit = {
    accMapping.remove(name)
  }

  def add(name: String, id: Long): Unit = {
    accMapping.put(name, id)
  }

  def getId(name: String): Option[Long] = {
    accMapping.get(name)
  }

  override def toString = s"SparkSolrAccumulatorContext($accMapping)"
}
