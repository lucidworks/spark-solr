package com.lucidworks.spark

import org.apache.spark.Partitioner

class SplitPartitioner extends Partitioner {
  override def numPartitions: Int = ???

  override def getPartition(key: Any): Int = ???
}
