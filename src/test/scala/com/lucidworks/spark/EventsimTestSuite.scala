package com.lucidworks.spark

import com.lucidworks.spark.rdd.SolrRDD
import org.apache.spark.Logging

class EventsimTestSuite extends EventsimBuilder with Logging {

  test("Simple Query using RDD") {
    val solrRDD = new SolrRDD(zkHost, collectionName, sc)
      .query("*:*")
      .rows(10)
      .select(Array("id"))
    assert(solrRDD.getNumPartitions == numShards)
    testCommons(solrRDD)
  }

  test("Split partitions default") {
    val solrRDD = new SolrRDD(zkHost, collectionName, sc).doSplits()
    testCommons(solrRDD)
  }

  test("Split partitions by field name") {
    val solrRDD = new SolrRDD(zkHost, collectionName, sc).splitField("id").splitsPerShard(2)
    testCommons(solrRDD)
  }

  def testCommons(solrRDD: SolrRDD): Unit = {
    val sparkCount = solrRDD.count()

    // assert counts
    assert(sparkCount == solrRDD.solrCount.toLong)
    assert(sparkCount == eventSimCount)
  }

}
