package com.lucidworks.spark

import com.lucidworks.spark.rdd.SolrRDD
import org.apache.spark.Logging

class EventsimTestSuite extends EventsimBuilder with Logging {

  test("Query using RDD") {
    val solrRDD = new SolrRDD(zkHost, collectionName, sc)
      .query("*:*")
      .rows(10)
      .select(Array("id"))

    val sparkCount = solrRDD.count()

    assert(sparkCount == solrRDD.solrCount.toLong)
    assert(sparkCount == eventSimCount)

  }
}
