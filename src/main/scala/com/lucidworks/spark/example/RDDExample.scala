package com.lucidworks.spark.example

import com.lucidworks.spark.rdd.SolrRDD
import com.lucidworks.spark.util.SolrSupport
import com.lucidworks.spark.SparkApp
import org.apache.commons.cli.{CommandLine, Option}
import org.apache.solr.client.solrj.request.CollectionAdminRequest
import org.apache.spark.{Logging, SparkConf, SparkContext}

class RDDExample extends SparkApp.RDDProcessor with Logging {

  override def getName: String = "old-rdd-example"

  override def getOptions: Array[Option] = Array(
    Option.builder().longOpt("query").hasArg.required(true).desc("Query to field").build()
  )

  override def run(conf: SparkConf, cli: CommandLine): Int = {
    val zkHost = cli.getOptionValue("zkHost", "localhost:9983")
    val collection = cli.getOptionValue("collection", "collection1")
    val queryStr = cli.getOptionValue("query", "*:*")

    // IMPORTANT: reload the collection to flush caches
    println(s"\nReloading collection $collection to flush caches!\n")
    val cloudSolrClient = SolrSupport.getCachedCloudClient(zkHost)
    val req = new CollectionAdminRequest.Reload()
    req.setCollectionName(collection)
    cloudSolrClient.request(req)

    val sc = new SparkContext(conf)
    val rdd = new SolrRDD(zkHost, collection, sc)
    val count = rdd.query(queryStr).count()

    log.info("Count is " + count)
    sc.stop()
    0
  }
}
