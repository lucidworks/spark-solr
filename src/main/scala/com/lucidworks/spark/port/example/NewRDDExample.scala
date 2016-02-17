package com.lucidworks.spark.port.example

import com.lucidworks.spark.SparkApp
import com.lucidworks.spark.util.SolrSupport
import com.lucidworks.spark.port.SolrScalaRDD
import org.apache.commons.cli.{Option, CommandLine}
import org.apache.solr.client.solrj.request.CollectionAdminRequest
import org.apache.spark.{Logging, SparkContext, SparkConf}

class NewRDDExample extends SparkApp.RDDProcessor with Logging{

  override def getName: String = "new-rdd-example"

  override def getOptions: Array[Option] = Array(
    Option.builder().longOpt("query").hasArg.required(true).desc("Query to field").build()
  )

  override def run(conf: SparkConf, cli: CommandLine): Int = {
    val zkHost = cli.getOptionValue("zkHost", "localhost:9983")
    val collection = cli.getOptionValue("collection", "collection1")
    val queryStr = cli.getOptionValue("query", "*:*")

    // IMPORTANT: reload the collection to flush caches
    println(s"\nReloading collection $collection to flush caches!\n")
    val cloudSolrClient = SolrSupport.getSolrServer(zkHost)
    val req = new CollectionAdminRequest.Reload()
    req.setCollectionName(collection)
    cloudSolrClient.request(req)

    val sc = new SparkContext(conf)
    val rdd = new SolrScalaRDD(zkHost, collection, sc)
    val count = rdd.count()

    log.info("Count is " + count)
    sc.stop()
    0
  }
}
