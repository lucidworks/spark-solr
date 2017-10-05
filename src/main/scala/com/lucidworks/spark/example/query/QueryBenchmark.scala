package com.lucidworks.spark.example.query

import com.lucidworks.spark.SparkApp
import com.lucidworks.spark.rdd.SelectSolrRDD
import com.lucidworks.spark.util.SolrSupport
import org.apache.commons.cli.{CommandLine, Option}
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.request.CollectionAdminRequest
import org.apache.spark.{SparkConf, SparkContext}

class QueryBenchmark extends SparkApp.RDDProcessor {
  def getName: String = "query-solr-benchmark"

  def getOptions: Array[Option] = {
    Array(
      Option.builder().longOpt("query").hasArg.required(false).desc("URL encoded Solr query to send to Solr, default is *:* (all docs)").build,
      Option.builder().longOpt("rows").hasArg.required(false).desc("Number of rows to fetch at once, default is 1000").build,
      Option.builder().longOpt("splitsPerShard").hasArg.required(false).desc("Number of splits per shard, default is 3").build,
      Option.builder().longOpt("splitField").hasArg.required(false).desc("Name of an indexed numeric field (preferably long type) used to split a shard, default is _version_").build,
      Option.builder().longOpt("fields").hasArg.required(false).desc("Comma-delimited list of fields to be returned from the query; default is all fields").build
    )
  }

  def run(conf: SparkConf, cli: CommandLine): Int = {

    val zkHost = cli.getOptionValue("zkHost", "localhost:9983")
    val collection = cli.getOptionValue("collection", "collection1")
    val queryStr = cli.getOptionValue("query", "*:*")
    val rows = cli.getOptionValue("rows", "1000").toInt
    val splitsPerShard = cli.getOptionValue("splitsPerShard", "3").toInt
    val splitField = cli.getOptionValue("splitField", "_version_")

    val sc = new SparkContext(conf)

    val solrQuery: SolrQuery = new SolrQuery(queryStr)

    val fields = cli.getOptionValue("fields", "")
    if (!fields.isEmpty)
      fields.split(",").foreach(solrQuery.addField)

    solrQuery.addSort(new SolrQuery.SortClause("id", "asc"))
    solrQuery.setRows(rows)

    val solrRDD: SelectSolrRDD = new SelectSolrRDD(zkHost, collection, sc)

    var startMs: Long = System.currentTimeMillis

    var count = solrRDD.query(solrQuery).splitField(splitField).splitsPerShard(splitsPerShard).count()

    var tookMs: Long = System.currentTimeMillis - startMs
    println(s"\nTook $tookMs ms read $count docs using queryShards with $splitsPerShard splits")

    // IMPORTANT: reload the collection to flush caches
    println(s"\nReloading collection $collection to flush caches!\n")
    val cloudSolrClient = SolrSupport.getCachedCloudClient(zkHost)
    val req = CollectionAdminRequest.reloadCollection(collection)
    cloudSolrClient.request(req)

    startMs = System.currentTimeMillis

    count = solrRDD.query(solrQuery).count()

    tookMs = System.currentTimeMillis - startMs
    println(s"\nTook $tookMs ms read $count docs using queryShards")

    sc.stop()
    0
  }
}
