package com.lucidworks.spark

import com.lucidworks.spark.query.StreamingResultsIterator
import com.lucidworks.spark.util.QueryConstants._
import com.lucidworks.spark.util.{SolrIndexSupport, SolrQuerySupport, SolrSupport}
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.SolrDocument
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters

/**
 * TODO: Add support for filter queries and be able to pass in SolrQuery object
 */
class SolrScalaRDD(
    val zkHost: String,
    val collection: String,
    @transient val sc: SparkContext,
    val query : Option[String] = Option(DEFAULT_QUERY),
    val fields: Option[Array[String]] = Option(null),
    val rows: Option[Int] = Option(DEFAULT_PAGE_SIZE),
    val splitField: Option[String] = Option(null),
    val splitsPerShard: Option[Int] = Option(DEFAULT_SPLITS_PER_SHARD))
  extends RDD[SolrDocument](sc, Seq.empty) with Logging{ //TODO: Do we need to pass any deps on parent RDDs for Solr?

  val uniqueKey = SolrQuerySupport.getUniqueKey(zkHost, collection)
  @transient val cloudClient = SolrSupport.getSolrClient(zkHost)

  protected def copy(
    query: Option[String] = query,
    fields: Option[Array[String]] = fields,
    rows: Option[Int] = rows,
    splitField: Option[String] = splitField,
    splitsPerShard: Option[Int] = splitsPerShard): SolrScalaRDD = {
    new SolrScalaRDD(zkHost, collection, sc, query, fields, rows, splitField, splitsPerShard)
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[SolrDocument] = {
    val rddPartition = split.asInstanceOf[SolrRDDPartition]
    log.info("Computing the partition " + rddPartition + "' for task" + context)
    //TODO: Add backup mechanism by being able to query any replica
    val shardUrl = rddPartition.solrShard.replicas(0).replicaLocation
    val streamingIterator = new StreamingResultsIterator(
      SolrSupport.getHttpSolrClient(shardUrl),
      rddPartition.query,
      rddPartition.cursorMark)

    context.addTaskCompletionListener { (context) =>
      logInfo(f"Fetched ${streamingIterator.getNumDocs} rows from shard $shardUrl for partition ${split.index}")
    }
    JavaConverters.asScalaIteratorConverter(streamingIterator.iterator()).asScala
  }

  override protected def getPartitions: Array[Partition] = {
    val shards = SolrIndexSupport.buildShardList(cloudClient, collection)
    val partitioner : ShardPartitioner = new ShardPartitioner(buildQuery, shards)
    partitioner.getPartitions
  }

  //TODO: Implement this and return the list of replicas. How to co-ordinate the shard url between this and compute method
    override def getPreferredLocations(split: Partition): Seq[String] = {
    val urls: Seq[String] = Seq.empty
    split.asInstanceOf[SolrRDDPartition].solrShard.replicas.foreach(f => urls + f.replicaHostName)
    urls
  }

  def query(q: String): SolrScalaRDD = {
    copy(query = Option(q))
  }

  def select(fl: Array[String]): SolrScalaRDD = {
    copy(fields = Option(fl))
  }

  def rows(rows: Int): SolrScalaRDD = {
    copy(rows = Option(rows))
  }

  def splitField(field: String): SolrScalaRDD = {
    copy(splitField = Option(field))
  }

  def splitsPerShard(splitsPerShard: Int): SolrScalaRDD = {
    copy(splitsPerShard = Option(splitsPerShard))
  }

  protected def buildQuery: SolrQuery = {
    var solrQuery : SolrQuery = SolrQuerySupport.toQuery(query.get)
    if (!solrQuery.getFields.eq(null) && solrQuery.getFields.length > 0)
      solrQuery = solrQuery.setFields(fields.getOrElse(Array.empty[String]):_*)
    if (!solrQuery.getRows.eq(null))
      solrQuery = solrQuery.setRows(rows.get)

    solrQuery.set("collection", collection)
    solrQuery.set("distrib", "false")
    solrQuery.setStart(0)

    if (solrQuery.getSortField == null || solrQuery.getSortField.isEmpty)
      solrQuery = solrQuery.addSort(SolrQuery.SortClause.asc(uniqueKey))

    solrQuery
  }

}

