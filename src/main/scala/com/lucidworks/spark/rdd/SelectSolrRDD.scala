package com.lucidworks.spark.rdd

import com.lucidworks.spark.query.StreamingResultsIterator
import com.lucidworks.spark.util.QueryConstants._
import com.lucidworks.spark.util.{SolrQuerySupport, SolrSupport}
import com.lucidworks.spark._
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.SolrDocument
import org.apache.solr.common.params.ShardParams
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConverters

class SelectSolrRDD(
    zkHost: String,
    collection: String,
    @transient private val sc: SparkContext,
    requestHandler: Option[String] = None,
    query : Option[String] = Option(DEFAULT_QUERY),
    fields: Option[Array[String]] = None,
    rows: Option[Int] = Option(DEFAULT_PAGE_SIZE),
    splitField: Option[String] = None,
    splitsPerShard: Option[Int] = None,
    solrQuery: Option[SolrQuery] = None,
    uKey: Option[String] = None,
    val maxRows: Option[Int] = None,
    val accumulator: Option[SparkSolrAccumulator] = None)
  extends SolrRDD[SolrDocument](zkHost, collection, sc, uKey = uKey)
  with LazyLogging {

  protected def copy(
    requestHandler: Option[String] = requestHandler,
    query: Option[String] = query,
    fields: Option[Array[String]] = fields,
    rows: Option[Int] = rows,
    splitField: Option[String] = splitField,
    splitsPerShard: Option[Int] = splitsPerShard,
    solrQuery: Option[SolrQuery] = solrQuery,
    uKey: Option[String] = uKey,
    maxRows: Option[Int] = maxRows): SelectSolrRDD = {
      new SelectSolrRDD(zkHost, collection, sc, requestHandler, query, fields, rows, splitField, splitsPerShard, solrQuery, uKey, maxRows, accumulator)
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[SolrDocument] = {

    val iterator: StreamingResultsIterator = split match {
      case partition: SelectSolrRDDPartition => {
        val url = getReplicaToQuery(partition, context.attemptNumber())
        val query = partition.query
        logger.debug(s"Using the shard url ${url} for getting partition data for split: ${split.index}")
        val solrRequestHandler = requestHandler.getOrElse(DEFAULT_REQUEST_HANDLER)
        query.setRequestHandler(solrRequestHandler)
        logger.debug(s"Using cursorMarks to fetch documents from ${partition.preferredReplica} for query: ${partition.query}")
        val resultsIterator = new StreamingResultsIterator(SolrSupport.getCachedHttpSolrClient(url, zkHost), partition.query, partition.cursorMark)
        context.addTaskCompletionListener { (context) =>
          logger.info(f"Fetched ${resultsIterator.getNumDocs} rows from shard $url for partition ${split.index}")
        }
        resultsIterator
      }
      case p: SolrLimitPartition => {
        // this is a single partition for the entire query ... we'll read all rows at once
        val query = p.query
        val solrRequestHandler = requestHandler.getOrElse(DEFAULT_REQUEST_HANDLER)
        query.setRequestHandler(solrRequestHandler)
        query.setRows(p.maxRows)
        query.set("distrib.singlePass", "true")
        query.setStart(null) // important! must start as null else the Iterator will advance the start position by the row size
        val resultsIterator = new StreamingResultsIterator(SolrSupport.getCachedCloudClient(p.zkhost), query)
        resultsIterator.setMaxSampleDocs(p.maxRows)
        context.addTaskCompletionListener { (context) =>
          logger.info(f"Fetched ${resultsIterator.getNumDocs} rows from the limit (${p.maxRows}) partition of ${p.collection}")
        }
        resultsIterator
      }
      case partition: AnyRef => throw new Exception("Unknown partition type '" + partition.getClass)
    }
    if (accumulator.isDefined)
      iterator.setAccumulator(accumulator.get)
    JavaConverters.asScalaIteratorConverter(iterator.iterator()).asScala
  }

  override def getPartitions: Array[Partition] = {
    val query = if (solrQuery.isEmpty) buildQuery else solrQuery.get
    val rq = requestHandler.getOrElse(DEFAULT_REQUEST_HANDLER)
    // if the user requested a max # of rows, use a single partition
    if (maxRows.isDefined) {
      logger.debug(s"Using a single limit partition for a maxRows=${maxRows} query.")
      return Array(SolrLimitPartition(0, zkHost, collection, maxRows.get, query))
    }

    val shardsTolerant : Boolean =
      if (query.get(ShardParams.SHARDS_TOLERANT) != null)
        query.get(ShardParams.SHARDS_TOLERANT).toBoolean
      else
        false
    val shards = SolrSupport.buildShardList(zkHost, collection, shardsTolerant)
    val numReplicas = shards.head.replicas.length
    val numSplits = splitsPerShard.getOrElse(calculateSplitsPerShard(query, shards.size, numReplicas))

    logger.debug(s"rq = $rq, setting query defaults for query = $query uniqueKey = $uniqueKey")
    SolrQuerySupport.setQueryDefaultsForShards(query, uniqueKey)
    // Freeze the index by adding a filter query on _version_ field
    val max = SolrQuerySupport.getMaxVersion(SolrSupport.getCachedCloudClient(zkHost), collection, query, DEFAULT_SPLIT_FIELD)
    if (max.isDefined) {
      val rangeFilter = DEFAULT_SPLIT_FIELD + ":[* TO " + max.get + "]"
      logger.debug("Range filter added to the query: " + rangeFilter)
      query.addFilterQuery(rangeFilter)
    }

    logger.debug(s"Using splitField=$splitField, splitsPerShard=$splitsPerShard, and numReplicas=$numReplicas for computing partitions.")

    logger.info(s"Updated Solr query: ${query.toString}")
    val partitions : Array[Partition] = if (numSplits > 1) {
      val splitFieldName = splitField.getOrElse(DEFAULT_SPLIT_FIELD)
      logger.debug(s"Applied $numSplits intra-shard splits on the $splitFieldName field for $collection to better utilize all active replicas. Set the 'split_field' option to override this behavior or set the 'splits_per_shard' option = 1 to disable splits per shard.")
      SolrPartitioner.getSplitPartitions(shards, query, splitFieldName, numSplits)
    } else {
      // no explicit split field and only one replica || splits_per_shard was explicitly set to 1, no intra-shard splitting needed
      SolrPartitioner.getShardPartitions(shards, query)
    }

    if (logger.underlying.isTraceEnabled()) {
      logger.trace(s"Found ${partitions.length} partitions: ${partitions.mkString(",")}")
    } else {
      logger.info(s"Found ${partitions.length} partitions")
    }
    partitions
  }

  override def query(q: String): SelectSolrRDD = copy(query = Some(q))

  override def query(solrQuery: SolrQuery): SelectSolrRDD = copy (solrQuery = Some (solrQuery))

  override def select(fl: String): SelectSolrRDD = copy(fields = Some(fl.split(",")))

  override def select(fl: Array[String]): SelectSolrRDD = copy(fields = Some(fl))

  override def rows(rows: Int): SelectSolrRDD = copy(rows = Some(rows))

  override def doSplits(): SelectSolrRDD = copy(splitField = Some(DEFAULT_SPLIT_FIELD))

  override def splitField(field: String): SelectSolrRDD = copy(splitField = Some(field))

  override def splitsPerShard(splitsPerShard: Int): SelectSolrRDD = copy(splitsPerShard = Some(splitsPerShard))

  override def requestHandler(requestHandler: String): SelectSolrRDD = copy(requestHandler = Some(requestHandler))

  override def buildQuery: SolrQuery = {
    var solrQuery : SolrQuery = SolrQuerySupport.toQuery(query.get)
    if (!solrQuery.getFields.eq(null) && solrQuery.getFields.length > 0) {
      solrQuery = solrQuery.setFields(fields.getOrElse(Array.empty[String]):_*)
    }
    if (!solrQuery.getRows.eq(null) && rows.isDefined) {
      solrQuery = solrQuery.setRows(rows.get)
    }

    solrQuery.set("collection", collection)
    solrQuery
  }

}

object SelectSolrRDD {
  def apply(zkHost: String, collection: String, sparkContext: SparkContext): SelectSolrRDD = {
    new SelectSolrRDD(zkHost, collection, sparkContext)
  }
}
