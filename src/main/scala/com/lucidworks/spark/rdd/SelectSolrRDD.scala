package com.lucidworks.spark.rdd

import com.lucidworks.spark.{SolrPartitioner, SolrRDDPartition}
import com.lucidworks.spark.query.StreamingResultsIterator
import com.lucidworks.spark.util.QueryConstants._
import com.lucidworks.spark.util.{SolrQuerySupport, SolrSupport}
import com.typesafe.scalalogging.LazyLogging
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.SolrDocument
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConverters

class SelectSolrRDD(
    zkHost: String,
    collection: String,
    @transient sc: SparkContext,
    requestHandler: Option[String] = None,
    query : Option[String] = Option(DEFAULT_QUERY),
    fields: Option[Array[String]] = None,
    rows: Option[Int] = Option(DEFAULT_PAGE_SIZE),
    splitField: Option[String] = None,
    splitsPerShard: Option[Int] = None,
    solrQuery: Option[SolrQuery] = None,
    uKey: Option[String] = None)
  extends SolrRDD[SolrDocument](zkHost, collection, sc)
  with LazyLogging {

  override type Self = SelectSolrRDD

  protected def copy(
    requestHandler: Option[String] = requestHandler,
    query: Option[String] = query,
    fields: Option[Array[String]] = fields,
    rows: Option[Int] = rows,
    splitField: Option[String] = splitField,
    splitsPerShard: Option[Int] = splitsPerShard,
    solrQuery: Option[SolrQuery] = solrQuery): Self = {
      new SelectSolrRDD(zkHost, collection, sc, requestHandler, query, fields, rows, splitField, splitsPerShard, solrQuery, uKey)
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[SolrDocument] = {
    split match {
      case partition: SolrRDDPartition =>
        //TODO: Add backup mechanism to StreamingResultsIterator by being able to query any replica in case the main url goes down
        val url = partition.preferredReplica.replicaUrl
        val query = partition.query
        logger.info("Using the shard url " + url + " for getting partition data for split: "+ split.index)
        val solrRequestHandler = requestHandler.getOrElse(DEFAULT_REQUEST_HANDLER)
        query.setRequestHandler(solrRequestHandler)
        logger.info("Using cursorMarks to fetch documents from "+partition.preferredReplica+" for query: "+partition.query)
        val resultsIterator = new StreamingResultsIterator(SolrSupport.getHttpSolrClient(url), partition.query, partition.cursorMark)
        context.addTaskCompletionListener { (context) =>
          logger.info(f"Fetched ${resultsIterator.getNumDocs} rows from shard $url for partition ${split.index}")
        }
        JavaConverters.asScalaIteratorConverter(resultsIterator.iterator()).asScala

      case partition: AnyRef => throw new Exception("Unknown partition type '" + partition.getClass)
    }
  }

  override def getPartitions: Array[Partition] = {
    val query = if (solrQuery.isEmpty) buildQuery else solrQuery.get
    val rq = requestHandler.getOrElse(DEFAULT_REQUEST_HANDLER)

    val shards = SolrSupport.buildShardList(zkHost, collection)
    logger.info(s"rq = $rq, setting query defaults for query = $query uniqueKey = $uniqueKey")
    SolrQuerySupport.setQueryDefaultsForShards(query, uniqueKey)
    // Freeze the index by adding a filter query on _version_ field
    val max = SolrQuerySupport.getMaxVersion(SolrSupport.getCachedCloudClient(zkHost), collection, query, DEFAULT_SPLIT_FIELD)
    if (max.isDefined) {
      val rangeFilter = DEFAULT_SPLIT_FIELD + ":[* TO " + max.get + "]"
      logger.info("Range filter added to the query: " + rangeFilter)
      query.addFilterQuery(rangeFilter)
    }

    val numReplicas = shards.head.replicas.length
    val numSplits = splitsPerShard.getOrElse(2 * numReplicas)
    logger.info(s"Using splitField=$splitField, splitsPerShard=$splitsPerShard, and numReplicas=$numReplicas for computing partitions.")

    val partitions : Array[Partition] = if (numSplits > 1) {
      val splitFieldName = splitField.getOrElse(DEFAULT_SPLIT_FIELD)
      logger.info(s"Applied $numSplits intra-shard splits on the $splitFieldName field for $collection to better utilize all active replicas. Set the 'split_field' option to override this behavior or set the 'splits_per_shard' option = 1 to disable splits per shard.")
      SolrPartitioner.getSplitPartitions(shards, query, splitFieldName, numSplits)
    } else {
      // no explicit split field and only one replica || splits_per_shard was explicitly set to 1, no intra-shard splitting needed
      SolrPartitioner.getShardPartitions(shards, query)
    }

    if (logger.underlying.isDebugEnabled) {
      logger.debug(s"Found ${partitions.length} partitions: ${partitions.mkString(",")}")
    } else {
      logger.info(s"Found ${partitions.length} partitions.")
    }
    partitions
  }
}

object SelectSolrRDD {
  def apply(zkHost: String, collection: String, sparkContext: SparkContext): SelectSolrRDD = {
    new SelectSolrRDD(zkHost, collection, sparkContext)
  }
}
