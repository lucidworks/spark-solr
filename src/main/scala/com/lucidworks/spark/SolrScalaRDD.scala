package com.lucidworks.spark

import java.util.Random

import com.lucidworks.spark.query.StreamingResultsIterator
import com.lucidworks.spark.util.{SolrQuerySupport, SolrSupport}
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.common.SolrDocument
import org.apache.solr.common.cloud._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters
import scala.collection.JavaConversions._
import com.lucidworks.spark.util.QueryConstants._

import scala.collection.mutable.ListBuffer

/**
 * TODO: Add support for filter queries and be able to pass in SolrQuery object
 * @param zkHost
 * @param collection
 * @param query
 * @param fields
 * @param rows
 * @param splitField
 * @param splitsPerShard
 * @param sc
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
    val documentIterator = new StreamingResultsIterator(cloudClient, rddPartition.query, rddPartition.cursorMark).iterator()
    JavaConverters.asScalaIteratorConverter(documentIterator).asScala
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

  override protected def getPartitions: Array[Partition] = {
    val shards = buildShardList(cloudClient, collection)
    val partitioner : ShardPartitioner = new ShardPartitioner(buildQuery, shards)
    partitioner.getPartitions
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

    if (solrQuery.getSortField != null || solrQuery.getSortField.isEmpty)
      solrQuery = solrQuery.addSort(SolrQuery.SortClause.asc(uniqueKey))

    solrQuery
  }

  override def getPreferredLocations(split: Partition): Seq[String] = ???
//    split.asInstanceOf[SolrRDDPartition].solrShard.replicas

  protected def buildShardList(solrClient: CloudSolrClient,
                              collection: String): List[SolrShard] = {
    val zkStateReader: ZkStateReader = solrClient.getZkStateReader

    val clusterState: ClusterState = zkStateReader.getClusterState

    var collections: Array[String] = null
    if (clusterState.hasCollection(collection)) {
      collections = Array[String](collection)
    }
    else {
      val aliases: Aliases = zkStateReader.getAliases
      val aliasedCollections: String = aliases.getCollectionAlias(collection)
      if (aliasedCollections == null) throw new IllegalArgumentException("Collection " + collection + " not found!")
      collections = aliasedCollections.split(",")
    }

    val liveNodes  = clusterState.getLiveNodes
    val random: Random = new Random(5150)

    val shards = new ListBuffer[SolrShard]()
    for (coll <- collections) {
      for (slice <- clusterState.getSlices(coll)) {
        var replicas  =  new ListBuffer[SolrReplica]()
        for (r <- slice.getReplicas) {
          if (r.getState == Replica.State.ACTIVE) {
            val replicaCoreProps: ZkCoreNodeProps = new ZkCoreNodeProps(r)
            if (liveNodes.contains(replicaCoreProps.getNodeName))
              replicas += new SolrReplica(0, replicaCoreProps.getCoreName, replicaCoreProps.getCoreUrl)
          }
        }
        val numReplicas: Int = replicas.size
        if (numReplicas == 0) throw new IllegalStateException("Shard " + slice.getName + " in collection " + coll + " does not have any active replicas!")
        shards += new SolrShard(slice.getName.charAt(slice.getName.length-1).toString.toInt, slice.getName, replicas.toList)
      }
    }
    shards.toList
  }

}

