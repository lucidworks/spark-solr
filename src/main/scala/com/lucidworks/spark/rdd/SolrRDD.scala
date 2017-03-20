package com.lucidworks.spark.rdd

import com.lucidworks.spark._
import com.lucidworks.spark.util.QueryConstants._
import com.lucidworks.spark.util.SolrQuerySupport
import com.typesafe.scalalogging.LazyLogging
import org.apache.solr.client.solrj.SolrQuery
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.Random

abstract class SolrRDD[T](
    val zkHost: String,
    val collection: String,
    @transient sc: SparkContext,
    val requestHandler: Option[String] = None,
    query : Option[String] = Option(DEFAULT_QUERY),
    fields: Option[Array[String]] = None,
    rows: Option[Int] = Option(DEFAULT_PAGE_SIZE),
    splitField: Option[String] = None,
    splitsPerShard: Option[Int] = None,
    solrQuery: Option[SolrQuery] = None,
    uKey: Option[String] = None)
  extends RDD[T](sc, Seq.empty)
  with LazyLogging {

  val uniqueKey: String = if (uKey.isDefined) uKey.get else SolrQuerySupport.getUniqueKey(zkHost, collection.split(",")(0))

  protected def copy(
      requestHandler: Option[String] = requestHandler,
      query: Option[String] = query,
      fields: Option[Array[String]] = fields,
      rows: Option[Int] = rows,
      splitField: Option[String] = splitField,
      splitsPerShard: Option[Int] = splitsPerShard,
      solrQuery: Option[SolrQuery] = solrQuery)

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val urls: Seq[String] = Seq.empty
    split match {
      case partition: CloudStreamPartition => Seq.empty
      case partition: SolrRDDPartition => Array(partition.preferredReplica.replicaHostName)
      case partition: AnyRef => logger.warn("Unknown partition type '" + partition.getClass + "'")
    }
    urls
  }

  def query(q: String)

  def query(solrQuery: SolrQuery): this

  def select(fl: String): Self = copy(fields = Some(fl.split(",")))

  def select(fl: Array[String]): Self = copy(fields = Some(fl))

  def rows(rows: Int): Self = copy(rows = Some(rows))

  def doSplits(): Self = copy(splitField = Some(DEFAULT_SPLIT_FIELD))

  def splitField(field: String): Self = copy(splitField = Some(field))

  def splitsPerShard(splitsPerShard: Int): Self = copy(splitsPerShard = Some(splitsPerShard))

  def useExportHandler: Self = copy(requestHandler = Some(QT_EXPORT))

  def requestHandler(requestHandler: String): Self = copy(requestHandler = Some(requestHandler))

  def solrCount: BigInt = SolrQuerySupport.getNumDocsFromSolr(collection, zkHost, solrQuery)

  protected def buildQuery: SolrQuery = {
    var solrQuery : SolrQuery = SolrQuerySupport.toQuery(query.get)
    if (!solrQuery.getFields.eq(null) && solrQuery.getFields.length > 0) {
      solrQuery = solrQuery.setFields(fields.getOrElse(Array.empty[String]):_*)
    }
    if (!solrQuery.getRows.eq(null)) {
      solrQuery = solrQuery.setRows(rows.get)
    }

    solrQuery.set("collection", collection)
    solrQuery
  }

}

object SolrRDD {

  def randomReplicaLocation(solrShard: SolrShard): String = {
    randomReplica(solrShard).replicaUrl
  }

  def randomReplica(solrShard: SolrShard): SolrReplica = {
    solrShard.replicas(Random.nextInt(solrShard.replicas.size))
  }

  def apply(
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
      uKey: Option[String] = None): SolrRDD[_] = {
    if (requestHandler.isDefined) {
      if (requiresStreamingRDD(requestHandler.get)) {
        new StreamingSolrRDD(zkHost, collection, sc, requestHandler, query, fields, rows, splitField, splitsPerShard, solrQuery, uKey)
      } else {
        new SelectSolrRDD(zkHost, collection, sc, requestHandler, query, fields, rows, splitField, splitsPerShard, solrQuery, uKey)
      }
    } else {
      new SelectSolrRDD(zkHost, collection, sc, Some(DEFAULT_REQUEST_HANDLER), query, fields, rows, splitField, splitsPerShard, solrQuery, uKey)
    }
  }

  def requiresStreamingRDD(rq: String): Boolean = {
    rq == QT_EXPORT || rq == QT_STREAM || rq == QT_SQL
  }

}

