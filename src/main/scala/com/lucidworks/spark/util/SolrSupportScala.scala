package com.lucidworks.spark.util

import java.net.{URL, InetAddress}
import java.util.Random

import com.lucidworks.spark.query.{ShardSplit, StringFieldShardSplitStrategy, NumberFieldShardSplitStrategy, ShardSplitStrategy}
import com.lucidworks.spark.util.SolrQuerySupport.SolrFieldMeta
import com.lucidworks.spark.{SolrScalaRDD, SplitRDDPartition, SolrReplica, SolrShard}
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.request.CollectionAdminRequest.SplitShard
import org.apache.solr.common.cloud._
import org.apache.spark.Logging
import org.apache.spark.sql.types.{DataTypes, DataType}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import scala.collection.JavaConverters._

object SolrSupportScala extends Logging {

  def buildShardList(solrClient: CloudSolrClient,
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

    val shards = new ListBuffer[SolrShard]()
    for (coll <- collections) {
      for (slice: Slice <- clusterState.getSlices(coll).asScala) {
        var replicas  =  new ListBuffer[SolrReplica]()
        for (r: Replica <- slice.getReplicas.asScala) {
          if (r.getState == Replica.State.ACTIVE) {
            val replicaCoreProps: ZkCoreNodeProps = new ZkCoreNodeProps(r)
            if (liveNodes.contains(replicaCoreProps.getNodeName)) {
              try {
                val addresses = InetAddress.getAllByName(new URL(replicaCoreProps.getBaseUrl).getHost)
                replicas += new SolrReplica(0, replicaCoreProps.getCoreName, replicaCoreProps.getCoreUrl, replicaCoreProps.getNodeName, addresses)
              } catch {
                case e : Exception => log.warn("Error resolving ip address " + replicaCoreProps.getNodeName + " . Exception " + e)
                  replicas += new SolrReplica(0, replicaCoreProps.getCoreName, replicaCoreProps.getCoreUrl, replicaCoreProps.getNodeName, Array.empty[InetAddress])
              }

            }

          }
        }
        val numReplicas: Int = replicas.size
        if (numReplicas == 0) throw new IllegalStateException("Shard " + slice.getName + " in collection " + coll + " does not have any active replicas!")
        shards += new SolrShard(slice.getName, replicas.toList)
      }
    }
    shards.toList
  }

  def splitShards(query: SolrQuery,
                  solrShard: SolrShard,
                  splitFieldName: String,
                  splitsPerShard: Int): List[ShardSplit] = {
    // Get the field type of split field
    var fieldDataType: Option[DataType] = None
    if ("_version_".equals(splitFieldName)) {
      fieldDataType = Some(DataTypes.LongType)
    } else {
      val fieldMetaMap = SolrQuerySupport.getFieldTypes(Array(splitFieldName), "")
      val solrFieldMeta = fieldMetaMap.get(splitFieldName)
      if (solrFieldMeta != null) {
        val fieldTypeClass = solrFieldMeta.fieldTypeClass
        fieldDataType = Some(SolrQuerySupport.SOLR_DATA_TYPES.get(fieldTypeClass))
      } else {
        log.warn("No field metadata found for " + splitFieldName + ", assuming it is a String!")
        fieldDataType = Some(DataTypes.StringType)
      }
    }
    if (fieldDataType.isEmpty)
      throw new IllegalArgumentException("Cannot determine DataType for split field " + splitFieldName)

    getSplits(fieldDataType.get, splitFieldName, splitsPerShard, query, solrShard)
  }

  def getSplits(fd: DataType, sF: String, sPS: Int, query: SolrQuery, shard: SolrShard): List[ShardSplit] = {
    var splitStrategy: Option[ShardSplitStrategy] = None
    if (fd.equals(DataTypes.LongType) || fd.equals(DataTypes.IntegerType)) {
      splitStrategy = Some(new NumberFieldShardSplitStrategy)
    } else if (fd.equals(DataTypes.StringType)) {
      splitStrategy = Some(new StringFieldShardSplitStrategy)
    } else {
      throw new IllegalArgumentException("Can only split shards on fields of type: long, int or String!")
    }
    splitStrategy.get.getSplits(SolrScalaRDD.randomReplicaLocation(shard), query, sF, sPS)
  }

}
