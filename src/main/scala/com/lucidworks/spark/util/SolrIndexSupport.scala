package com.lucidworks.spark.util

import java.util.Random

import com.lucidworks.spark.{SolrReplica, SolrShard}
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.common.cloud._

import scala.collection.mutable.ListBuffer

import scala.collection.JavaConverters._

object SolrIndexSupport {

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
            if (liveNodes.contains(replicaCoreProps.getNodeName))
              replicas += new SolrReplica(0, replicaCoreProps.getCoreName, replicaCoreProps.getCoreUrl, replicaCoreProps.getNodeName)
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
