package com.lucidworks.spark

import com.lucidworks.spark.util.SolrSupport
import com.lucidworks.spark.util.SolrSupport.WorkerShardSplit
import org.apache.solr.client.solrj.SolrQuery

// Unit tests for testing splits for select and export handler
class TestShardSplits extends SparkSolrFunSuite{
  val splitFieldName = "_version_"

  test("Shard splits with 1 shard and 2 replicas") {
    val query = new SolrQuery("*:*")
    val solrShard = SolrShard("shard1", List(
      SolrReplica(0, "replica1", "http://replica1", "localhost", Array()),
      SolrReplica(1, "replica2", "http://replica2", "localhost", Array()))
    )

    val splits: List[WorkerShardSplit] = SolrSupport.getShardSplits(query, solrShard, splitFieldName, 5)
    assert(splits.size == 5)
    splits.zipWithIndex.foreach {
      case (split, i) =>
        assert(split.query.get("partitionKeys") == splitFieldName)
        assert(split.query.getFilterQueries.apply(0) == s"{!hash workers=5 worker=$i}")
        if (i%2 == 0)
          assert(split.replica.replicaName == "replica1")
        else
          assert(split.replica.replicaName == "replica2")
    }
  }


  test("Shard partitions with 2 shards and 2 replicas") {
    val query = new SolrQuery("*:*")
    val solrShard1 = SolrShard("shard1", List(
      SolrReplica(0, "replica1", "http://replica1", "localhost", Array()),
      SolrReplica(1, "replica2", "http://replica2", "localhost", Array()))
    )
    val solrShard2 = SolrShard("shard2", List(
      SolrReplica(0, "replica1", "http://replica1", "localhost", Array()),
      SolrReplica(1, "replica2", "http://replica2", "localhost", Array()))
    )
    val solrShards = List(solrShard1, solrShard2)

    val partitions = SolrPartitioner.getSplitPartitions(solrShards, query, splitFieldName, 2)
    assert(partitions.length == 4)
    partitions.zipWithIndex.foreach {
      case (partition, i) =>
        val spartition = partition.asInstanceOf[SelectSolrRDDPartition]
        assert(spartition.cursorMark == "*")
        assert(spartition.query.get("partitionKeys") == splitFieldName)
        assert(spartition.query.getFilterQueries.apply(0) == s"{!hash workers=2 worker=${i%2}}")
        if (i < 2)
          assert(spartition.solrShard == solrShard1)
        if (i > 2)
          assert(spartition.solrShard == solrShard2)
        if (i%2 == 0)
          assert(spartition.preferredReplica.replicaName == "replica1")
        else
          assert(spartition.preferredReplica.replicaName == "replica2")
    }
  }

  test("Export handler splits with 1 shard and 2 replicas") {
    val query = new SolrQuery("*:*")
    val shard = SolrShard("shard1", List(
      SolrReplica(0, "replica1", "http://replica1", "localhost", Array()),
      SolrReplica(1, "replica2", "http://replica2", "localhost", Array()))
    )
    val splits = SolrSupport.getExportHandlerSplits(query, shard, splitFieldName, 4)
    splits.zipWithIndex.foreach{
      case (split, i) =>
        assert(split.query.equals(query))
        assert(split.numWorkers == 4)
        assert(split.workerId == i)
        if (i%2 == 0)
          assert(split.replica.replicaName == "replica1")
        else
          assert(split.replica.replicaName == "replica2")

    }
  }

  test("Export handler partitions with 2 shards and 2 replicas") {
    val query = new SolrQuery("*:*")
    val solrShard1 = SolrShard("shard1", List(
      SolrReplica(0, "replica1", "http://replica1", "localhost", Array()),
      SolrReplica(1, "replica2", "http://replica2", "localhost", Array()))
    )
    val solrShard2 = SolrShard("shard2", List(
      SolrReplica(0, "replica1", "http://replica1", "localhost", Array()),
      SolrReplica(1, "replica2", "http://replica2", "localhost", Array()))
    )
    val solrShards = List(solrShard1, solrShard2)

    val partitions = SolrPartitioner.getExportHandlerPartitions(solrShards, query, splitFieldName, 4)
    assert(partitions.length == 8)
    partitions.zipWithIndex.foreach {
      case (partition, i) =>
        val hpartition = partition.asInstanceOf[ExportHandlerPartition]
        assert(hpartition.index == i)
        assert(hpartition.numWorkers == 4)
        if (i < 4)
          assert(hpartition.solrShard == solrShard1)
        if (i > 4)
          assert(hpartition.solrShard == solrShard2)
        if (i%2 == 0)
          assert(hpartition.preferredReplica.replicaName == "replica1")
        else
          assert(hpartition.preferredReplica.replicaName == "replica2")
    }
  }

}
