package com.lucidworks.spark

import com.lucidworks.spark.util.{SolrCloudUtil, SolrSupport}
import org.apache.solr.client.solrj.SolrQuery
import org.apache.spark.sql.SaveMode._
import com.lucidworks.spark.util.ConfigurationConstants._
import org.apache.solr.client.solrj.request.CollectionAdminRequest
import org.apache.solr.client.solrj.request.CollectionAdminRequest.{CreateAlias, DeleteAlias}
/**
 This class is used to test the PartitionByTimeQuerySupport class
  */
class TestPartitionByTimeQuerySupport extends TestSuiteBuilder {

  test("Test partition selection for query") {
    val collection1Name = "test" + "_2014_11_24_17_30"
    val collection2Name="test" + "_2014_11_24_17_31"
    val collection3Name="test" + "_2014_11_24_17_33"
    val baseCollectionName="test"
    SolrCloudUtil.buildCollection(zkHost, collection1Name, null, 1, cloudClient, sc)
    SolrCloudUtil.buildCollection(zkHost, collection2Name, null, 1, cloudClient, sc)
    SolrCloudUtil.buildCollection(zkHost, collection3Name, null, 1, cloudClient, sc)
    try {
      val jsonFileLocation = "src/test/resources/test-data/events.json"
      val jsonDF = sparkSession.read.json(jsonFileLocation)
      assert(jsonDF.count == 100)

      var col1DF=jsonDF.filter(jsonDF("timestamp_tdt") >= "2014-11-24T17:30" && jsonDF("timestamp_tdt") < "2014-11-24T17:31")
      assert(col1DF.count == 32)
      col1DF=col1DF.drop(col1DF("_version_"))
      var col2DF=jsonDF.filter(jsonDF("timestamp_tdt") >= "2014-11-24T17:31" && jsonDF("timestamp_tdt") < "2014-11-24T17:32")
      assert(col2DF.count == 31)
      col2DF=col2DF.drop(col2DF("_version_"))
      var col3DF=jsonDF.filter(jsonDF("timestamp_tdt") >= "2014-11-24T17:33" && jsonDF("timestamp_tdt") < "2014-11-24T17:34")
      assert(col3DF.count == 37)
      col3DF=col3DF.drop(col3DF("_version_"))

      val solrOpts_writing1 = Map("zkhost" -> zkHost, "collection" -> collection1Name)
      val solrOpts_writing2 = Map("zkhost" -> zkHost, "collection" -> collection2Name)
      val solrOpts_writing3 = Map("zkhost" -> zkHost, "collection" -> collection3Name)

      col1DF.write.format("solr").options(solrOpts_writing1).mode(Overwrite).save()
      col2DF.write.format("solr").options(solrOpts_writing2).mode(Overwrite).save()
      col3DF.write.format("solr").options(solrOpts_writing3).mode(Overwrite).save()

      // Explicit commit to make sure all docs are visible
      val solrCloudClient = SolrSupport.getCachedCloudClient(zkHost)
      solrCloudClient.commit(collection1Name, true, true)
      solrCloudClient.commit(collection2Name, true, true)
      solrCloudClient.commit(collection3Name, true, true)

      // No query, return all the partitons
      var solrOpts = Map("zkhost" -> zkHost, "collection" -> baseCollectionName,"partition_by" -> "time","time_period" -> "1MINUTES")
      var solrDF = sparkSession.read.format("solr").options(solrOpts).load()
      assert(solrDF.count == 100)

      //query to select all partitions
      solrOpts = Map("zkhost" -> zkHost, "collection" -> baseCollectionName,"partition_by" -> "time","time_period" -> "1MINUTES","solr.params" -> "fq=timestamp_tdt:[* TO *]")
      solrDF = sparkSession.read.format("solr").options(solrOpts).load()
      assert(solrDF.count == 100)

      // querying a range
      solrOpts = Map("zkhost" -> zkHost, "collection" -> baseCollectionName,"partition_by" -> "time","time_period" -> "1MINUTES","filters" -> "timestamp_tdt:[2014-11-24T17:30:00Z TO 2014-11-24T17:32:00Z]")
      solrDF = sparkSession.read.format("solr").options(solrOpts).load()
      assert(solrDF.count == 63)

      // querying a range with multiple timestamps
      solrOpts = Map("zkhost" -> zkHost, "collection" -> baseCollectionName,"partition_by" -> "time","time_period" -> "1MINUTES","filters" -> "timestamp_tdt:[2014-11-24T17:30:00Z TO *],timestamp_tdt:[* TO 2014-11-24T17:31:00Z]")
      solrDF = sparkSession.read.format("solr").options(solrOpts).load()
      assert(solrDF.count == 34)

    } finally {
      SolrCloudUtil.deleteCollection(collection1Name, cluster)
      SolrCloudUtil.deleteCollection(collection2Name, cluster)
      SolrCloudUtil.deleteCollection(collection3Name, cluster)
    }
  }

  test("Test partition selection with gaps") {
    val rangeQuery = "timestamp:{2017-09-09T00:00:00.00Z TO *]"
    val solrQuery = new SolrQuery()
    solrQuery.addFilterQuery(rangeQuery)

    val dfParams = Map(
      PARTITION_BY -> "time",
      TIMESTAMP_FIELD_NAME -> "timestamp",
      TIME_PERIOD -> "1DAYS",
      DATETIME_PATTERN -> "yyyy_MM_dd",
      TIMEZONE_ID -> "UTC",
      "collection" -> "events"
    )
    val solrConf = new SolrConf(dfParams)

    val timePartitioningQuery = new TimePartitioningQuery(solrConf, solrQuery)
    val allPartitions = List("events_2017_09_01", "events_2017_09_03", "events_2017_09_05", "events_2017_09_07",
      "events_2017_09_10", "events_2017_09_11", "events_2017_09_13")
    val selectedPartitions = timePartitioningQuery.getCollectionsForRangeQuery(rangeQuery, allPartitions)
    assert(selectedPartitions.toSet == Set("events_2017_09_07", "events_2017_09_10", "events_2017_09_11", "events_2017_09_13"))
  }

  test("Test partition selection") {
    val rangeQuery = "timestamp:{2017-09-09T00:00:00.00Z TO *]"
    val solrQuery = new SolrQuery()
    solrQuery.addFilterQuery(rangeQuery)

    val dfParams = Map(
      PARTITION_BY -> "time",
      TIMESTAMP_FIELD_NAME -> "timestamp",
      TIME_PERIOD -> "1DAYS",
      DATETIME_PATTERN -> "yyyy_MM_dd",
      TIMEZONE_ID -> "UTC",
      "collection" -> "events"
    )
    val solrConf = new SolrConf(dfParams)

    val timePartitioningQuery = new TimePartitioningQuery(solrConf, solrQuery)
    val allPartitions = List("events_2017_09_01", "events_2017_09_03", "events_2017_09_05", "events_2017_09_07",
      "events_2017_09_09", "events_2017_09_10", "events_2017_09_11", "events_2017_09_13")
    val selectedPartitions = timePartitioningQuery.getCollectionsForRangeQuery(rangeQuery, allPartitions)
    assert(selectedPartitions.toSet == Set("events_2017_09_09", "events_2017_09_10", "events_2017_09_11", "events_2017_09_13"))
  }

  test("Test partition selection months") {
    val rangeQuery = "timestamp:{2017-09-09T00:00:00.00Z TO *]"
    val solrQuery = new SolrQuery()
    solrQuery.addFilterQuery(rangeQuery)

    val dfParams = Map(
      PARTITION_BY -> "time",
      TIMESTAMP_FIELD_NAME -> "timestamp",
      TIME_PERIOD -> "1MONTH",
      DATETIME_PATTERN -> "yyyy_MM",
      TIMEZONE_ID -> "UTC",
      "collection" -> "events"
    )
    val solrConf = new SolrConf(dfParams)

    val timePartitioningQuery = new TimePartitioningQuery(solrConf, solrQuery)
    val allPartitions = List("events_2017_10","events_2017_09","events_2017_08","events_2017_07")
    val selectedPartitions = timePartitioningQuery.getCollectionsForRangeQuery(rangeQuery, allPartitions)
    assert(selectedPartitions.toSet === Set("events_2017_10", "events_2017_09"))
  }

  test("Test range query filtering") {
    val rangeQuery = "ts:{2016-06-18T00:00:00.00Z TO *]"
    val solrQuery = new SolrQuery()
    solrQuery.addFilterQuery(rangeQuery)

    val dfParams = Map(
      PARTITION_BY -> "time",
      TIMESTAMP_FIELD_NAME -> "ts",
      TIME_PERIOD -> "1DAYS",
      DATETIME_PATTERN -> "yyyy_MM_dd",
      TIMEZONE_ID -> "UTC",
      "collection" -> "eventsim"
    )
    val solrConf = new SolrConf(dfParams)

    val timePartitioningQuery = new TimePartitioningQuery(solrConf, solrQuery)
    val filterQueries = timePartitioningQuery.filterRangeQueries(solrQuery.getFilterQueries, "ts:")
    assert(filterQueries(0) === "ts:{2016-06-18T00:00:00.00Z TO *]")
    val aps = "eventsim_2016_06_10,eventsim_2016_06_11,eventsim_2016_06_12,eventsim_2016_06_13," +
      "eventsim_2016_06_14,eventsim_2016_06_15,eventsim_2016_06_16,eventsim_2016_06_17,eventsim_2016_06_18," +
      "eventsim_2016_06_19,eventsim_2016_06_20,eventsim_2016_06_21,eventsim_2016_06_22,eventsim_2016_06_23," +
      "eventsim_2016_06_24,eventsim_2016_06_25"
    val selectedPartitions = timePartitioningQuery.getCollectionsForRangeQuery(rangeQuery, aps.split(",").toList)
    val sps = timePartitioningQuery.getCollectionsForRangeQuery(rangeQuery, selectedPartitions.mkString(",").split(",").toList)
    assert(sps.length === 8)
  }

  test("Test partition selection upper bound") {
    val rangeQuery = "timestamp:[* TO 2017-09-09T00:00:00.00Z}"
    val solrQuery = new SolrQuery()
    solrQuery.addFilterQuery(rangeQuery)

    val dfParams = Map(
      PARTITION_BY -> "time",
      TIMESTAMP_FIELD_NAME -> "timestamp",
      TIME_PERIOD -> "1DAYS",
      DATETIME_PATTERN -> "yyyy_MM_dd",
      TIMEZONE_ID -> "UTC",
      "collection" -> "events"
    )
    val solrConf = new SolrConf(dfParams)

    val timePartitioningQuery = new TimePartitioningQuery(solrConf, solrQuery)
    val allPartitions = List("events_2017_09_01", "events_2017_09_03", "events_2017_09_05", "events_2017_09_07",
      "events_2017_09_09", "events_2017_09_10", "events_2017_09_11", "events_2017_09_13")
    val selectedPartitions = timePartitioningQuery.getCollectionsForRangeQuery(rangeQuery, allPartitions)
    assert(selectedPartitions.toSet == Set("events_2017_09_01", "events_2017_09_03", "events_2017_09_05", "events_2017_09_07", "events_2017_09_09"))
  }

  test("Test partition selection lower bound and upper bound") {
    val rangeQuery1 = "timestamp:[* TO 2017-09-09T00:00:00.00Z}"
    val rangeQuery2 = "timestamp:{2017-09-07T00:00:00.00Z TO *]"
    val solrQuery = new SolrQuery()
    solrQuery.addFilterQuery(rangeQuery1)
    solrQuery.addFilterQuery(rangeQuery2)

    val dfParams = Map(
      PARTITION_BY -> "time",
      TIMESTAMP_FIELD_NAME -> "timestamp",
      TIME_PERIOD -> "1DAYS",
      DATETIME_PATTERN -> "yyyy_MM_dd",
      TIMEZONE_ID -> "UTC",
      "collection" -> "events"
    )
    val solrConf = new SolrConf(dfParams)

    val timePartitioningQuery = new TimePartitioningQuery(solrConf, solrQuery)
    val allPartitions = List("events_2017_09_01", "events_2017_09_03", "events_2017_09_05", "events_2017_09_07",
      "events_2017_09_09", "events_2017_09_10", "events_2017_09_11", "events_2017_09_13")
    val selectedPartitions = timePartitioningQuery.getCollectionsForRangeQueries(Array(rangeQuery1, rangeQuery2), allPartitions)
    assert(selectedPartitions.toSet == Set("events_2017_09_07", "events_2017_09_09"))
  }

  test("Test alias query") {
    val aliasName = "testAlias"
    val solrClient = SolrSupport.getCachedCloudClient(zkHost)
    val collection1Name = aliasName + "_2011_04_10"
    val collection2Name = aliasName + "_2011_04_11"
    SolrCloudUtil.buildCollection(zkHost, collection1Name, null, 1, cloudClient, sc)
    SolrCloudUtil.buildCollection(zkHost, collection2Name, null, 1, cloudClient, sc)
    try {
      val aliasCreateRequest = CollectionAdminRequest.createAlias(aliasName, s"$collection2Name,$collection1Name")
      aliasCreateRequest.process(solrClient)
      val dfParams = Map(
        PARTITION_BY -> "time",
        TIMESTAMP_FIELD_NAME -> "timestamp",
        TIME_PERIOD -> "1DAYS",
        DATETIME_PATTERN -> "yyyy_MM_dd",
        TIMEZONE_ID -> "UTC",
        "collection" -> aliasName,
        COLLECTION_ALIAS -> aliasName,
        SOLR_ZK_HOST_PARAM -> zkHost
      )
      val solrConf = new SolrConf(dfParams)
      val timePartitioningQuery = new TimePartitioningQuery(solrConf, new SolrQuery())
      val allPartitions = timePartitioningQuery.findAllPartitions
      val aliases = timePartitioningQuery.getPartitions(true)
      val solrRelation = new SolrRelation(dfParams, None, sparkSession)
      assert(solrRelation.collection === s"${collection1Name},${collection2Name}")
      assert(allPartitions.size == 2)
      assert(aliases.size == 2)
    } finally {
      val deleteAlias = CollectionAdminRequest.deleteAlias(aliasName)
      deleteAlias.process(solrClient)
      SolrCloudUtil.deleteCollection(collection1Name, cluster)
      SolrCloudUtil.deleteCollection(collection2Name, cluster)
    }
  }

  test("Test alias query with pre-resolved collections") {
    val aliasName = "testAlias"
    val solrClient = SolrSupport.getCachedCloudClient(zkHost)
    val collection1Name = aliasName + "_2011_04_10"
    val collection2Name = aliasName + "_2011_04_11"
    SolrCloudUtil.buildCollection(zkHost, collection1Name, null, 1, cloudClient, sc)
    SolrCloudUtil.buildCollection(zkHost, collection2Name, null, 1, cloudClient, sc)
    try {
      val aliasCreateRequest = CollectionAdminRequest.createAlias(aliasName, s"$collection1Name,$collection2Name")
      aliasCreateRequest.process(solrClient)
      val dfParams = Map(
        PARTITION_BY -> "time",
        TIMESTAMP_FIELD_NAME -> "timestamp",
        TIME_PERIOD -> "1DAYS",
        DATETIME_PATTERN -> "yyyy_MM_dd",
        TIMEZONE_ID -> "UTC",
        "collection" -> s"${collection1Name},${collection2Name}",
        COLLECTION_ALIAS -> aliasName,
        SOLR_ZK_HOST_PARAM -> zkHost
      )
      val solrConf = new SolrConf(dfParams)
      val timePartitioningQuery = new TimePartitioningQuery(solrConf, new SolrQuery())
      val aliases = timePartitioningQuery.getPartitions(true)
      assert(aliases.size == 2)
      val solrRelation = new SolrRelation(dfParams, None, sparkSession)
      assert(solrRelation.collection === s"${collection1Name},${collection2Name}")
    } finally {
      val deleteAlias = CollectionAdminRequest.deleteAlias(aliasName)
      deleteAlias.process(solrClient)
      SolrCloudUtil.deleteCollection(collection1Name, cluster)
      SolrCloudUtil.deleteCollection(collection2Name, cluster)
    }
  }

  test("Test alias query with pre-resolved collections monthly") {
    val aliasName = "testAlias"
    val solrClient = SolrSupport.getCachedCloudClient(zkHost)
    val collection1Name = aliasName + "_2011_04"
    val collection2Name = aliasName + "_2011_05"
    SolrCloudUtil.buildCollection(zkHost, collection1Name, null, 1, cloudClient, sc)
    SolrCloudUtil.buildCollection(zkHost, collection2Name, null, 1, cloudClient, sc)
    try {
      val aliasCreateRequest = CollectionAdminRequest.createAlias(aliasName, s"$collection1Name,$collection2Name")
      aliasCreateRequest.process(solrClient)
      val dfParams = Map(
        PARTITION_BY -> "time",
        TIMESTAMP_FIELD_NAME -> "timestamp",
        TIME_PERIOD -> "1MONTH",
        DATETIME_PATTERN -> "yyyy_MM",
        TIMEZONE_ID -> "UTC",
        "collection" -> s"${collection1Name},${collection2Name}",
        COLLECTION_ALIAS -> aliasName,
        SOLR_ZK_HOST_PARAM -> zkHost
      )
      val solrConf = new SolrConf(dfParams)
      val timePartitioningQuery = new TimePartitioningQuery(solrConf, new SolrQuery())
      val aliases = timePartitioningQuery.getPartitions(true)
      assert(aliases.size == 2)
      val solrRelation = new SolrRelation(dfParams, None, sparkSession)
      assert(solrRelation.collection === s"${collection1Name},${collection2Name}")
    } finally {
      val deleteAlias = CollectionAdminRequest.deleteAlias(aliasName)
      deleteAlias.process(solrClient)
      SolrCloudUtil.deleteCollection(collection1Name, cluster)
      SolrCloudUtil.deleteCollection(collection2Name, cluster)
    }
  }
}
