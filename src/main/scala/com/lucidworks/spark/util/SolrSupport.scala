package com.lucidworks.spark.util

import java.beans.{IntrospectionException, Introspector, PropertyDescriptor}
import java.lang.reflect.Modifier
import java.net.{ConnectException, InetAddress, SocketException, URL}
import java.util.Date
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.cache._
import com.lucidworks.spark.fusion.FusionPipelineClient
import com.lucidworks.spark.rdd.SolrRDD
import com.lucidworks.spark.{SolrReplica, SolrShard}
import com.lucidworks.spark.filter.DocFilterContext
import com.lucidworks.spark.query._
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.httpclient.NoHttpResponseException
import org.apache.solr.client.solrj.request.UpdateRequest
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.client.solrj.{SolrClient, SolrQuery, SolrServerException}
import org.apache.solr.client.solrj.impl._
import org.apache.solr.common.{SolrDocument, SolrException, SolrInputDocument}
import org.apache.solr.common.cloud._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.{OperationTimeoutException, SessionExpiredException}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.JavaConversions._
import util.control.Breaks._

object CacheSolrClient {
  private val loader = new CacheLoader[String, CloudSolrClient]() {
    def load(zkHost: String): CloudSolrClient = {
      SolrSupport.getNewSolrCloudClient(zkHost)
    }
  }

  private val listener = new RemovalListener[String, CloudSolrClient]() {
    def onRemoval(rn: RemovalNotification[String, CloudSolrClient]): Unit = {
      if (rn != null && rn.getValue != null) {
        rn.getValue.close()
      }
    }
  }

  val cache: LoadingCache[String, CloudSolrClient] = CacheBuilder
    .newBuilder()
    .removalListener(listener)
    .build(loader)
}

/**
 * TODO: Use Solr schema API to index field names
 */
object SolrSupport extends LazyLogging {

  def setupKerberosIfNeeded(): Unit = synchronized {
    val loginProp = System.getProperty(Krb5HttpClientConfigurer.LOGIN_CONFIG_PROP)
    if (loginProp != null && !loginProp.isEmpty) {
      HttpClientUtil.addConfigurer(new Krb5HttpClientConfigurer)
      logger.debug(s"Installed the Krb5HttpClientConfigurer for Solr security using config: $loginProp")
    }
  }

  def getHttpSolrClient(shardUrl: String): HttpSolrClient = {
    setupKerberosIfNeeded()
    new HttpSolrClient(shardUrl)
  }

  // This method should not be used directly. The method [[SolrSupport.getCachedCloudClient]] should be used instead
  private def getSolrCloudClient(zkHost: String): CloudSolrClient =  {
    setupKerberosIfNeeded()
    val solrClient = new CloudSolrClient(zkHost)
    solrClient.connect()
    solrClient
  }

  // Use this only if you want a new SolrCloudClient instance. This new instance should be closed by the methods downstream
  def getNewSolrCloudClient(zkHost: String): CloudSolrClient = {
    getSolrCloudClient(zkHost)
  }

  def getCachedCloudClient(zkHost: String): CloudSolrClient = {
    CacheSolrClient.cache.get(zkHost)
  }

  def getSolrBaseUrl(zkHost: String) = {
    val solrClient = getCachedCloudClient(zkHost)
    val liveNodes = solrClient.getZkStateReader.getClusterState.getLiveNodes
    if (liveNodes.isEmpty) {
      throw new RuntimeException("No live nodes found for cluster: " + zkHost)
    }
    var solrBaseUrl = solrClient.getZkStateReader.getBaseUrlForNodeName(liveNodes.iterator().next())
    if (!solrBaseUrl.endsWith("?")) solrBaseUrl += "/"
    solrBaseUrl
  }

  def indexDStreamOfDocs(
      zkHost: String,
      collection: String,
      batchSize: Int,
      docs: DStream[SolrInputDocument]): Unit =
    docs.foreachRDD(rdd => indexDocs(zkHost, collection, batchSize, rdd))

  def sendDStreamOfDocsToFusion(
      fusionUrl: String,
      fusionCredentials: String,
      docs: DStream[_],
      batchSize: Int): Unit = {

    val urls = fusionUrl.split(",").distinct
    val url = new URL(urls(0))
    val pipelinePath = url.getPath

    docs.foreachRDD(rdd => {
      rdd.foreachPartition(docIter => {
        val creds = if (fusionCredentials != null) fusionCredentials.split(":") else null
        if (creds.size != 3) throw new Exception("Not valid format for Fusion credentials. Except 3 objects separated by :")
        val fusionClient = if (creds != null) new FusionPipelineClient(fusionUrl, creds(0), creds(1), creds(2)) else new FusionPipelineClient(fusionUrl)
        var batch = List.empty[Any]
        val indexedAt = new Date()

        while(docIter.hasNext) {
          val inputDoc = docIter.next()
          batch.add(inputDoc)
          if (batch.size >= batchSize) {
            fusionClient.postBatchToPipeline(pipelinePath, batch)
            batch = List.empty[Any]
          }
        }

        if (batch.nonEmpty) {
          fusionClient.postBatchToPipeline(pipelinePath, batch)
          batch = List.empty[Any]
        }

        fusionClient.shutdown()
      })
    })
  }

  def indexDocs(
      zkHost: String,
      collection: String,
      batchSize: Int,
      rdd: RDD[SolrInputDocument]): Unit = indexDocs(zkHost, collection, batchSize, rdd, None)

  def indexDocs(
      zkHost: String,
      collection: String,
      batchSize: Int,
      rdd: RDD[SolrInputDocument],
      commitWithin: Option[Int]): Unit = {
    //TODO: Return success or false by boolean ?
    rdd.foreachPartition(solrInputDocumentIterator => {
      val solrClient = getCachedCloudClient(zkHost)
      val batch = new ArrayBuffer[SolrInputDocument]()
      var numDocs = 0
      while (solrInputDocumentIterator.hasNext) {
        val doc = solrInputDocumentIterator.next()
        batch += doc
        if (batch.length >= batchSize) {
          numDocs += batch.length
          sendBatchToSolrWithRetry(zkHost, solrClient, collection, batch, commitWithin)
          batch.clear
        }
      }
      if (batch.nonEmpty) {
        numDocs += batch.length
        sendBatchToSolrWithRetry(zkHost, solrClient, collection, batch, commitWithin)
        batch.clear
      }
    })
  }

  def sendBatchToSolrWithRetry(
      zkHost: String,
      solrClient: SolrClient,
      collection: String,
      batch: Iterable[SolrInputDocument],
      commitWithin: Option[Int]): Unit = {
    try {
      sendBatchToSolr(solrClient, collection, batch, commitWithin)
    } catch {
      // Reset the cache when SessionExpiredException is thrown. Plus side is that the job won't fail
      case e @ (_: SessionExpiredException | _:OperationTimeoutException) =>
        logger.info("Got an exception with message '" + e.getMessage +  "'.  Resetting the cached solrClient")
        CacheSolrClient.cache.invalidate(zkHost)
        val newClient = SolrSupport.getCachedCloudClient(zkHost)
        sendBatchToSolr(newClient, collection, batch, commitWithin)
    }
  }

  def sendBatchToSolr(solrClient: SolrClient, collection: String, batch: Iterable[SolrInputDocument]): Unit =
    sendBatchToSolr(solrClient, collection, batch, None)

  def sendBatchToSolr(
      solrClient: SolrClient,
      collection: String,
      batch: Iterable[SolrInputDocument],
      commitWithin: Option[Int]): Unit = {
    val req = new UpdateRequest()
    req.setParam("collection", collection)

    val initialTime = System.currentTimeMillis()

    if (commitWithin.isDefined)
      req.setCommitWithin(commitWithin.get)

    logger.info("Sending batch of " + batch.size + " to collection " + collection)

    req.add(asJavaCollection(batch))

    try {
      solrClient.request(req)
      val timeTaken = (System.currentTimeMillis() - initialTime)/1000.0
      logger.info("Took '" + timeTaken + "' secs to index '" + batch.size + "' documents")
    } catch {
      case e: Exception =>
        if (shouldRetry(e)) {
          logger.error("Send batch to collection " + collection + " failed due to " + e + " ; will retry ...")
          try {
            Thread.sleep(2000)
          } catch {
            case ie: InterruptedException => Thread.interrupted()
          }

          try {
            solrClient.request(req)
          } catch {
            case ex: Exception =>
              logger.error("Send batch to collection " + collection + " failed due to: " + e, e)
              ex match {
                case re: RuntimeException => throw re
                case e: Exception => throw new RuntimeException(e)
              }
          }
        } else {
          logger.error("Send batch to collection " + collection + " failed due to: " + e, e)
          e match {
            case re: RuntimeException => throw re
            case ex: Exception => throw new RuntimeException(ex)
          }
        }

    }

  }

  def shouldRetry(exc: Exception): Boolean = {
    val rootCause = SolrException.getRootCause(exc)
    rootCause match {
      case e: ConnectException => true
      case e: NoHttpResponseException => true
      case e: SocketException => true
      case _ => false
    }
  }

  /**
   * Uses reflection to map bean public fields and getters to dynamic fields in Solr.
   */
  def autoMapToSolrInputDoc(
      docId: String,
      obj: Object,
      dynamicFieldOverrides: Map[String, String]): SolrInputDocument =
    autoMapToSolrInputDoc("id", docId, obj, dynamicFieldOverrides)

  def autoMapToSolrInputDoc(
      idFieldName: String,
      docId: String,
      obj: Object,
      dynamicFieldOverrides: Map[String, String]): SolrInputDocument = {
    val doc = new SolrInputDocument()
    doc.setField(idFieldName, docId)
    if (obj == null) return doc

    val objClass = obj.getClass
    val fields = new mutable.HashSet[String]()
    val publicFields = objClass.getFields

    if (publicFields != null) {
      breakable {
        for (f <- publicFields) {
          // only non-static public
          if (Modifier.isStatic(f.getModifiers) || !Modifier.isPublic(f.getModifiers))
            break()
          else {
            var value: Option[Object] = None
            try {
              value = Some(f.get(obj))
            } catch {
              case e: IllegalAccessException => logger.error("Exception during reflection ", e)
            }

            if (value.isDefined) {
              val fieldName = f.getName
              fields.add(fieldName)
              val fieldOverride = if (dynamicFieldOverrides != null) dynamicFieldOverrides.get(fieldName) else null
              if (f.getType != null)
                addField(doc, fieldName, value, f.getType, fieldOverride)
            }
          }
        }
      }

    }

    var props: Option[Array[PropertyDescriptor]] = None
    try {
      val info = Introspector.getBeanInfo(objClass)
      props = Some(info.getPropertyDescriptors)
    } catch {
      case e: IntrospectionException => logger.warn("Can't get BeanInfo for class: " + objClass)
    }

    if (props.isDefined) {
      for (pd <- props.get) {
        val propName  = pd.getName
        breakable {
          if ("class".equals(propName) || fields.contains(propName)) break()
          else {
            val readMethod = pd.getReadMethod
            readMethod.setAccessible(true)
            if (readMethod != null) {
              var value: Option[Object] = None
              try {
                value = Some(readMethod.invoke(obj))
              } catch {
                case e: Exception => logger.warn("failed to invoke read method for property '" + pd.getName + "' on " +
                  "object of type '" + objClass.getName + "' due to: " + e)
              }

              if (value.isDefined) {
                fields.add(propName)
                val propOverride  = if (dynamicFieldOverrides != null) dynamicFieldOverrides.get(propName) else None
                if (pd.getPropertyType != null)
                  addField(doc, propName, value.get, pd.getPropertyType, propOverride)
              }
            }
          }
        }
      }
    }
    doc
  }

  def addField(
      doc: SolrInputDocument,
      fieldName: String,
      value: Object,
      classType: Class[_],
      dynamicFieldSuffix: Option[String]): Unit = {
    if (classType.isArray) return // TODO: Array types not supported yet ...

    if (dynamicFieldSuffix.isDefined) {
      doc.addField(fieldName + dynamicFieldSuffix.get, value)
    } else {
      var suffix = getDefaultDynamicFieldMapping(classType)
      if (suffix.isDefined) {
        // treat strings with multiple terms as text only if using the default!
        if ("_s".equals(suffix.get)) {
          if (value != null) {
            value match {
              case v1: String =>
                if (v1.indexOf(" ") != -1) suffix = Some("_t")
                val key = fieldName + suffix.get
                doc.addField(key, value)
              case v1: Any =>
                val v = String.valueOf(v1)
                if (v.indexOf(" ") != -1) suffix = Some("_t")
                val key = fieldName + suffix.get
                doc.addField(key, value)
            }
          }
        } else {
          val key = fieldName + suffix.get
          doc.addField(key, value)
        }
      }

    }
  }

  def getDefaultDynamicFieldMapping(clazz: Class[_]): Option[String] = {
    if (classOf[String] == clazz) return Some("_s")
    else if ((classOf[java.lang.Long] == clazz) || (classOf[Long] == clazz)) return Some("_l")
    else if ((classOf[java.lang.Integer] == clazz) || (classOf[Int] == clazz)) return Some("_i")
    else if ((classOf[java.lang.Double] == clazz) || (classOf[Double] == clazz)) return Some("_d")
    else if ((classOf[java.lang.Float] == clazz) || (classOf[Float] == clazz)) return Some("_f")
    else if ((classOf[java.lang.Boolean] == clazz) || (classOf[Boolean] == clazz)) return Some("_b")
    else if (classOf[Date] == clazz) return Some("_tdt")
    logger.debug("failed to map class '" + clazz + "' to a known dynamic type")
    None
  }

  def filterDocuments(
      filterContext: DocFilterContext,
      zkHost: String,
      collection: String,
      docs: DStream[SolrInputDocument]): DStream[SolrInputDocument] = {
    val partitionIndex = new AtomicInteger(0)
    val idFieldName = filterContext.getDocIdFieldName

    docs.mapPartitions(solrInputDocumentIterator => {
      val startNano: Long = System.nanoTime()
      val partitionId: Int = partitionIndex.incrementAndGet()

      val partitionFq: String = "docfilterid_i:" + partitionId
      // TODO: Can this be used concurrently? probably better to have each partition check it out from a pool
      val solr = EmbeddedSolrServerFactory.singleton.getEmbeddedSolrServer(zkHost, collection)

      // index all docs in this partition, then match queries
      var numDocs: Int = 0
      val inputDocs: mutable.Map[String, SolrInputDocument] = new mutable.HashMap[String, SolrInputDocument]
      while (solrInputDocumentIterator.hasNext) {
        numDocs += 1
        val doc: SolrInputDocument = solrInputDocumentIterator.next()
        doc.setField("docfilterid_i", partitionId)
        solr.add(doc)
        inputDocs.put(doc.getFieldValue(idFieldName).asInstanceOf[String], doc)
      }
      solr.commit

      for (q: SolrQuery <- filterContext.getQueries) {
        val query = q.getCopy
        query.setFields(idFieldName)
        query.setRows(inputDocs.size)
        query.addFilterQuery(partitionFq)

        var queryResponse: Option[QueryResponse] = None
        try {
          queryResponse = Some(solr.query(query))
        }
        catch {
          case e: SolrServerException =>
            throw new RuntimeException(e)
        }

        if (queryResponse.isDefined) {
          for (doc: SolrDocument  <- queryResponse.get.getResults) {
            val docId: String = doc.getFirstValue(idFieldName).asInstanceOf[String]
            val inputDoc = inputDocs.get(docId)
            if (inputDoc.isDefined) filterContext.onMatch(q, inputDoc.get)
          }

          solr.deleteByQuery(partitionFq, 100)
          val durationNano: Long = System.nanoTime - startNano

          logger.debug("Partition " + partitionId + " took " + TimeUnit.MILLISECONDS.convert(durationNano, TimeUnit.NANOSECONDS) + "ms to process " + numDocs + " docs")
          for (inputDoc <- inputDocs.values) {
            inputDoc.removeField("docfilterid_i")
          }
        }
      }

      inputDocs.valuesIterator
    })
  }

  def buildShardList(zkHost: String, collection: String): List[SolrShard] = {
    val solrClient = getCachedCloudClient(zkHost)
    val zkStateReader: ZkStateReader = solrClient.getZkStateReader
    val clusterState: ClusterState = zkStateReader.getClusterState
    var collections = Array.empty[String]
    for(col <- collection.split(",")) {
      if (clusterState.hasCollection(col)) {
        collections = collections :+ col
      }
    else {
      val aliases: Aliases = zkStateReader.getAliases
      val aliasedCollections: String = aliases.getCollectionAlias(col)
      if (aliasedCollections == null) {
        throw new IllegalArgumentException("Collection " + col + " not found!")
      }
      collections = aliasedCollections.split(",")
      }
    }
    val liveNodes  = clusterState.getLiveNodes

    val shards = new ListBuffer[SolrShard]()
    for (coll <- collections) {
      for (slice: Slice <- clusterState.getSlices(coll)) {
        var replicas  =  new ListBuffer[SolrReplica]()
        for (r: Replica <- slice.getReplicas) {
          if (r.getState == Replica.State.ACTIVE) {
            val replicaCoreProps: ZkCoreNodeProps = new ZkCoreNodeProps(r)
            if (liveNodes.contains(replicaCoreProps.getNodeName)) {
              try {
                val addresses = InetAddress.getAllByName(new URL(replicaCoreProps.getBaseUrl).getHost)
                replicas += SolrReplica(0, replicaCoreProps.getCoreName, replicaCoreProps.getCoreUrl, replicaCoreProps.getNodeName, addresses)
              } catch {
                case e : Exception => logger.warn("Error resolving ip address " + replicaCoreProps.getNodeName + " . Exception " + e)
                  replicas += SolrReplica(0, replicaCoreProps.getCoreName, replicaCoreProps.getCoreUrl, replicaCoreProps.getNodeName, Array.empty[InetAddress])
              }

            }

          }
        }
        val numReplicas: Int = replicas.size
        if (numReplicas == 0) {
          throw new IllegalStateException("Shard " + slice.getName + " in collection " + coll + " does not have any active replicas!")
        }
        shards += SolrShard(slice.getName, replicas.toList)
      }
    }
    shards.toList
  }

  def getShardSplits(
      query: SolrQuery,
      solrShard: SolrShard,
      splitFieldName: String,
      splitsPerShard: Int): List[WorkerShardSplit] = {
    query.set("partitionKeys", splitFieldName)
    val splits = ListBuffer.empty[WorkerShardSplit]
    val replicas = solrShard.replicas
    val sortedReplicas = replicas.sortBy(r => r.replicaName)
    val numReplicas = replicas.size

    for (i <- 0 until splitsPerShard) {
      val fq = s"{!hash workers=$splitsPerShard worker=$i}"
      // with hash, we can hit all replicas in the shard in parallel
      val replica =
        if (numReplicas >1)
          if (i < numReplicas) sortedReplicas.get(i) else sortedReplicas.get(i % numReplicas)
        else
          sortedReplicas.get(0)
      val splitQuery = query.getCopy
      splitQuery.addFilterQuery(fq)
      splits += WorkerShardSplit(splitQuery, replica)
    }
    splits.toList
  }


  // Workaround for SOLR-10490. TODO: Remove once fixed
  def getExportHandlerSplits(
      query: SolrQuery,
      solrShard: SolrShard,
      splitFieldName: String,
      splitsPerShard: Int): List[ExportHandlerSplit] = {
    val splits = ListBuffer.empty[ExportHandlerSplit]
    val replicas = solrShard.replicas
    val sortedReplicas = replicas.sortBy(r => r.replicaName)
    val numReplicas = replicas.size

    for (i <- 0 until splitsPerShard) {
      val replica =
        if (numReplicas >1)
          if (i < numReplicas) sortedReplicas.get(i) else sortedReplicas.get(i % numReplicas)
        else
          sortedReplicas.get(0)
      splits += ExportHandlerSplit(query, replica, splitsPerShard, i)
    }
    splits.toList
  }

  case class WorkerShardSplit(query: SolrQuery, replica: SolrReplica)
  case class ExportHandlerSplit(query: SolrQuery, replica: SolrReplica, numWorkers: Int, workerId: Int)
}
