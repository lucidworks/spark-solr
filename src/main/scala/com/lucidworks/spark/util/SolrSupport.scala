package com.lucidworks.spark.util

import java.beans.{IntrospectionException, Introspector, PropertyDescriptor}
import java.lang.reflect.Modifier
import java.net.{SocketException, ConnectException, URL, InetAddress}
import java.util.Date
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.cache._
import com.lucidworks.spark.fusion.FusionPipelineClient
import com.lucidworks.spark.rdd.SolrRDD
import com.lucidworks.spark.{SolrReplica, SolrShard}
import com.lucidworks.spark.filter.DocFilterContext
import com.lucidworks.spark.query.{ShardSplit, StringFieldShardSplitStrategy, NumberFieldShardSplitStrategy, ShardSplitStrategy}
import org.apache.commons.httpclient.NoHttpResponseException
import org.apache.solr.client.solrj.request.UpdateRequest
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.client.solrj.{SolrServerException, SolrClient, SolrQuery}
import org.apache.solr.client.solrj.impl._
import org.apache.solr.common.{SolrDocument, SolrException, SolrInputDocument}
import org.apache.solr.common.cloud._
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, DataType}
import org.apache.spark.streaming.dstream.DStream

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
object SolrSupport extends Logging {

  def setupKerberosIfNeeded(): Unit = synchronized {
   val solrJaasAuthConfig: Option[String] = Some(System.getProperty(Krb5HttpClientConfigurer.LOGIN_CONFIG_PROP))
   if (solrJaasAuthConfig.isDefined) {
     val configurer: Option[HttpClientConfigurer] = Some(HttpClientUtil.getConfigurer)
     if (configurer.isDefined) {
       if (configurer.get.isInstanceOf[Krb5HttpClientConfigurer]) {
         HttpClientUtil.setConfigurer(new Krb5HttpClientConfigurer)
         log.info("Installed the Krb5HttpClientConfigurer for Solr security using config: " + solrJaasAuthConfig)
       }
     }
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
            fusionClient.postBatchToPipeline(batch)
            batch = List.empty[Any]
          }
        }

        if (batch.nonEmpty) {
          fusionClient.postBatchToPipeline(batch)
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
      rdd: RDD[SolrInputDocument]) = {
    //TODO: Return success or false by boolean ?
    rdd.foreachPartition(solrInputDocumentIterator => {
      val solrClient = getCachedCloudClient(zkHost)
      val batch = new ArrayBuffer[SolrInputDocument]()
      val indexedAt: Date = new Date()
      var numDocs = 0
      while (solrInputDocumentIterator.hasNext) {
        val doc = solrInputDocumentIterator.next()
        doc.setField("_indexed_at_tdt", indexedAt)
        batch += doc
        if (batch.length >= batchSize) {
          numDocs += batch.length
          sendBatchToSolr(solrClient, collection, batch)
          batch.clear
        }
      }
      if (batch.nonEmpty) {
        numDocs += batch.length
        sendBatchToSolr(solrClient, collection, batch)
        batch.clear
      }
    })
  }

  def sendBatchToSolr(solrClient: SolrClient, collection: String, batch: Iterable[SolrInputDocument]): Unit = {
    val req = new UpdateRequest()
    req.setParam("collection", collection)

    if (log.isDebugEnabled) {
      log.debug("Sending batch of " + batch.size + " to collection " + collection)
    }

    req.add(asJavaCollection(batch))

    try {
      solrClient.request(req)
    } catch {
      case e: Exception =>
        if (shouldRetry(e)) {
          log.error("Send batch to collection " + collection + " failed due to " + e + " ; will retry ...")
          try {
            Thread.sleep(2000)
          } catch {
            case ie: InterruptedException => Thread.interrupted()
          }

          try {
            solrClient.request(req)
          } catch {
            case ex: Exception =>
              log.error("Send batch to collection " + collection + " failed due to: " + e, e)
              ex match {
                case re: RuntimeException => throw re
                case e: Exception => throw new RuntimeException(e)
              }
          }
        } else {
          log.error("Send batch to collection " + collection + " failed due to: " + e, e)
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
      for (f <- publicFields) {
        breakable {
          // only non-static public
          if (Modifier.isStatic(f.getModifiers) || !Modifier.isPublic(f.getModifiers))
            break()
          else {
            var value: Option[Object] = None
            try {
              value = Some(f.get(obj))
            } catch {
              case e: IllegalAccessException => log.error("Exception during reflection ", e)
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
      case e: IntrospectionException => log.warn("Can't get BeanInfo for class: " + objClass)
    }

    if (props.isDefined) {
      for (pd <- props.get) {
        val propName  = pd.getName
        breakable {
          if ("class".equals(propName) || fields.contains(propName)) break()
          else {
            val readMethod = pd.getReadMethod
            readMethod.setAccessible(true);
            if (readMethod != null) {
              var value: Option[Object] = None
              try {
                value = Some(readMethod.invoke(obj))
              } catch {
                case e: Exception => log.warn("failed to invoke read method for property '" + pd.getName + "' on " +
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
    if (log.isDebugEnabled)
      log.debug("failed to map class '" + clazz + "' to a known dynamic type")
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

          if (log.isDebugEnabled) log.debug("Partition " + partitionId + " took " + TimeUnit.MILLISECONDS.convert(durationNano, TimeUnit.NANOSECONDS) + "ms to process " + numDocs + " docs")
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

    if (clusterState.hasCollection(collection)) {
      collections = Array(collection)
    }
    else {
      val aliases: Aliases = zkStateReader.getAliases
      val aliasedCollections: String = aliases.getCollectionAlias(collection)
      if (aliasedCollections == null) {
        throw new IllegalArgumentException("Collection " + collection + " not found!")
      }
      collections = aliasedCollections.split(",")
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
                replicas += new SolrReplica(0, replicaCoreProps.getCoreName, replicaCoreProps.getCoreUrl, replicaCoreProps.getNodeName, addresses)
              } catch {
                case e : Exception => log.warn("Error resolving ip address " + replicaCoreProps.getNodeName + " . Exception " + e)
                  replicas += new SolrReplica(0, replicaCoreProps.getCoreName, replicaCoreProps.getCoreUrl, replicaCoreProps.getNodeName, Array.empty[InetAddress])
              }

            }

          }
        }
        val numReplicas: Int = replicas.size
        if (numReplicas == 0) {
          throw new IllegalStateException("Shard " + slice.getName + " in collection " + coll + " does not have any active replicas!")
        }
        shards += new SolrShard(slice.getName, replicas.toList)
      }
    }
    shards.toList
  }

  def splitShards(
      query: SolrQuery,
      solrShard: SolrShard,
      splitFieldName: String,
      splitsPerShard: Int): List[ShardSplit[_]] = {

    var fieldDataType: Option[DataType] = None
    if ("_version_".equals(splitFieldName)) {
      fieldDataType = Some(DataTypes.LongType)
    } else {
      // Get the field type of split field
      val fieldMetaMap = SolrQuerySupport.getFieldTypes(Set(splitFieldName), SolrRDD.randomReplicaLocation(solrShard))
      val solrFieldMeta = fieldMetaMap.get(splitFieldName)
      if (solrFieldMeta.isDefined) {
        val fieldTypeClass  = solrFieldMeta.get.fieldTypeClass
        if (fieldTypeClass.isDefined) {
          fieldDataType = SolrQuerySupport.SOLR_DATA_TYPES.get(fieldTypeClass.get)
        } else
          fieldDataType = Some(DataTypes.StringType)
      } else {
        log.warn("No field metadata found for " + splitFieldName + ", assuming it is a String!")
        fieldDataType = Some(DataTypes.StringType)
      }
    }
    if (fieldDataType.isEmpty) {
      throw new IllegalArgumentException("Cannot determine DataType for split field " + splitFieldName)
    }

    getSplits(fieldDataType.get, splitFieldName, splitsPerShard, query, solrShard)
  }

  def getSplits(fd: DataType, sF: String, sPS: Int, query: SolrQuery, shard: SolrShard): List[ShardSplit[_]]= {
    var splitStrategy: Option[ShardSplitStrategy] = None

    if (fd.equals(DataTypes.LongType) || fd.equals(DataTypes.IntegerType)) {
      splitStrategy = Some(new NumberFieldShardSplitStrategy)
    } else if (fd.equals(DataTypes.StringType)) {
      splitStrategy = Some(new StringFieldShardSplitStrategy)
    } else {
      throw new IllegalArgumentException("Can only split shards on fields of type: long, int or String!")
    }

    if (splitStrategy.isDefined) {
      splitStrategy.get.getSplits(SolrRDD.randomReplicaLocation(shard), query, sF, sPS).toList
    } else {
      throw new IllegalArgumentException("No split strategy found for DataType '" + fd + "'")
    }
  }

}
