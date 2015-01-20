package com.lucidworks.spark

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.SolrServer
import org.apache.solr.client.solrj.SolrServerException
import org.apache.solr.client.solrj.impl.CloudSolrServer
import org.apache.solr.client.solrj.impl.HttpSolrServer
import org.apache.solr.client.solrj.SolrServerException
import org.apache.solr.common.SolrDocument
import org.apache.solr.common.SolrDocumentList
import org.apache.solr.common.SolrException
import org.apache.solr.common.cloud.ClusterState
import org.apache.solr.common.cloud.Replica
import org.apache.solr.common.cloud.Slice
import org.apache.solr.common.cloud.ZkCoreNodeProps
import org.apache.solr.common.cloud.ZkStateReader
import org.apache.solr.client.solrj.response.QueryResponse

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.{HashMap,SynchronizedMap}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._
import scala.collection.JavaConversions._

import java.io.IOException
import java.net.ConnectException
import java.net.SocketException
import java.util.Random

class SolrScalaRDD(val zkHost:String, val collection:String) extends Serializable {

  val log = LoggerFactory.getLogger(getClass)
  private val defaultPageSize = 50
  private var currentPageSize = 0

  /**
   * Iterates over the entire results set of a query (all hits).
   */
  //private class PagedResultsIterator implements Iterator<SolrDocument>, Iterable<SolrDocument> {
  class PagedResultsIterator(solrServer: SolrServer, solrQuery: SolrQuery, var cursorMark: String) extends Iterator[SolrDocument] {//with Iterable[SolrDocument] {

  	var currentPage: SolrDocumentList = null
  	var iterPos: Int = 0
  	var totalDocs: Long = 0
  	var numDocs: Long = 0
  	val closeAfterIterating = !(solrServer.isInstanceOf[CloudSolrServer])
  	if (solrQuery.getRows() == null)
      solrQuery.setRows(defaultPageSize) // default page size

    def this(solrServer:SolrServer, solrQuery:SolrQuery) {
      this(solrServer, solrQuery, null)
    }

    def hasNext(): Boolean = {
      if (currentPage == null || iterPos == currentPageSize) {
        try {
          currentPage = fetchNextPage()
          currentPageSize = currentPage.size()
          iterPos = 0
        } catch {
          case sse: SolrServerException => {
          	throw new RuntimeException(sse)
          }          
        }
      }

      val hasNext: Boolean = (iterPos < currentPageSize)
      if (!hasNext && closeAfterIterating) {
        try {
          solrServer.shutdown()
        } catch {
          case e: Exception => {}
        }
      }
      return hasNext
    }

    protected def getStartForNextPage(): Int = {
      val currentStart: Int = solrQuery.getStart()
      if(currentStart != 0) 
        currentStart + solrQuery.getRows()
      else
        0
    }

    @throws(classOf[SolrServerException]) protected def fetchNextPage(): SolrDocumentList = {
      val start: Int = if (cursorMark != null) 0 else getStartForNextPage()
      val resp: QueryResponse = querySolr(solrServer, solrQuery, start, cursorMark)
      if (cursorMark != null)
        cursorMark = resp.getNextCursorMark()

      iterPos = 0
      val docs: SolrDocumentList = resp.getResults()
      totalDocs = docs.getNumFound()
      docs
    }

    def next(): SolrDocument = {
      if (currentPage == null || iterPos >= currentPageSize)
        throw new NoSuchElementException("No more docs available!")

      numDocs += 1
      val ret = currentPage.get(iterPos)
      iterPos += 1
      ret
    }

    def remove() = {
      throw new UnsupportedOperationException("remove is not supported")
    }

    def iterator(): Iterator[SolrDocument] = {
      this
    }

  }

  // can't serialize CloudSolrServers so we cache them in a static context to reuse by the zkHost
  //private static final Map<String,CloudSolrServer> cachedServers = new HashMap<String,CloudSolrServer>()
  var cachedServers = new HashMap[String, CloudSolrServer] with SynchronizedMap[String, CloudSolrServer]

  protected def getSolrServer(zkHost: String): CloudSolrServer = {
    var cloudSolrServer: CloudSolrServer = cachedServers.getOrElse(zkHost, new CloudSolrServer(zkHost))
    if (cloudSolrServer == null) {
      cloudSolrServer = new CloudSolrServer(zkHost)
      cloudSolrServer.connect()
      cachedServers.put(zkHost, cloudSolrServer)
    }
    cloudSolrServer.connect() // zkStateReader is null without this
    cloudSolrServer
  }

  //public JavaRDD<SolrDocument> query(JavaSparkContext jsc, final SolrQuery query, boolean useDeepPagingCursor) throws SolrServerException {
  @throws(classOf[SolrServerException]) def query(sc: SparkContext, query: SolrQuery, useDeepPagingCursor: Boolean): RDD[SolrDocument] = {
    if (useDeepPagingCursor)
      queryDeep(sc, query)

    val cloudSolrServer: CloudSolrServer = getSolrServer(zkHost)
    var results = new ArrayBuffer[SolrDocument]
    val resultsIter = new PagedResultsIterator(cloudSolrServer, query)
    while (resultsIter.hasNext()) results.append(resultsIter.next)
    return sc.parallelize(results)
  }

  @throws(classOf[SolrServerException]) def queryShards(sc: SparkContext, query:SolrQuery): RDD[SolrDocument] = {
    // first get a list of replicas to query for this collection
    val cloudSolrServer = getSolrServer(zkHost)
    val shards: ArrayBuffer[String] = buildShardList(cloudSolrServer)
    //val shards: ArrayBuffer[String] = buildShardList(getSolrServer(zkHost))

    // we'll be directing queries to each shard, so we don't want distributed
    query.set("distrib", false);
    query.set("collection", collection);
    query.setStart(0);
    if (query.getRows() == null)
      query.setRows(defaultPageSize) // default page size

    // parallelize the requests to the shards
    val docs = sc.parallelize(shards).flatMap(shardUrl => {
      new PagedResultsIterator(new HttpSolrServer(shardUrl), query, "*")
    })

    docs
  }

  protected def buildShardList(cloudSolrServer: CloudSolrServer): ArrayBuffer[String] = {
    val zkStateReader: ZkStateReader = cloudSolrServer.getZkStateReader()
    val clusterState: ClusterState = zkStateReader.getClusterState()
    val liveNodes = asScalaSet(clusterState.getLiveNodes())
    val slices: Iterable[Slice] = clusterState.getSlices(collection)
    if (slices == null)
      throw new IllegalArgumentException("Collection "+collection+" not found!")

    val random = new Random()
    var shards = new ArrayBuffer[String]
    for (slice <- slices) {
      val replicas = new ArrayBuffer[String]
      for (r:Replica <- slice.getReplicas()) {
        val replicaCoreProps: ZkCoreNodeProps = new ZkCoreNodeProps(r)
        if (liveNodes.contains(replicaCoreProps.getNodeName()))
          replicas.append(replicaCoreProps.getCoreUrl())
      }
      val numReplicas: Int = replicas.size
      if (numReplicas == 0)
        throw new IllegalStateException("Shard "+slice.getName()+" does not have any active replicas!")

      val replicaUrl: String = if(numReplicas == 1) replicas.get(0) else replicas.get(random.nextInt(replicas.size()))
      shards.append(replicaUrl)
    }

    return shards
  }

  //public JavaRDD<SolrDocument> queryDeep(JavaSparkContext jsc, final SolrQuery query) throws SolrServerException {
  @throws(classOf[SolrServerException]) def queryDeep(sc: SparkContext, query: SolrQuery): RDD[SolrDocument] = {
    var cursors = new ArrayBuffer[String]

    // stash this for later use when we're actually querying for data
    val fields: String = query.getFields()

    query.set("collection", collection)
    query.setStart(0)
    query.setFields("id")

    if (query.getRows() == null)
      query.setRows(defaultPageSize) // default page size

    val cloudSolrServer: CloudSolrServer = getSolrServer(zkHost)
    var nextCursorMark: String = "*"
    while (true) {
      cursors.append(nextCursorMark)
      query.set("cursorMark", nextCursorMark)
      val resp: QueryResponse = cloudSolrServer.query(query)
      nextCursorMark = resp.getNextCursorMark()
      if (nextCursorMark == null || resp.getResults().isEmpty())
        break
    }

    val cursorJavaRDD: RDD[String] = sc.parallelize(cursors)

    query.setFields(fields)

    // now we need to execute all the cursors in parallelß
    val docs: RDD[SolrDocument] = cursorJavaRDD.flatMap( cursorMark => {
      querySolr(getSolrServer(zkHost), query, 0, cursorMark).getResults()
    })
    docs
  }
  

  @throws(classOf[SolrServerException]) protected def querySolr(solrServer: SolrServer, solrQuery: SolrQuery, startIndex: Int, cursorMark: String): QueryResponse = {
    var resp: QueryResponse = null

    try {
      if (cursorMark != null) {
        solrQuery.setStart(0)
        solrQuery.set("cursorMark", cursorMark)
      } else {
        solrQuery.setStart(startIndex)
      }
      resp = solrServer.query(solrQuery) // Check the arg type
    } catch {
      case exc: SolrServerException => {
        // re-try once in the event of a communications error with the server
        val rootCause: Throwable = SolrException.getRootCause(exc)
        val wasCommError:Boolean =
              (rootCause.isInstanceOf[ConnectException] ||
               rootCause.isInstanceOf[IOException] ||
               rootCause.isInstanceOf[SocketException])
        if (wasCommError) {
          try {
            Thread.sleep(2000L)
          } catch {
            case ie: InterruptedException => {
              Thread.interrupted()
            }
          }

          resp = solrServer.query(solrQuery)
        } else {
          throw exc
        }
      }
    }
    resp
  }

}