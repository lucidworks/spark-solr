package com.lucidworks.spark

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream

import org.apache.solr.common.SolrInputDocument
import org.apache.solr.common.SolrException
import org.apache.solr.client.solrj.SolrServer
import org.apache.solr.client.solrj.impl.CloudSolrServer
import org.apache.solr.client.solrj.request.UpdateRequest

import scala.collection.mutable.{HashMap,SynchronizedMap}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions.asJavaCollection

import java.net.ConnectException
import java.net.SocketException
import java.util.Collection
import org.apache.commons.httpclient.ConnectTimeoutException
import org.apache.commons.httpclient.NoHttpResponseException

object SolrSupportScala {

  val log = LoggerFactory.getLogger(getClass)
  var solrServers = new HashMap[String, CloudSolrServer] with SynchronizedMap[String, CloudSolrServer]
  
  def getSolrServer(key:String):CloudSolrServer = {
    var solr = solrServers.getOrElse(key, new CloudSolrServer(key))
    solrServers.put(key, solr)
    solr
  }

  
  def indexDStreamOfDocs(zkHost:String, collection:String, batchSize:Integer, docs:DStream[SolrInputDocument]) = {
  	docs.foreachRDD(rdd => {
  	  rdd.foreachPartition(solrInputDocumentIterator => {
  	  	val solrServer = getSolrServer(zkHost)
        var batch = new ArrayBuffer[SolrInputDocument]
        while (solrInputDocumentIterator.hasNext) {
          batch.append(solrInputDocumentIterator.next)
          if (batch.size >= batchSize)
            sendBatchToSolr(solrServer, collection, batch)
        }
        if (!batch.isEmpty)
          sendBatchToSolr(solrServer, collection, batch)
  	  })
  	})
  }
  

  def sendBatchToSolr(solrServer:SolrServer, collection:String, batch:ArrayBuffer[SolrInputDocument]) = {
  	var req = new UpdateRequest()
  	req.setParam("collection", collection);
    req.add(asJavaCollection(batch))
    try {
      solrServer.request(req);
    } catch {
      case e: Exception => {
        if (shouldRetry(e)) {
          log.error("Send batch to collection "+collection+" failed due to "+e+"; will retry ...")

          try {
          	Thread.sleep(2000);
          } catch {
          	case ie: InterruptedException => Thread.interrupted()
          }

          try {
            solrServer.request(req);
          } catch {
            case e1: Exception => {
              log.error("Retry send batch to collection "+collection+" failed due to: "+e1, e1);
              if (e1.isInstanceOf[RuntimeException]) {
                throw e1.asInstanceOf[RuntimeException]
              } else {
                throw new RuntimeException(e1)
              }
            }
          } // end catch
        }
        else {
          log.error("Send batch to collection "+collection+" failed due to: "+e, e)
          if (e.isInstanceOf[RuntimeException]) {
            throw e.asInstanceOf[RuntimeException]
          } else {
            throw new RuntimeException(e)
          }
        }
      }
    }
  }

  private def shouldRetry(exc:Exception): Boolean = {
  	val rootCause = SolrException.getRootCause(exc)
  	rootCause.isInstanceOf[ConnectException] || 
  		rootCause.isInstanceOf[ConnectTimeoutException] || 
  		rootCause.isInstanceOf[NoHttpResponseException] || 
  		rootCause.isInstanceOf[SocketException]
  }
  

}
