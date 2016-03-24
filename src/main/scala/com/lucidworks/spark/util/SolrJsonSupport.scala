package com.lucidworks.spark.util

import java.io.{InputStream, InputStreamReader, BufferedReader}
import java.net.URL

import org.apache.http.{HttpEntity, HttpResponse}
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.{HttpGet, HttpUriRequest, HttpPost}
import org.apache.solr.client.solrj.SolrServerException
import org.apache.solr.client.solrj.impl.HttpClientUtil
import org.apache.solr.common.SolrException
import org.apache.solr.common.params.ModifiableSolrParams
import org.apache.spark.Logging

import scala.util.control.Breaks._

import org.json4s._
import org.json4s.jackson.JsonMethods._

import com.lucidworks.spark.util.JsonUtil._

object SolrJsonSupport extends Logging {

  /**
   * Utility function for sending HTTP GET request to Solr with built-in retry support.
   */
  def getJson(httpClient: HttpClient, getUrl: String, attempts: Int): JValue = {

    var triedAttempts = attempts
    var json: Option[JValue] = None
    if (triedAttempts >= 1) {
      try {
        triedAttempts -= 1
        json = Some(getJson(httpClient, getUrl))
      } catch {
        case e: Exception =>
          if (triedAttempts > 0 && SolrSupport.shouldRetry(e)) {
            //log.warn("Request to "+getUrl+" failed due to: "+exc.getMessage()+
            //    ", sleeping for 5 seconds before re-trying the request ...")
            try {
              Thread.sleep(2000)
            } catch {
              case ie: InterruptedException => Thread.interrupted()
            }

            // retry using recursion with one-less attempt available
            getJson(httpClient, getUrl, triedAttempts)
          } else {
            throw e
          }
      }
    }

    json.getOrElse(JNothing)
  }

  def getJson(httpClient: HttpClient, getUrl: String): JValue = {
    var newGetUrl = getUrl
    // ensure we're requesting JSON back from Solr
    val url = new URL(getUrl)
    val queryString = url.getQuery
    if (queryString != null) {
      if (!queryString.contains("wt=json")) {
        newGetUrl += "&wt=json"
      }
    } else {
      newGetUrl += "?wt=json"
    }

    val httpGet = new HttpGet(newGetUrl)
    if (log.isDebugEnabled)
      log.debug("Requesting url: " + getUrl)
    doJsonRequest(httpClient, newGetUrl, httpGet)
  }

  /**
   * Utility function for sending HTTP GET request to Solr and then doing some
   * validation of the response.
   */
  def doJsonRequest(httpClient: HttpClient, url: String, request: HttpUriRequest): JValue = {

    // Execute the request
    val response: HttpResponse = httpClient.execute(request)

    // Get hold of the response entity
    val entity = response.getEntity
    val statusCode = response.getStatusLine.getStatusCode

    // Parse the error message in case of unsuccessful request
    if (statusCode != 200) {
      val body = getHttpResponseAsString(entity)
      throw new SolrException(SolrException.ErrorCode.getErrorCode(statusCode),
        "Request [" + url + "] failed due to: " + response.getStatusLine + ": " + body)
    }

    // If the response does not enclose an entity, there is no need to worry about connection release
    if (entity != null) {
      try {
        val jValue: JValue = parse(new StreamInput(entity.getContent))

        // Check the response JSON from Solr to see if it is an error
        if (jValue.has("responseHeader") && (jValue \ "responseHeader").has("status")) {
          val jStatus = jValue \ "responseHeader" \ "status"
          jStatus match {
            case status: JInt =>
              if (status.values == BigInt(-1)) {
                throw new SolrServerException("Unable to determine outcome of the request to:" + url + "! Response: " + compact(jValue))
              }
              if (status.values != BigInt(0)) {
                var errorMsg: Option[String] = None
                if (jValue.has("error") && (jValue \ "error").has("msg")) {
                  errorMsg = Some(compact(render(jValue \ "error" \ "msg")))
                } else {
                  errorMsg = Some(compact(render(jValue)))
                }
                throw new SolrException(SolrException.ErrorCode.getErrorCode(statusCode),
                "Request to " + url + " failed due to: " + errorMsg.get)
              }
            case status: AnyRef => throw new Exception("unknown data type for 'status' in object " + compact(jValue \ "responseHeader"))
          }
          jValue
        } else {
          throw new Exception("Could not find 'responseHeader' in json: " + compact(jValue))
        }

      } catch {
        case ex: RuntimeException =>
          // In case of an unexpected exception you may want to abort
          // the HTTP request in order to shut down the underlying
          // connection and release it back to the connection manager.
          request.abort()
          throw ex
        case ex: Exception =>
          log.error("Exception while parsing JSON stream for url '" + url + "'. The payload is " + getHttpResponseAsString(entity))
          throw ex
      }
    } else {
      throw new Exception("No entity found in the response")
    }

  }

  def getHttpResponseAsString(entity: HttpEntity): String = {
    val body = new StringBuilder
    if (entity != null) {
      val inStream = entity.getContent
      var line: String = null
      try {
        val reader = new BufferedReader(new InputStreamReader(inStream, "UTF-8"))
        line = reader.readLine()
        while (line != null) {
          body.append(line)
          line = reader.readLine
        }
      } catch {
        case ignore: Exception =>  // squelch it - just trying to compose an error message here
      } finally {
        inStream.close()
      }
    }
    body.result()
  }

  def getHttpClient(): HttpClient = {
    val params = new ModifiableSolrParams()
    params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, 128)
    params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, 32)
    params.set(HttpClientUtil.PROP_FOLLOW_REDIRECTS, false)
    HttpClientUtil.createClient(params)
  }

}
