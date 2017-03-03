package com.lucidworks.spark.port.example.events

import java.net.URL
import java.util.{Calendar, TimeZone}

import com.lucidworks.spark.SparkApp.RDDProcessor
import com.lucidworks.spark.fusion.FusionPipelineClient
import org.apache.commons.cli.{CommandLine, Option}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions.bufferAsJavaList
import scala.collection.mutable.ListBuffer

class EventsimIndexer extends RDDProcessor {
  val DEFAULT_ENDPOINT = 
    "http://localhost:8764/api/apollo/index-pipelines/eventsim-default/collections/eventsim/index"

  def getName: String = "eventsim"

  def getOptions: Array[Option] = {
    Array(
      Option.builder()
        .hasArg().required(true)
        .desc("Path to an eventsim JSON file")
        .longOpt("eventsimJson").build,
      Option.builder()
        .hasArg()
        .desc("Fusion endpoint(s); default is " + DEFAULT_ENDPOINT)
        .longOpt("fusion").build,
      Option.builder()
        .hasArg()
        .desc("Fusion username; default is admin")
        .longOpt("fusionUser").build,
      Option.builder()
        .hasArg()
        .desc("Fusion password; required if fusionAuthEnbled=true")
        .longOpt("fusionPass").build,
      Option.builder()
        .hasArg()
        .desc("Fusion security realm; default is native")
        .longOpt("fusionRealm").build,
      Option.builder()
        .hasArg()
        .desc("Fusion authentication enabled; default is true")
        .longOpt("fusionAuthEnabled").build,
      Option.builder()
        .hasArg()
        .desc("Fusion indexing batch size; default is 100")
        .longOpt("fusionBatchSize").build
    )
  }

  def run(conf: SparkConf, cli: CommandLine): Int = {
    val fusionEndpoints: String = cli.getOptionValue("fusion", DEFAULT_ENDPOINT)
    val fusionAuthEnabled: Boolean =
      "true".equalsIgnoreCase(cli.getOptionValue("fusionAuthEnabled", "true"))
    val fusionUser: String = cli.getOptionValue("fusionUser", "admin")

    val fusionPass: String = cli.getOptionValue("fusionPass")
    if (fusionAuthEnabled && (fusionPass == null || fusionPass.isEmpty))
      throw new IllegalArgumentException("Fusion password is required when authentication is enabled!")

    val fusionRealm: String = cli.getOptionValue("fusionRealm", "native")
    val fusionBatchSize: Int = cli.getOptionValue("fusionBatchSize", "100").toInt

    val urls = fusionEndpoints.split(",").distinct
    val url = new URL(urls(0))
    val pipelinePath = url.getPath

    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    sparkSession.read.json(cli.getOptionValue("eventsimJson")).foreachPartition(rows => {
      val fusion: FusionPipelineClient =
        if (fusionAuthEnabled) new FusionPipelineClient(fusionEndpoints, fusionUser, fusionPass, fusionRealm)
        else new FusionPipelineClient(fusionEndpoints)

      val batch = new ListBuffer[Map[String,_]]()
      rows.foreach(next => {
        var userId : String = ""
        var sessionId : String = ""
        var ts : Long = 0

        val fields = new ListBuffer[Map[String,_]]()
        for (c <- 0 to next.length-1) {
          val obj = next.get(c)
          if (obj != null) {
            var colValue = obj
            val fieldName = next.schema.fieldNames(c)
            if ("ts" == fieldName || "registration" == fieldName) {
              ts = obj.asInstanceOf[Long]
              val cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
              cal.setTimeInMillis(ts)
              colValue = cal.getTime.toInstant.toString
            } else if ("userId" == fieldName) {
              userId = obj.toString
            } else if ("sessionId" == fieldName) {
              sessionId = obj.toString
            }
            fields += Map("name" -> fieldName, "value" -> colValue)
          }
        }

        batch += Map("id" -> s"$userId-$sessionId-$ts", "fields" -> fields)

        if (batch.size == fusionBatchSize) {
          fusion.postBatchToPipeline(pipelinePath, bufferAsJavaList(batch))
          batch.clear
        }
      })

      // post the final batch if any left over
      if (!batch.isEmpty) {
        fusion.postBatchToPipeline(pipelinePath, bufferAsJavaList(batch))
        batch.clear
      }
    })

    sparkSession.stop()

    0
  }
}
