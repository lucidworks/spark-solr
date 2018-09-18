package com.lucidworks.spark.example.ml

import java.net.URI
import java.util.Locale

import com.lucidworks.spark.{LazyLogging, SparkApp}
import com.lucidworks.spark.util.SolrSupport
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option.{builder => OptionBuilder}
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.input.PortableDataStream
import org.joda.time.DateTimeZone
import org.joda.time.format.{DateTimeFormat, DateTimeFormatterBuilder, ISODateTimeFormat}

import collection.mutable.ListBuffer
import collection.JavaConversions._
import scala.io.Source
import scala.util.control.NonFatal

/** Example application to index each article in the 20 newsgroups data as a Solr document.
  * The 20 newsgroups data is downloadable from [[http://qwone.com/~jason/20Newsgroups/]].
  *
  * Articles in any of the three archives available there can be indexed,
  * after first downloading it from the above page and unpacking it.
  *
  * The path you supply as an argument to the `--path` cmdline option (see the
  * "Example invocation" section below) should be the directory containing the newsgroup
  * folders.  All files found recursively under this path will be indexed to Solr.
  *
  * == Prerequisites ==
  *
  * Start Solr in cloud mode, and create a target collection, e.g. (after downloading the
  * binary Solr distribution - see [[https://lucene.apache.org/solr/]] - then unpacking and
  * changing to the unpacked root directory, e.g. `solr-5.4.1/`):
  *
  * {{{
  *   bin/solr -c && bin/solr create -c testing -shards 2
  * }}}
  *
  * == Document fields ==
  *
  * Each header present in the newsgroup articles will be indexed to a Solr dynamic field
  * name prefixed with the header name, e.g. Subject: text will be indexed into a field
  * named Subject_txt_en`.
  *
  * Note that the set of headers in each of the three available archives is different; details
  * are on the download page above.
  *
  * The body of each article will be indexed into the `content_txt_en` field.
  *
  * The `newsgroup_s` field will contain the name of the article's parent directory.
  *
  * The `id` field value will be in the format `newsgroup_articlenum`, e.g. "comp.graphics_38659",
  * where `newsgroup` is the name of the article's parent directory, and `articlenum` is the
  * article filename.
  *
  * The `filepath_s` field will contain the full path of the article source file.
  *
  * If you downloaded the `20news-19997.tar.gz` archive, the only one with the Date: header,
  * dates will be indexed into two fields: the `Date_s` field will contain the original Date:
  * text, and the `Date_tdt` field will contain the date reformatted in ISO-8601 format.
  *
  * == Example invocation ==
  *
  * You must first run `mvn -DskipTests package` in the spark-solr project, and you must download
  * a Spark 1.6.1 binary distribution and point the environment variable `$SPARK_HOME`
  * to the unpacked distribution directory.
  *
  * {{{
  *   $SPARK_HOME/bin/spark-submit --master 'local[2]' --class com.lucidworks.spark.SparkApp \
  *   target/spark-solr-2.0.0-SNAPSHOT-shaded.jar newsgroups2solr -zkHost localhost:9983     \
  *   -collection ml20news -path /relative/or/absolute/path/to/20news-18828`
  * }}}
  *
  * To see a description of all available options, run the following:
  *
  * {{{
  *   $SPARK_HOME/bin/spark-submit --class com.lucidworks.spark.SparkApp \
  *   target/spark-solr-2.0.0-SNAPSHOT-shaded.jar newsgroups2solr --help
  * }}}
  */
class NewsgroupsIndexer extends SparkApp.RDDProcessor with LazyLogging {
  import NewsgroupsIndexer._
  def getName = "newsgroups2solr"
  def getOptions = Array(
    OptionBuilder().longOpt("path").hasArg.argName("PATH").required
      .desc("Path from which to recursively load newsgroup articles").build,
    OptionBuilder().longOpt("collection").hasArg.argName("NAME").required(false)
      .desc("Target Solr collection; default: $DefaultCollection").build)
  def run(conf: SparkConf, cli: CommandLine): Int = {
    val path = cli.getOptionValue("path")
    val collection = cli.getOptionValue("collection", DefaultCollection)
    val zkHost = cli.getOptionValue("zkHost", DefaultZkHost)
    val batchSize = cli.getOptionValue("batchSize", DefaultBatchSize).toInt
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true)
    // Use binaryFiles() because wholeTextFiles() assumes files are UTF-8, but article encoding is Latin-1
    sc.binaryFiles(path).foreachPartition(rows => {
      var numDocs = 0
      val solrServer = SolrSupport.getCachedCloudClient(zkHost)
      val batch = ListBuffer.empty[SolrInputDocument]
      def sendBatch(): Unit = {
        SolrSupport.sendBatchToSolr(solrServer, collection, batch.toList)
        numDocs += batch.size
        logger.info(s"Sent $numDocs docs to Solr from $path")
        batch.clear()
      }
      rows.foreach(row => {  // each row is a Tuple: (path, PortableDataStream)
        val (group, articleNum) = parseFilePath(path, row._1)
        val doc = loadNewsgroupArticle(row._2)
        // Newsgroup name is the parent directory; if this file's path doesn't have a parent directory
        // after removing the source path, use the first listed newsgroup from the article content
        val newsgroup = group.getOrElse(doc.getFieldValues("Newsgroups_ss").head)
        doc.addField("id", s"${newsgroup}_$articleNum")
        doc.addField("newsgroup_s", newsgroup)
        doc.addField("filepath_s", row._1)
        batch += doc
        if (batch.size >= batchSize) sendBatch()
      })
      if (batch.nonEmpty) sendBatch()
    })
    // Explicit commit to make sure all docs are visible
    val solrServer = SolrSupport.getCachedCloudClient(zkHost)
    solrServer.commit(collection, true, true)

    sc.stop()
    0
  }
  def parseFilePath(basePath: String, filePath: String): Tuple2[Option[String],String] = {
    val segments = new URI(basePath).relativize(new URI(filePath)).getPath.split('/').reverse
    val articleNum = segments(0) // Trailing segment is the filename, which is the article number
    // Parent segment, if there is one, is the identified newsgroup
    val newsgroup = if (segments.length > 1) Some(segments(1)) else None
    (newsgroup, articleNum)
  }
  def loadNewsgroupArticle(stream: PortableDataStream): SolrInputDocument = {
    val doc = new SolrInputDocument
    val inputStream = stream.open
    try {
      var noMoreHeaders = false
      val content = new StringBuilder
      for (line <- Source.fromInputStream(inputStream, "ISO-8859-1").getLines) {
        var cleanedLine = NonXmlCharsRegex.replaceAllIn(line, " ") // (Nonprinting) non-XML chars -> spaces
        if (noMoreHeaders) {
          content.append(cleanedLine).append("\n")
        } else {
          NewsgroupHeaderRegex.findFirstMatchIn(cleanedLine) match {
            case None =>
              noMoreHeaders = true
              content.append(cleanedLine).append("\n")
            case Some(fieldValue) => {
              val field = NonAlphaNumCharsRegex.replaceAllIn(fieldValue.group(1), "_")
              val value = fieldValue.group(2)
              field match {
                case "Message-ID" => doc.addField(s"${field}_s", value.trim)
                case "From" | "Subject" | "Sender" => doc.addField(s"${field}_txt_en", value.trim)
                case "Newsgroups" => value.split(",").map(_.trim).filter(_.length > 0)
                  .foreach(newsgroup => doc.addField(s"${field}_ss", newsgroup))
                case "Date" => // 2 fields: original date text, and reformatted as ISO-8601
                  val trimmedValue = value.trim
                  doc.addField(s"${field}_s", trimmedValue)
                  DateConverter.toISO8601(trimmedValue).foreach(doc.addField(s"${field}_tdt", _))
                case _ => doc.addField(s"${field}_txt", value)
              }
            }
          }
        }
      }
      doc.addField("content_txt_en", content.toString())
    } finally {
      inputStream.close()
    }
    doc
  }
}
object NewsgroupsIndexer {
  val DefaultZkHost = "localhost:9983"
  val DefaultBatchSize = "100"
  val DefaultCollection = "ml20news"
  val NonXmlCharsRegex = "[\u0000-\u0008\u000B\u000C\u000E-\u001F]".r
  val NewsgroupHeaderRegex = "^([^: \t]+):[ \t]*(.*)".r
  val NonAlphaNumCharsRegex = "[^_A-Za-z0-9]".r
}

/** Converts 3-letter time zone IDs to IDs that Joda-Time understands, parses dates using
  * a set of date formats known to be present in the 20 newsgroups data, then converts them
  * to ISO8601 format.
  */
object DateConverter extends Serializable with LazyLogging {
  // Map of time zone abbreviations to time zone IDs, from <http://www.timeanddate.com/time/zones/>,
  // <http://www.worldtimezone.com>, and Joda-Time v2.2 DateTimeZone.getAvailableIds()
  val ZoneMap = Map("+3000" -> "Europe/Moscow", // 19 Apr 93 16:15:19 +3000 <- invalid offset, should be +0300
    "ACST" -> "Australia/Adelaide", "BST" -> "Europe/London", "CDT" -> "America/Chicago",
    "CET" -> "Europe/Brussels", "CST" -> "America/Chicago", "ECT" -> "America/Guayaquil",
    "EDT" -> "America/New_York", "EST" -> "America/New_York", "GMT" -> "Etc/GMT",
    "GMT+12" -> "Etc/GMT+12", "IDT" -> "Asia/Jerusalem", "KST" -> "Asia/Seoul",
    "MDT" -> "America/Denver", "MET" -> "Europe/Berlin", "MEZ" -> "Europe/Berlin",
    "MST" -> "America/Denver", "NZDT" -> "Pacific/Auckland", "NZST" -> "Pacific/Auckland",
    "PDT" -> "America/Los_Angeles", "PST" -> "America/Los_Angeles", "TUR" -> "Asia/Istanbul",
    "UT" -> "Etc/UTC", "UTC" -> "Etc/UTC")
    .map(e => e._1 -> DateTimeZone.forID(e._2))
  // Below can't be triple quoted; interpolated raw strings and escapes don't mix: https://issues.scala-lang.org/browse/SI-6476
  val ZonesRegex = s"\\s+\\(?((?i)${ZoneMap.keys.map(z => s"\\Q$z\\E").mkString("|")})\\)?$$".r
  val DateParsers = Array("dd MMM yy",
    "dd MMM yy HH:mm",
    "dd MMM yy HH:mm Z",  // Z is for offsets, e.g. +0200, -0400
    "dd MMM yy HH:mm:ss",
    "dd MMM yy HH:mm:ss Z",
    "MM/dd/yy",
    "MMM dd, yy",
    "yyyy-MM-dd HH:mm:ss",
    "MMM dd HH:mm:ss yy").map(DateTimeFormat.forPattern(_).getParser)
  val Formatter = new DateTimeFormatterBuilder().append(null, DateParsers)
    .toFormatter.withPivotYear(1970).withLocale(Locale.ENGLISH)
  val MultiSpaceRegex = """\s{2,}""".r
  val TrailingOffsetRegex = """\s*\([-+]\d{4}\)$""".r  // Sun, 18 Apr 93 13:35:23 EDT(-0400)
  val DayOfWeekPattern = "(?i:Sun|Mon|Tue(?:s)?|Wed(?:nes)?|Thu(?:rs)?|Fri|Sat(?:ur)?)(?i:day)?"
  val DowRegex = s"$DayOfWeekPattern,?\\s*|\\s*\\($DayOfWeekPattern\\)$$".r
  def toISO8601(date: String): Option[String] = {
    try {
      var zone = DateTimeZone.UTC
      val dateSingleSpaced = MultiSpaceRegex.replaceAllIn(date, " ")
      val dateNoExtraTrailingOffset = TrailingOffsetRegex.replaceFirstIn(dateSingleSpaced, "")
      val dateNoDow = DowRegex.replaceFirstIn(dateNoExtraTrailingOffset, "")
      val dateNoZone = ZonesRegex.replaceAllIn(dateNoDow, m => {
        zone = ZoneMap(m.group(1).toUpperCase(Locale.ROOT))
        ""}) // remove time zone abbreviations
      Some(Formatter.withZone(zone).parseDateTime(dateNoZone).toString(ISODateTimeFormat.dateTimeNoMillis()))
    } catch {
      case NonFatal(e) => logger.error(s"Failed to parse date '$date': $e")
        None
    }
  }
}
