package com.lucidworks.spark

import java.io.File
import java.util.UUID

import com.lucidworks.spark.util.{EventsimUtil, SolrCloudUtil}
import org.apache.commons.io.FileUtils
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.cloud.MiniSolrCloudCluster
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.eclipse.jetty.servlet.ServletHolder
import org.junit.Assert._
import org.restlet.ext.servlet.ServerServlet
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SolrCloudTestBuilder extends BeforeAndAfterAll with Logging { this: Suite =>

  @transient var cluster: MiniSolrCloudCluster = _
  @transient var cloudClient: CloudSolrClient = _
  var zkHost: String = _
  var testWorkingDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val solrXml = new File("src/test/resources/solr.xml")
    val solrXmlContents: String = TestSolrCloudClusterSupport.readSolrXml(solrXml)

    val targetDir = new File("target")
    if (!targetDir.isDirectory)
      fail("Project 'target' directory not found at :" + targetDir.getAbsolutePath)

    testWorkingDir = new File(targetDir, "scala-solrcloud-" + System.currentTimeMillis)
    if (!testWorkingDir.isDirectory)
      testWorkingDir.mkdirs

    // need the schema stuff
    val extraServlets: java.util.SortedMap[ServletHolder, String] = new java.util.TreeMap[ServletHolder, String]()

    val solrSchemaRestApi : ServletHolder = new ServletHolder("SolrSchemaRestApi", classOf[ServerServlet])
    solrSchemaRestApi.setInitParameter("org.restlet.application", "org.apache.solr.rest.SolrSchemaRestApi")
    extraServlets.put(solrSchemaRestApi, "/schema/*")

    cluster = new MiniSolrCloudCluster(1, null /* hostContext */,
      testWorkingDir.toPath, solrXmlContents, extraServlets, null /* extra filters */)
    cloudClient = new CloudSolrClient(cluster.getZkServer.getZkAddress, true)
    cloudClient.connect()

    assertTrue(!cloudClient.getZkStateReader.getClusterState.getLiveNodes.isEmpty)
    zkHost = cluster.getZkServer.getZkAddress
  }

  override def afterAll(): Unit = {
    cloudClient.close()
    cluster.shutdown()

    if (testWorkingDir != null && testWorkingDir.isDirectory) {
      FileUtils.deleteDirectory(testWorkingDir)
    }

    super.afterAll()
  }

}

trait SparkSolrContextBuilder extends BeforeAndAfterAll { this: Suite =>

  @transient var sc: SparkContext = _
  @transient var sqlContext: SQLContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set("spark.default.parallelism", "1")
    sc = new SparkContext(conf)
    sqlContext = new SQLContext(sc)
  }

  override def afterAll(): Unit = {
    sc.stop()
    super.afterAll()
  }
}

// General builder to be used by all the tests that need Solr and Spark running
trait TestSuiteBuilder extends SparkSolrFunSuite with SparkSolrContextBuilder with SolrCloudTestBuilder {}

// Builder to be used by all the tests that need Eventsim data. All test methods will re-use the same collection name
trait EventsimBuilder extends TestSuiteBuilder {

  val collectionName: String = "EventsimTest-" + UUID.randomUUID().toString

  override def beforeAll(): Unit = {
    super.beforeAll()
    SolrCloudUtil.buildCollection(zkHost, collectionName, null, numShards, cloudClient, sc)
    EventsimUtil.defineSchemaForEventSim(zkHost, collectionName)
    EventsimUtil.loadEventSimDataSet(zkHost, collectionName, sqlContext)
  }


  override def afterAll(): Unit = {
    SolrCloudUtil.deleteCollection(collectionName, cluster)
    super.afterAll()
  }

  def eventSimCount: Int = 1000

  def fieldsCount: Int = 19

  def numShards: Int = 2
}

trait MovielensBuilder extends TestSuiteBuilder {

  val moviesColName: String = "movielens_movies"
  val ratingsColName: String = "movielens_ratings"
  val userColName: String = "movielens_users"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createCollections()
    MovieLensUtil.indexMovieLensDataset(sqlContext, zkHost)
  }

  override def afterAll(): Unit = {
    deleteCollections()
    super.afterAll()
  }

  def createCollections(): Unit = {
    SolrCloudUtil.buildCollection(zkHost, moviesColName, null, 1, cloudClient, sc)
    SolrCloudUtil.buildCollection(zkHost, ratingsColName, null, 1, cloudClient, sc)
    SolrCloudUtil.buildCollection(zkHost, userColName, null, 1, cloudClient, sc)
  }

  def deleteCollections(): Unit = {
    SolrCloudUtil.deleteCollection(ratingsColName, cluster)
    SolrCloudUtil.deleteCollection(moviesColName, cluster)
    SolrCloudUtil.deleteCollection(userColName, cluster)
  }
}

object MovieLensUtil {
  val dataDir: String = "src/test/resources/ml-100k"

  def indexMovieLensDataset(sqlContext: SQLContext, zkhost: String): Unit = {
    sqlContext.udf.register("toInt", (str: String) => str.toInt)

    var userDF = sqlContext.read.format("com.databricks.spark.csv").option("delimiter","|").option("header", "false").load(s"${dataDir}/u.user")
    userDF.registerTempTable("user")
    userDF = sqlContext.sql("select C0 as user_id,toInt(C1) as age,C2 as gender,C3 as occupation,C4 as zip_code from user")
    userDF.write.format("solr").options(Map("zkhost" -> zkhost, "collection" -> "movielens_users", "gen_uniq_key" -> "true")).save

    var itemDF = sqlContext.read.format("com.databricks.spark.csv").option("delimiter","|").option("header", "false").load(s"${dataDir}/u.item")
    itemDF.registerTempTable("item")
    itemDF = sqlContext.sql("select C0 as movie_id, C1 as title, C1 as title_txt_en, C2 as release_date, C3 as video_release_date, C4 as imdb_url, C5 as genre_unknown, C6 as genre_action, C7 as genre_adventure, C8 as genre_animation, C9 as genre_children, C10 as genre_comedy, C11 as genre_crime, C12 as genre_documentary, C13 as genre_drama, C14 as genre_fantasy, C15 as genre_filmnoir, C16 as genre_horror, C17 as genre_musical, C18 as genre_mystery, C19 as genre_romance, C20 as genre_scifi, C21 as genre_thriller, C22 as genre_war, C23 as genre_western from item")

    itemDF.registerTempTable("item")
    itemDF = sqlContext.sql("select *, concat(genre_unknown,genre_action,genre_adventure,genre_animation,genre_children,genre_comedy,genre_crime,genre_documentary,genre_drama,genre_fantasy,genre_filmnoir,genre_horror,genre_musical,genre_mystery,genre_romance,genre_scifi,genre_thriller,genre_war,genre_western) as genre_list from item")

    // this is pretty horrific but we need to built a multi-valued string field of genres for this movie
    sqlContext.udf.register("genres", (genres: String) => {
      var list = scala.collection.mutable.ListBuffer.empty[String]
      var arr = genres.toCharArray
      val g = List("unknown","action","adventure","animation","children","comedy","crime","documentary","drama","fantasy","filmnoir","horror","musical","mystery","romance","scifi","thriller","war","western")
      for (i <- arr.indices) {
        if (arr(i) == '1')
          list += g(i)
      }
      list
    })

    itemDF.registerTempTable("item")
    itemDF = sqlContext.sql("select *, genres(genre_list) as genre from item")
    itemDF = itemDF.drop("genre_list")

    itemDF.write.format("solr").options(Map("zkhost" -> zkhost, "collection" -> "movielens_movies", "gen_uniq_key" -> "true", "batch_size" -> "2000")).mode(org.apache.spark.sql.SaveMode.Overwrite).save

    sqlContext.udf.register("secs2ts", (secs: Long) => new java.sql.Timestamp(secs*1000))

    var ratingDF = sqlContext.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header", "false").load(s"${dataDir}/u.data")
    ratingDF.registerTempTable("rating")
    ratingDF = sqlContext.sql("select C0 as user_id, C1 as movie_id, toInt(C2) as rating, secs2ts(C3) as rating_timestamp from rating")
//    ratingDF.printSchema
    ratingDF.write.format("solr").options(Map("zkhost" -> zkhost, "collection" -> "movielens_ratings", "gen_uniq_key" -> "true", "batch_size" -> "10000")).mode(org.apache.spark.sql.SaveMode.Overwrite).save

  }

}