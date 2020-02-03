package solr

import com.lucidworks.spark.{SolrRelation, SolrStreamWriter}
import com.lucidworks.spark.util.Constants
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode

class DefaultSource extends RelationProvider with CreatableRelationProvider with StreamSinkProvider with DataSourceRegister {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    try {
      new SolrRelation(parameters, sqlContext.sparkSession)
    } catch {
      case re: RuntimeException => throw re
      case e: Exception => throw new RuntimeException(e)
    }
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      df: DataFrame): BaseRelation = {
    try {
      // TODO: What to do with the saveMode?
      val solrRelation: SolrRelation = new SolrRelation(parameters, Some(df), sqlContext.sparkSession)
      solrRelation.insert(df, overwrite = true)
      solrRelation
    } catch {
      case re: RuntimeException => throw re
      case e: Exception => throw new RuntimeException(e)
    }
  }

  override def shortName(): String = Constants.SOLR_FORMAT

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    new SolrStreamWriter(sqlContext.sparkSession, parameters, partitionColumns, outputMode)
  }
}
