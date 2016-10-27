package solr

import com.lucidworks.spark.SolrRelation
import com.lucidworks.spark.util.{ConfigurationConstants, Constants}
import org.apache.spark.sql.solr.SolrSparkSession
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources.{DataSourceRegister, BaseRelation, CreatableRelationProvider, RelationProvider}

class DefaultSource extends RelationProvider with CreatableRelationProvider with DataSourceRegister {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    try {
      sqlContext.sparkSession match {
        case sHiveContext: SolrSparkSession =>
          if (parameters.isDefinedAt(ConfigurationConstants.SOLR_COLLECTION_PARAM))
            sHiveContext.checkReadAccess(parameters.get(ConfigurationConstants.SOLR_COLLECTION_PARAM).get, "solr")
        case _ =>
      }
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
      sqlContext.sparkSession match {
        case sHiveContext: SolrSparkSession =>
          if (parameters.isDefinedAt(ConfigurationConstants.SOLR_COLLECTION_PARAM))
            sHiveContext.checkWriteAccess(parameters.get(ConfigurationConstants.SOLR_COLLECTION_PARAM).get, "solr")
        case _ =>
      }

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
}
