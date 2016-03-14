package solr

import com.lucidworks.spark.SolrRelation
import com.lucidworks.spark.util.Constants
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources.{DataSourceRegister, BaseRelation, CreatableRelationProvider, RelationProvider}

class DefaultSource extends RelationProvider with CreatableRelationProvider with DataSourceRegister {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    try {
      new SolrRelation(parameters, sqlContext)
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
      val solrRelation: SolrRelation = new SolrRelation(parameters, sqlContext, Some(df))
      solrRelation.insert(df, overwrite = true)
      solrRelation
    } catch {
      case re: RuntimeException => throw re
      case e: Exception => throw new RuntimeException(e)
    }
  }

  override def shortName(): String = Constants.SOLR_FORMAT
}
