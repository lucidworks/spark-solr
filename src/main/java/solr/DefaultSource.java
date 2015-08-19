package solr;

import com.lucidworks.spark.SolrRelation;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.sources.RelationProvider;
import scala.collection.immutable.Map;

import java.io.Serializable;

public class DefaultSource implements RelationProvider, CreatableRelationProvider, Serializable {

  public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> config) {
    SolrRelation solrRelation = null;
    try {
      solrRelation = new SolrRelation(sqlContext, config);
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException)e;
      } else {
        throw new RuntimeException(e);
      }
    }
    return solrRelation;
  }

  public BaseRelation createRelation(SQLContext sqlContext, SaveMode saveMode, Map<String, String> config, DataFrame dataFrame) {
    SolrRelation solrRelation = null;
    try {
      solrRelation = new SolrRelation(sqlContext, config, dataFrame);
      solrRelation.insert(dataFrame, true);
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException)e;
      } else {
        throw new RuntimeException(e);
      }
    }
    return solrRelation;
  }
}
