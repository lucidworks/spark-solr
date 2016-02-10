package solr;

import com.lucidworks.spark.SolrJavaRelation;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.sources.RelationProvider;
import scala.collection.immutable.Map;

import java.io.Serializable;

public class DefaultSource implements RelationProvider, CreatableRelationProvider, Serializable {

  public static final String SOLR_FORMAT = "solr";

  public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> config) {
    SolrJavaRelation solrRelation = null;
    try {
      solrRelation = new SolrJavaRelation(sqlContext, config);
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
    SolrJavaRelation solrRelation = null;
    try {
      solrRelation = new SolrJavaRelation(sqlContext, config, dataFrame);
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
