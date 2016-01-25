package com.lw.spark;

import org.apache.spark.SparkConf;
import static com.lw.spark.util.ConfigurationConstants.*;

public class SolrConf {

  private final SparkConf sparkConf;

  public SolrConf(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
  }

  public String getQuery() {
    return sparkConf.get(SOLR_QUERY_PARAM, "*:*");
  }

  public String getFields() {
    return sparkConf.get(SOLR_FIELD_LIST_PARAM, "fields");
  }

  public int getRows() {
    return sparkConf.getInt(SOLR_ROWS_PARAM, 1000);
  }

  public String getSplitField() {
    return sparkConf.get(SOLR_SPLIT_FIELD_PARAM, null);
  }

  public int getSplitsPerShard() {
    return sparkConf.getInt(SOLR_SPLITS_PER_SHARD_PARAM, 20);
  }

  public boolean preserveSchema() {
    return sparkConf.getBoolean(PRESERVE_SCHEMA, false);
  }

  public boolean escapeFieldNames() {
    return sparkConf.getBoolean(ESCAPE_FIELDNAMES, false);
  }

}
