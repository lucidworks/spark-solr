package com.lucidworks.spark;

import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.spark.SparkConf;
import scala.collection.JavaConverters;

import java.io.Serializable;
import java.util.*;

import static com.lucidworks.spark.util.ConfigurationConstants.*;

public class SolrConf implements Serializable{

  private final SparkConf sparkConf;
  private Map<String, String> sqlOptions = Collections.emptyMap();
  private ModifiableSolrParams solrConfigParams;
  private static final List<String> ESCAPE_FIELD_NAMES = Arrays.asList("fl", "rows", "q");

  public SolrConf(SparkConf sparkConf) {
    this(sparkConf, null);
  }

  public SolrConf(SparkConf sparkConf, scala.collection.immutable.Map<String, String> config) {
    this.sparkConf = sparkConf;
    if (config != null) {
      this.sqlOptions = JavaConverters.asJavaMapConverter(config).asJava();
    }
    this.solrConfigParams = parseSolrParams(this.sparkConf, this.sqlOptions);
  }

  public String getQuery() {
    if (sqlOptions.containsKey(SOLR_QUERY_PARAM)) {
      return sqlOptions.get(SOLR_QUERY_PARAM);
    }
    return sparkConf.get(SOLR_QUERY_PARAM, "*:*");
  }

  public String getFields() {
    if (sqlOptions.containsKey(SOLR_FIELD_LIST_PARAM)) {
      return sqlOptions.get(SOLR_FIELD_LIST_PARAM);
    }
    return sparkConf.get(SOLR_FIELD_LIST_PARAM, null);
  }

  public String[] getFieldList() {
    return (getFields() != null) ? getFields().split(","): new String[0];
  }

  public int getRows() {
    if (sqlOptions.containsKey(SOLR_ROWS_PARAM)) {
      return Integer.parseInt(sqlOptions.get(SOLR_ROWS_PARAM));
    }
    return sparkConf.getInt(SOLR_ROWS_PARAM, 1000);
  }

  public String getSplitField() {
    if (sqlOptions.containsKey(SOLR_SPLIT_FIELD_PARAM)) {
      return sqlOptions.get(SOLR_SPLIT_FIELD_PARAM);
    }
    return sparkConf.get(SOLR_SPLIT_FIELD_PARAM, null);
  }

  public int getSplitsPerShard() {
    if (sqlOptions.containsKey(SOLR_SPLIT_FIELD_PARAM)) {
      return Integer.parseInt(sqlOptions.get(SOLR_SPLIT_FIELD_PARAM));
    }
    return sparkConf.getInt(SOLR_SPLITS_PER_SHARD_PARAM, 20);
  }

  public boolean preserveSchema() {
    if (sqlOptions.containsKey(PRESERVE_SCHEMA)) {
      return Boolean.parseBoolean(sqlOptions.get(PRESERVE_SCHEMA));
    }
    return sparkConf.getBoolean(PRESERVE_SCHEMA, false);
  }

  public boolean escapeFieldNames() {
    if (sqlOptions.containsKey(ESCAPE_FIELDNAMES)) {
      return Boolean.parseBoolean(sqlOptions.get(ESCAPE_FIELDNAMES));
    }
    return sparkConf.getBoolean(ESCAPE_FIELDNAMES, false);
  }

  public ModifiableSolrParams getSolrParams() {
    return this.solrConfigParams;
  }

  // Extract config params with prefix "solr."
  private static ModifiableSolrParams parseSolrParams(SparkConf conf, Map<String, String> sqlOptions) {
    ModifiableSolrParams params = new ModifiableSolrParams();

    for (int i=0; i<conf.getAll().length; i++) {
      if (conf.getAll()[i]._1().startsWith(CONFIG_PREFIX)) {
        String param = conf.getAll()[i]._1().substring(CONFIG_PREFIX.length());
        if (!ESCAPE_FIELD_NAMES.contains(param) && conf.get(param) != null) {
          params.add(param, conf.get(param));
        }
      }
    }

    for (String s: sqlOptions.keySet()) {
      if (s.startsWith(CONFIG_PREFIX)) {
        String param = s.substring(CONFIG_PREFIX.length());
        if (!ESCAPE_FIELD_NAMES.contains(param) && sqlOptions.get(param) != null) {
          params.add(param, sqlOptions.get(s));
        }
      }
    }
    return params;
  }
}
