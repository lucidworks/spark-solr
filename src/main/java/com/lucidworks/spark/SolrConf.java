package com.lucidworks.spark;

import org.apache.solr.common.params.ModifiableSolrParams;
import scala.collection.JavaConverters;

import java.io.Serializable;
import java.util.*;

import static com.lucidworks.spark.util.ConfigurationConstants.*;
import static com.lucidworks.spark.util.QueryConstants.*;

public class SolrConf implements Serializable{

  private Map<String, String> sqlOptions = Collections.emptyMap();
  private ModifiableSolrParams solrConfigParams;
  private static final List<String> ESCAPE_FIELD_NAMES = Arrays.asList("fl", "rows", "q");

  public SolrConf(scala.collection.immutable.Map<String, String> config) {
    if (config != null) {
      this.sqlOptions = JavaConverters.asJavaMapConverter(config).asJava();
    }
    this.solrConfigParams = parseSolrParams(this.sqlOptions);
  }

  public String getZKHost() {
    if (sqlOptions.containsKey(SOLR_ZK_HOST_PARAM)) {
      return sqlOptions.get(SOLR_ZK_HOST_PARAM);
    }
    return null;
  }

  public String getCollection() {
    if (sqlOptions.containsKey(SOLR_COLLECTION_PARAM)) {
      return sqlOptions.get(SOLR_COLLECTION_PARAM);
    }
    return null;
  }

  public String getQuery() {
    if (sqlOptions.containsKey(SOLR_QUERY_PARAM)) {
      return sqlOptions.get(SOLR_QUERY_PARAM);
    }
    return DEFAULT_QUERY;
  }

  public String getFields() {
    if (sqlOptions.containsKey(SOLR_FIELD_LIST_PARAM)) {
      return sqlOptions.get(SOLR_FIELD_LIST_PARAM);
    }
    return null;
  }

  public String[] getFieldList() {
    return (getFields() != null) ? getFields().split(","): new String[0];
  }

  public int getRows() {
    if (sqlOptions.containsKey(SOLR_ROWS_PARAM)) {
      return Integer.parseInt(sqlOptions.get(SOLR_ROWS_PARAM));
    }
    return DEFAULT_PAGE_SIZE;
  }

  public String getSplitField() {
    if (sqlOptions.containsKey(SOLR_SPLIT_FIELD_PARAM)) {
      return sqlOptions.get(SOLR_SPLIT_FIELD_PARAM);
    }
    return null;
  }

  public int getSplitsPerShard() {
    if (sqlOptions.containsKey(SOLR_SPLITS_PER_SHARD_PARAM)) {
      return Integer.parseInt(sqlOptions.get(SOLR_SPLITS_PER_SHARD_PARAM));
    }
    return DEFAULT_SPLITS_PER_SHARD;
  }

  public boolean escapeFieldNames() {
    return Boolean.parseBoolean(sqlOptions.get(ESCAPE_FIELDNAMES));
  }

  public ModifiableSolrParams getSolrParams() {
    return this.solrConfigParams;
  }

  // Extract config params with prefix "solr."
  private static ModifiableSolrParams parseSolrParams(Map<String, String> sqlOptions) {
    ModifiableSolrParams params = new ModifiableSolrParams();

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
