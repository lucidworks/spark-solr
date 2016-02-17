package com.lucidworks.spark;

import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.spark.SparkConf;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.io.Serializable;
import java.util.*;

import static com.lucidworks.spark.util.ConfigurationConstants.*;
import static com.lucidworks.spark.util.QueryConstants.*;

public class SolrConf implements Serializable{

  private Map<String, String> options = Collections.emptyMap();
  private ModifiableSolrParams solrConfigParams;
  private static final List<String> ESCAPE_FIELD_NAMES = Arrays.asList("fl", "rows", "q");

  public SolrConf(scala.collection.immutable.Map<String, String> config) {
    if (config != null) {
      this.options = JavaConverters.asJavaMapConverter(config).asJava();
    }
    this.solrConfigParams = parseSolrParams(this.options);
  }

  public SolrConf(SparkConf conf) {
    for (Tuple2<String, String> c: conf.getAll()) {
      options.put(c._1(), c._2());
    }
    this.solrConfigParams = parseSolrParams(this.options);
  }

  public String getZKHost() {
    if (options.containsKey(SOLR_ZK_HOST_PARAM)) {
      return options.get(SOLR_ZK_HOST_PARAM);
    }
    return null;
  }

  public String getCollection() {
    if (options.containsKey(SOLR_COLLECTION_PARAM)) {
      return options.get(SOLR_COLLECTION_PARAM);
    }
    return null;
  }

  public String getQuery() {
    if (options.containsKey(SOLR_QUERY_PARAM)) {
      return options.get(SOLR_QUERY_PARAM);
    }
    return DEFAULT_QUERY;
  }

  public String getFields() {
    if (options.containsKey(SOLR_FIELD_LIST_PARAM)) {
      return options.get(SOLR_FIELD_LIST_PARAM);
    }
    return null;
  }

  public String[] getFieldList() {
    return (getFields() != null) ? getFields().split(","): new String[0];
  }

  public int getRows() {
    if (options.containsKey(SOLR_ROWS_PARAM)) {
      return Integer.parseInt(options.get(SOLR_ROWS_PARAM));
    }
    return DEFAULT_PAGE_SIZE;
  }

  public String getSplitField() {
    if (options.containsKey(SOLR_SPLIT_FIELD_PARAM)) {
      return options.get(SOLR_SPLIT_FIELD_PARAM);
    }
    return null;
  }

  public int getSplitsPerShard() {
    if (options.containsKey(SOLR_SPLITS_PER_SHARD_PARAM)) {
      return Integer.parseInt(options.get(SOLR_SPLITS_PER_SHARD_PARAM));
    }
    return DEFAULT_SPLITS_PER_SHARD;
  }

  public boolean escapeFieldNames() {
    return Boolean.parseBoolean(options.get(ESCAPE_FIELDNAMES));
  }

  public ModifiableSolrParams getSolrParams() {
    return this.solrConfigParams;
  }

  // Extract config params with prefix "solr."
  private static ModifiableSolrParams parseSolrParams(Map<String, String> options) {
    ModifiableSolrParams params = new ModifiableSolrParams();

    for (String s: options.keySet()) {
      if (s.startsWith(CONFIG_PREFIX)) {
        String param = s.substring(CONFIG_PREFIX.length());
        if (!ESCAPE_FIELD_NAMES.contains(param) && options.get(param) != null) {
          params.add(param, options.get(s));
        }
      }
    }
    return params;
  }

}
