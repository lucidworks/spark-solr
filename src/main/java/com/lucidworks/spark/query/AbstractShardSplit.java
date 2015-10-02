package com.lucidworks.spark.query;

import org.apache.solr.client.solrj.SolrQuery;

import java.io.Serializable;

public abstract class AbstractShardSplit<T> implements ShardSplit<T>, Serializable {

  protected SolrQuery query;
  protected String shardUrl;
  protected String rangeField;
  protected T lowerInc;
  protected T upper;
  protected T min;
  protected T max;
  protected Long numHits = null;
  protected String fq;

  protected AbstractShardSplit(SolrQuery query, String shardUrl, String rangeField, T min, T max, T lowerInc, T upper) {
    this.query = query;
    this.shardUrl = shardUrl;
    this.rangeField = rangeField;
    this.min = min;
    this.max = max;
    this.lowerInc = lowerInc;
    this.upper = upper;
    this.fq = buildSplitFq();
  }

  protected AbstractShardSplit(SolrQuery query, String shardUrl, String rangeField, String fq) {
    this.query = query;
    this.shardUrl = shardUrl;
    this.rangeField = rangeField;
    this.fq = fq;
  }

  protected String buildSplitFq() {
    StringBuilder sb = new StringBuilder();
    if (lowerInc != null) {
      if (lowerInc.equals(upper)) {
        sb.append(rangeField).append(":").append(lowerInc);
      } else {
        String exc = max.equals(upper) ? "]" : "}";
        sb.append(rangeField).append(":[").append(lowerInc).append(" TO ").append(upper).append(exc);
      }
    } else {
      sb.append("-").append(rangeField).append(":[* TO *]");
    }
    return sb.toString();
  }

  public SolrQuery getQuery() {
    return query;
  }

  public SolrQuery getSplitQuery() {
    SolrQuery splitQuery = query.getCopy();
    splitQuery.addFilterQuery(getSplitFilterQuery());
    return splitQuery;
  }

  public String getSplitFieldName() {
    return rangeField;
  }

  public String getSplitFilterQuery() {
    return fq;
  }

  public String getShardUrl() {
    return shardUrl;
  }

  public T getUpper() {
    return upper;
  }

  public T getLowerInc() {
    return lowerInc;
  }

  public Long getNumHits() {
    return numHits;
  }

  public void setNumHits(Long numHits) {
    this.numHits = numHits;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(fq).append(" (").append((numHits != null) ? numHits.toString() : "?").append(")");
    return sb.toString();
  }
}
