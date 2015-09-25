package com.lucidworks.spark.query;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.QueryResponse;

import java.io.IOException;
import java.io.Serializable;

public abstract class AbstractFieldShardSplitStrategy implements ShardSplitStrategy, Serializable {

  protected FieldStatsInfo getFieldStatsInfo(SolrClient solrClient, String field) throws IOException, SolrServerException {
    SolrQuery statsQuery = new SolrQuery("*:*");
    statsQuery.setRows(0);
    statsQuery.setStart(0);
    statsQuery.set("distrib", false);
    statsQuery.setGetFieldStatistics(field);
    QueryResponse qr = solrClient.query(statsQuery);
    return qr.getFieldStatsInfo().get(field);
  }

  protected void fetchNumHits(SolrClient solrClient, ShardSplit ss) throws IOException, SolrServerException {
    SolrQuery splitQuery = ss.getSplitQuery();
    splitQuery = splitQuery.getCopy();
    splitQuery.setRows(0);
    splitQuery.setStart(0);
    QueryResponse qr = solrClient.query(splitQuery);
    ss.setNumHits(qr.getResults().getNumFound());
  }
}
