package com.lucidworks.spark.filter;

import org.apache.commons.cli.CommandLine;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;
import java.util.List;

/**
 * Used by the document filtering framework to delegate the loading and
 * of queries used to filter documents and what to do when a document
 * matches a query.
 */
public interface DocFilterContext extends Serializable {
  void init(JavaStreamingContext jssc, CommandLine cli);
  String getDocIdFieldName();
  List<SolrQuery> getQueries();
  void onMatch(SolrQuery query, SolrInputDocument inputDoc);
}
