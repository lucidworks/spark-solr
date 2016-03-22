package com.lucidworks.spark.query;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;

import java.io.IOException;
import java.io.Serializable;

/**
 * Implements a shard splitting strategy for integral fields (long / integer). Concrete classes mostly serve
 * to create split objects for a specific type of field and expose field stats (min/max).
 * All of the split logic, mainly how to balance splits, is implemented in the base class as balancing is not type specific.
 */
public class NumberFieldShardSplitStrategy extends AbstractFieldShardSplitStrategy<Long> implements Serializable {

  public static Logger log = Logger.getLogger(NumberFieldShardSplitStrategy.class);

  @Override
  protected ShardSplit<Long> createShardSplit(SolrQuery query,
                                              String shardUrl,
                                              String rangeField,
                                              FieldStatsInfo stats,
                                              Long lowerInc,
                                              Long upper)
  {
    Long min = (Long)stats.getMin();
    Long max = (Long)stats.getMax();

    if (lowerInc == null)
      lowerInc = min;

    if (upper == null)
      upper = max;

    return new NumericShardSplit(query, shardUrl, rangeField, min, max, lowerInc, upper);
  }

  @Override
  protected FieldStatsInfo getFieldStatsInfo(SolrClient solrClient, String shardUrl, SolrQuery solrQuery, String splitFieldName) throws IOException, SolrServerException {
    long _startMs = System.currentTimeMillis();

    SolrQuery statsQuery = solrQuery.getCopy();
    statsQuery.setRows(1);
    statsQuery.setStart(0);
    statsQuery.set("distrib", false);
    statsQuery.remove("cursorMark");
    statsQuery.setFields(splitFieldName);
    statsQuery.setSort(splitFieldName, SolrQuery.ORDER.asc);
    QueryResponse qr = solrClient.query(statsQuery);
    SolrDocumentList results = qr.getResults();

    NamedList<Object> nl = new NamedList<Object>();
    long count = qr.getResults().getNumFound();
    if (count != 0) {
      Object o = results.get(0).getFirstValue(splitFieldName);
      long min = Long.parseLong(o.toString());

      // get max value of this field using a top 1 query
      statsQuery.setSort(splitFieldName, SolrQuery.ORDER.desc);
      qr = solrClient.query(statsQuery);
      long max = Long.parseLong(qr.getResults().get(0).getFirstValue(splitFieldName).toString());

      //NamedList<Object> nl = new NamedList<Object>();
      nl.add("min", new Long(min));
      nl.add("max", new Long(max));
      nl.add("count", new Long(count));

      long _diffMs = (System.currentTimeMillis() - _startMs);
      log.info("Took " + _diffMs + " ms to lookup min/max from index for " + splitFieldName + " in shard " + shardUrl);
    }
    else {
      nl.add("count", new Long(count));
      log.info("Cannot get min/max for because no results were produced by the query ");
    }
    return new FieldStatsInfo(nl, splitFieldName);
  }

  class NumericShardSplit extends AbstractShardSplit<Long> {

    NumericShardSplit(SolrQuery query, String shardUrl, String rangeField, Long min, Long max, Long lowerInc, Long upper) {
      super(query, shardUrl, rangeField, min, max, lowerInc, upper);
    }

    @Override
    public Long nextUpper(Long lower, long increment) {
      Long nextUpper = lower + increment;
      // don't go beyond this object's upper
      return nextUpper < upper ? nextUpper : upper;
    }

    @Override
    public long getRange() {
      return (upper - lowerInc);
    }
  }
}
