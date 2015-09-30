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
import java.util.ArrayList;
import java.util.List;

/**
 * Implements a shard splitting strategy for integral fields (long / integer).
 */
public class NumberFieldShardSplitStrategy extends AbstractFieldShardSplitStrategy<Long> implements Serializable {

  public static Logger log = Logger.getLogger(NumberFieldShardSplitStrategy.class);

  @Override
  protected ShardSplit<Long> createShardSplit(SolrQuery query,
                                              String shardUrl,
                                              String rangeField,
                                              FieldStatsInfo stats, Long lowerInc, Long upper) {

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
    if (results.getNumFound() == 0)
      throw new IllegalStateException("Cannot get min/max for "+splitFieldName+" from "+shardUrl+"!");

    long min = ((Long)results.get(0).getFirstValue(splitFieldName)).longValue();
    long count = qr.getResults().getNumFound();

    // get max value of this field using a top 1 query
    statsQuery.setSort(splitFieldName, SolrQuery.ORDER.desc);
    qr = solrClient.query(statsQuery);
    long max = ((Long)qr.getResults().get(0).getFirstValue(splitFieldName)).longValue();

    NamedList<Object> nl = new NamedList<Object>();
    nl.add("min", new Long(min));
    nl.add("max", new Long(max));
    nl.add("count", new Long(count));

    long _diffMs = (System.currentTimeMillis() - _startMs);
    log.info("Took " + _diffMs + " ms to lookup min/max from index for " + splitFieldName + " in shard " + shardUrl);

    return new FieldStatsInfo(nl, splitFieldName);
  }

  class NumericShardSplit extends AbstractShardSplit<Long> {

    NumericShardSplit(SolrQuery query, String shardUrl, String rangeField, Long min, Long max, Long lowerInc, Long upper) {
      super(query, shardUrl, rangeField, min, max, lowerInc, upper);
    }

    public List<ShardSplit> reSplit(SolrClient solrClient, long docsPerSplit) throws IOException, SolrServerException {

      int numSplits = Math.round(getNumHits() / docsPerSplit);
      if (numSplits == 1 && getNumHits() > docsPerSplit)
        numSplits = 2;

      List<ShardSplit> list = new ArrayList<ShardSplit>();
      long range = (upper - lowerInc);
      if (range <= 0) {
        list.add(this); // nothing to split
        return list;
      }

      long bucketSize = Math.round(range / numSplits);
      long lowerBound = this.lowerInc;
      for (int b = 0; b < numSplits; b++) {
        long upperBound = (b < numSplits-1) ? lowerBound + bucketSize : this.upper;
        NumericShardSplit sub =
            new NumericShardSplit(this.query, this.shardUrl, this.rangeField, this.min, this.max, lowerBound, upperBound);
        sub.fetchNumHits(solrClient);
        list.add(sub);
        lowerBound = upperBound;
      }

      return list;
    }
  }
}
