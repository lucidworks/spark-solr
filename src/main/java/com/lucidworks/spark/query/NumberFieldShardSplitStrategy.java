package com.lucidworks.spark.query;

import com.lucidworks.spark.SolrSupport;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FieldStatsInfo;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NumberFieldShardSplitStrategy extends AbstractFieldShardSplitStrategy implements Serializable {

  public static Logger log = Logger.getLogger(NumberFieldShardSplitStrategy.class);

  public List<ShardSplit> getSplits(String shardUrl,
                                    SolrQuery query,
                                    String splitFieldName,
                                    int splitsPerShard)
      throws IOException, SolrServerException
  {
    List<NumericShardSplit> splits = new ArrayList<NumericShardSplit>(splitsPerShard);

    SolrClient solrClient = SolrSupport.getHttpSolrClient(shardUrl);
    FieldStatsInfo fsi = getFieldStatsInfo(solrClient, splitFieldName);
    log.info("Using stats: "+fsi);

    Object minObj = fsi.getMin();
    Object maxObj = fsi.getMax();
    long min = (minObj != null && minObj instanceof Number) ? ((Number)minObj).longValue() : -1L;
    long max = (maxObj != null && maxObj instanceof Number) ? ((Number)maxObj).longValue() : -1L;
    if (min < 0 || max < 0)
      throw new IllegalStateException("The min/max is not available for "+splitFieldName+
          "! Cannot determine shard splits for "+shardUrl);

    if (min >= max)
      throw new IllegalStateException("Invalid min/max values for "+splitFieldName+
          " in shard "+shardUrl+"! stats: "+fsi);

    // start by creating splits based on max-min/num splits and then refine to balance out split sizes
    appendSplits(solrClient, splits, query, shardUrl, splitFieldName, splitsPerShard, min, max);

    long docsPerSplit = Math.round(fsi.getCount()/splitsPerShard);
    // magic number to allow split sizes to be a little larger than the desired # of docs per split
    long threshold = Math.round(1.18 * docsPerSplit);

    // balance the splits best we can in three passes over the list
    for (int b=0; b < 3; b++) {
      splits = balanceSplits(splits, threshold, docsPerSplit, solrClient);
    }

    // join any small splits, if they aren't adjacent, then just OR the fq's together
    long halfDocsPerSplit = Math.round(threshold/1.8);
    for (int b=0; b < splits.size(); b++) {
      NumericShardSplit ss = splits.get(b);
      if (ss.numHits < halfDocsPerSplit) {
        for (int j=b+1; j < splits.size(); j++) {
          NumericShardSplit tmp = splits.get(j);
          if (tmp.numHits < halfDocsPerSplit) {
            NumericShardSplit nfss = ss.join(tmp);
            splits.set(b, nfss);
            splits.remove(j);
            break;
          }
        }
      }
    }

    // add a final split to catch missing values if any?
    long missingCount = fsi.getMissing();
    if (missingCount > 0) {
      NumericShardSplit missingValuesSplit =
          new NumericShardSplit(query, shardUrl, splitFieldName, "-"+splitFieldName+":[* TO *]");
      missingValuesSplit.setNumHits(missingCount);
      splits.add(missingValuesSplit);
      if (missingCount > (docsPerSplit*2)) {
        log.warn("Found "+missingCount+" missing values for field "+splitFieldName+
            " in shard "+shardUrl+". This can lead to poor performance when processing this shard.");
      }
    }

    List<ShardSplit> retVal = new ArrayList<ShardSplit>(splits.size());
    retVal.addAll(splits);
    return retVal;
  }

  protected List<NumericShardSplit> balanceSplits(List<NumericShardSplit> splits,
                                                   long threshold,
                                                   long docsPerSplit,
                                                   SolrClient solrClient)
      throws IOException, SolrServerException
  {
    List<NumericShardSplit> finalSplits = new ArrayList<NumericShardSplit>(splits.size());
    for (int s=0; s < splits.size(); s++) {
      NumericShardSplit split = splits.get(s);
      long hits = split.numHits;

      if (hits < threshold && s < splits.size()-1) {

        int j = s;
        do {

          long nextJoinSize = split.numHits + splits.get(j+1).numHits;
          if (nextJoinSize > threshold)
            break;

          j += 1;
          split = split.join(splits.get(j));

          if (j+1 == splits.size())
            break;

        } while(split.numHits < threshold);

        finalSplits.add(split);

        s = j;
      } else if (hits > docsPerSplit*1.8) {
        // note, recursion here
        List<NumericShardSplit> reSplitList = reSplit(solrClient, split, docsPerSplit);
        if (reSplitList.size() > 1)
          reSplitList = balanceSplits(reSplitList, threshold, docsPerSplit, solrClient);
        finalSplits.addAll(reSplitList);
      } else {
        finalSplits.add(split);
      }
    }

    return finalSplits;
  }

  protected void appendSplits(SolrClient solrClient,
                              List<NumericShardSplit> splits,
                              SolrQuery query,
                              String shardUrl,
                              String splitFieldName,
                              long splitsPerShard,
                              long min,
                              long max)
      throws IOException, SolrServerException
  {
    long range = max - min;
    long bucketSize = Math.round(range / splitsPerShard);
    long lowerInc = min;
    for (int b = 0; b < splitsPerShard; b++) {
      long upper = (b < splitsPerShard-1) ? lowerInc + bucketSize : max;
      NumericShardSplit split =
          new NumericShardSplit(query, shardUrl, splitFieldName, min, max, lowerInc, upper);
      fetchNumHits(solrClient, split);
      splits.add(split);
      lowerInc = upper;
    }
  }

  protected List<NumericShardSplit> reSplit(SolrClient solrClient, NumericShardSplit split, long docsPerSplit)
      throws IOException, SolrServerException
  {
    long range = split.getRange();
    if (range <= 0)
      return Collections.singletonList(split); // nothing to split

    long numSplits = Math.round(split.numHits / docsPerSplit);
    if (numSplits == 1 && split.numHits > docsPerSplit)
      numSplits = 2;

    List<NumericShardSplit> list = new ArrayList<NumericShardSplit>();
    long bucketSize = Math.round(range / numSplits);
    long lowerInc = split.lowerInc;
    for (int b = 0; b < numSplits; b++) {
      long upper = (b < numSplits-1) ? lowerInc + bucketSize : split.upper;
      NumericShardSplit sub =
          new NumericShardSplit(split.query, split.shardUrl, split.rangeField, split.min, split.max, lowerInc, upper);
      fetchNumHits(solrClient, sub);
      list.add(sub);
      lowerInc = upper;
    }

    return list;
  }

  class NumericShardSplit implements ShardSplit, Serializable {
    private SolrQuery query;
    private String shardUrl;
    private String rangeField;
    private Long numHits;
    private String fq;
    private long lowerInc;
    private long upper;
    private long min;
    private long max;

    NumericShardSplit(SolrQuery query, String shardUrl, String rangeField, long min, long max, long lowerInc, long upper) {
      this.query = query;
      this.shardUrl = shardUrl;
      this.rangeField = rangeField;
      this.lowerInc = lowerInc;
      this.upper = upper;
      this.min = min;
      this.max = max;
      this.fq = buildSplitFq();
    }

    NumericShardSplit(SolrQuery query, String shardUrl, String rangeField, String fq) {
      this.query = query;
      this.shardUrl = shardUrl;
      this.rangeField = rangeField;
      this.fq = fq;
      this.lowerInc = -1L;
    }

    public String getShardUrl() {
      return shardUrl;
    }

    public Long getNumHits() {
      return numHits;
    }

    public void setNumHits(Long numHits) {
      this.numHits = numHits;
    }

    public SolrQuery getSplitQuery() {
      SolrQuery splitQuery = query.getCopy();
      splitQuery.addFilterQuery(fq);
      return splitQuery;
    }

    private long getRange() {
      return upper - lowerInc;
    }

    private String buildSplitFq() {
      StringBuilder sb = new StringBuilder();
      String lowerClause = (min == lowerInc) ? "[*" : "["+lowerInc;
      String upperClause = (max == upper) ? "*]" : upper+"}";
      sb.append(rangeField).append(":").append(lowerClause).append(" TO ").append(upperClause);
      return sb.toString();
    }

    NumericShardSplit join(NumericShardSplit other) {
      if (lowerInc == -1L || other.lowerInc == -1L) {
        NumericShardSplit joined = new NumericShardSplit(query, shardUrl, rangeField, this.fq + " OR " + other.fq);
        joined.setNumHits(this.numHits + other.numHits);
        return joined;
      }

      NumericShardSplit small = (other.lowerInc < lowerInc) ? other : this;
      NumericShardSplit big = (other.lowerInc < lowerInc) ? this : other;
      NumericShardSplit joined;
      if (small.upper == big.lowerInc) {
        // the splits are adjacent
        joined = new NumericShardSplit(query, shardUrl, rangeField, min, max, small.lowerInc, big.upper);
      } else {
        // the splits are not adjacent, so only option is to OR the fq's
        joined = new NumericShardSplit(query, shardUrl, rangeField, small.fq + " OR " + big.fq);
      }
      joined.setNumHits(this.numHits + other.numHits);
      return joined;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(fq).append(" (").append((numHits != null) ? numHits.toString() : "?").append(")");
      return sb.toString();
    }
  }
}
