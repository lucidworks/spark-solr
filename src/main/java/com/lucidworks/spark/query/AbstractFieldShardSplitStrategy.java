package com.lucidworks.spark.query;

import com.lucidworks.spark.SolrSupport;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.QueryResponse;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractFieldShardSplitStrategy<T> implements ShardSplitStrategy, Serializable {

  public static Logger log = Logger.getLogger(AbstractFieldShardSplitStrategy.class);

  // these are magic number used in the split calculations below,
  // they were determined heuristically to generate relatively balanced split sizes
  protected static final double thresholdFactor = 1.18d;

  // any splits that are above this threshold will generate a warning to the user so they know they aren't getting optimal performance for this split
  // and that they might want to increase the number of splits per shard
  protected static final double splitSizeWarnThreshold = 1.40d;

  // as this value approaches 1.0, the variance between the splits gets smaller with fewer/smaller outliers but it takes longer to calculate them
  // as it approaches 2.0, the variance grows and the outliers get bigger, but splits can be calculated with fewer refinement queries into Solr
  protected static final double resplitThreshold = 1.5d;

  // should be around 2.0, used when trying to join non-adjacent splits that are nearly half the size of the docsPerSplit threshold
  protected static final double smallDocsFactor = 1.8d;

  public List<ShardSplit> getSplits(String shardUrl,
                                    SolrQuery query,
                                    String splitFieldName,
                                    int splitsPerShard)
      throws IOException, SolrServerException
  {
    long _startMs = System.currentTimeMillis();

    List<ShardSplit> splits = new ArrayList<ShardSplit>(splitsPerShard);

    try (SolrClient solrClient = SolrSupport.getHttpSolrClient(shardUrl)) {
      // start by creating splits based on max-min/num splits and then refine to balance out split sizes
      FieldStatsInfo fsi = appendSplits(solrClient, splits, query, shardUrl, splitFieldName, splitsPerShard);

      long docsPerSplit = Math.round(fsi.getCount() / splitsPerShard);

      // magic number to allow split sizes to be a little larger than the desired # of docs per split
      long threshold = Math.round(thresholdFactor * docsPerSplit);

      // balance the splits best we can in three passes over the list
      for (int b = 0; b < 3; b++) {
        splits = balanceSplits(splits, threshold, docsPerSplit, solrClient, fsi, splitsPerShard, 0);
      }

      // lastly, scan for any small splits that aren't adjacent that can be joined into a larger split
      joinNonAdjacentSmallSplits(fsi, splits, threshold);

      // add a final split to catch missing values if any?
      Long missingCount = fsi.getMissing();
      if (missingCount == null) {
        SolrQuery missingQuery = query.getCopy();
        missingQuery.addFilterQuery("-" + splitFieldName + ":[* TO *]");
        missingQuery.setRows(0);
        QueryResponse qr = solrClient.query(missingQuery);
        missingCount = qr.getResults().getNumFound();
      }

      if (missingCount > 0) {
        ShardSplit missingValuesSplit =
            new FqSplit(query, shardUrl, splitFieldName, "-"+splitFieldName+":[* TO *]");
        missingValuesSplit.setNumHits(missingCount);
        splits.add(missingValuesSplit);
        if (missingCount > (docsPerSplit*2)) {
          log.warn("Found "+missingCount+" missing values for field "+splitFieldName+
              " in shard "+shardUrl+". This can lead to poor performance when processing this shard.");
        }
      }
    }

    long _diffMs = (System.currentTimeMillis() - _startMs);

    long total = 0L;
    for (ShardSplit ss : splits) {
      total += ss.getNumHits();
    }

    long avg = Math.round((double) total / splits.size());
    log.info("Took " + _diffMs + " ms to find " + splits.size() + " splits for " +
        splitFieldName + " with avg size: " + avg + ", total: " + total);
    // warn the user about any large outliers
    long high = Math.round(avg * splitSizeWarnThreshold);
    for (int s=0; s < splits.size(); s++) {
      ShardSplit ss = splits.get(s);
      long numHits = ss.getNumHits();
      if (numHits > high) {
        double p = (double) numHits / avg - 1d;
        long pct = Math.round(p * 100);
        log.warn("Size of split " + s + " " + ss + " is " + pct + "% larger than the avg split size " + avg +
            "; this could lead to sub-optimal job execution times.");
      }
    }

    return splits;
  }

  protected FieldStatsInfo getFieldStatsInfo(SolrClient solrClient, String shardUrl, SolrQuery solrQuery, String splitFieldName) throws IOException, SolrServerException {
    SolrQuery statsQuery = solrQuery.getCopy();
    statsQuery.setRows(0);
    statsQuery.setStart(0);
    statsQuery.set("distrib", false);
    statsQuery.remove("cursorMark");
    statsQuery.setGetFieldStatistics(splitFieldName);
    QueryResponse qr = solrClient.query(statsQuery);
    return qr.getFieldStatsInfo().get(splitFieldName);
  }

  public Long fetchNumHits(SolrClient solrClient, ShardSplit split) throws IOException, SolrServerException {
    SolrQuery splitQuery = split.getQuery().getCopy();
    splitQuery.addFilterQuery(split.getSplitFilterQuery());
    splitQuery.setRows(0);
    splitQuery.setStart(0);
    QueryResponse qr = solrClient.query(splitQuery);
    split.setNumHits(qr.getResults().getNumFound());
    return split.getNumHits();
  }

  /**
   * Finds min/max of the split field and then builds splits that span the range.
   */
  protected FieldStatsInfo appendSplits(SolrClient solrClient,
                                        List<ShardSplit> splits,
                                        SolrQuery query,
                                        String shardUrl,
                                        String splitFieldName,
                                        int splitsPerShard)
      throws IOException, SolrServerException
  {
    FieldStatsInfo fsi = getFieldStatsInfo(solrClient, shardUrl, query, splitFieldName);
    if (fsi.getMin() == null || fsi.getMax() == null) {
      throw new IllegalStateException("No min/max for " + splitFieldName +
          "! Check your Solr index to verify if "+splitFieldName+" has values.");
    }
    log.info("Using stats: " + fsi);

    // we just build a single big split and then call resplit on it
    ShardSplit firstSplit =
        createShardSplit(query, shardUrl, splitFieldName, fsi, null, null);
    long numHits = fsi.getCount();
    firstSplit.setNumHits(numHits);
    long docsPerSplit = Math.round(numHits / splitsPerShard);
    splits.addAll(reSplit(solrClient, firstSplit, docsPerSplit, fsi));
    return fsi;
  }

  protected List<ShardSplit> balanceSplits(List<ShardSplit> splits,
                                           long threshold,
                                           long docsPerSplit,
                                           SolrClient solrClient,
                                           FieldStatsInfo fsi,
                                           int splitsPerShard,
                                           int depth)
      throws IOException, SolrServerException
  {
    List<ShardSplit> finalSplits = new ArrayList<ShardSplit>(splits.size());
    for (int s=0; s < splits.size(); s++) {
      ShardSplit split = splits.get(s);
      long hits = split.getNumHits();

      if (hits < threshold && s < splits.size()-1) {

        int j = s;
        do {

          long nextJoinSize = split.getNumHits() + splits.get(j+1).getNumHits();
          if (nextJoinSize > threshold)
            break;

          j += 1;
          split = join(fsi, split, splits.get(j));

          if (j+1 == splits.size())
            break;

        } while(split.getNumHits() < threshold);

        finalSplits.add(split);

        s = j;
      } else if (depth <= 20 && hits > docsPerSplit*resplitThreshold) {
        // note, recursion here ... use depth to cut it off when there is a very large outlier that won't converge

        // when we re-split a big split, we want to over-split a little bit (small splits will get combined cheaply)
        long resplitDocsPerSplit = (splitsPerShard <= 4) ? Math.round(docsPerSplit * 0.6) : docsPerSplit;
        List<ShardSplit> reSplitList = reSplit(solrClient, split, resplitDocsPerSplit, fsi);
        if (reSplitList.size() > 1) {
          reSplitList = balanceSplits(reSplitList, threshold, docsPerSplit, solrClient, fsi, splitsPerShard, ++depth);
        }
        finalSplits.addAll(reSplitList);
      } else {
        finalSplits.add(split);
      }
    }

    return finalSplits;
  }

  public List<ShardSplit> reSplit(SolrClient solrClient, ShardSplit<T> toBeSplit, long docsPerSplit, FieldStatsInfo fsi) throws IOException, SolrServerException {

    List<ShardSplit> list = new ArrayList<ShardSplit>();
    long range = toBeSplit.getRange();
    if (range <= 0) {
      list.add(toBeSplit); // nothing to split
      return list;
    }

    long _startMs = System.currentTimeMillis();

    int numSplits = Math.round(toBeSplit.getNumHits() / docsPerSplit);
    if (numSplits == 1 && toBeSplit.getNumHits() > docsPerSplit)
      numSplits = 2;

    long bucketSize = Math.round((double)range / numSplits);
    if (bucketSize < 1) {
      list.add(toBeSplit); // bucket size is too small ... nothing to split
      return list;
    }

    T lowerBound = toBeSplit.getLowerInc();
    for (int b = 0; b < numSplits; b++) {
      T upperBound = (b < numSplits-1) ? toBeSplit.nextUpper(lowerBound, bucketSize) : toBeSplit.getUpper();
      ShardSplit<T> sub =
          createShardSplit(toBeSplit.getQuery(), toBeSplit.getShardUrl(), toBeSplit.getSplitFieldName(), fsi, lowerBound, upperBound);
      fetchNumHits(solrClient, sub);
      list.add(sub);

      if (b < numSplits-1 && upperBound.equals(toBeSplit.getUpper())) {
        // we hit the max too soon, break out of this loop
        break;
      }

      lowerBound = upperBound;
    }

    long _diffMs = (System.currentTimeMillis() - _startMs);
    log.info("Took "+_diffMs+" ms to re-split "+toBeSplit.toString()+" into "+
        list.size()+" sub-splits to achieve "+docsPerSplit+" docs per split");

    return list;
  }

  protected void joinNonAdjacentSmallSplits(FieldStatsInfo stats, List<ShardSplit> splits, long threshold) {
    // join any small splits, if they aren't adjacent, then just OR the fq's together
    long halfDocsPerSplit = Math.round(threshold/smallDocsFactor);
    for (int b=0; b < splits.size(); b++) {
      ShardSplit ss = splits.get(b);
      if (ss.getNumHits() < halfDocsPerSplit) {
        for (int j=b+1; j < splits.size(); j++) {
          ShardSplit tmp = splits.get(j);
          if (tmp.getNumHits() < halfDocsPerSplit) {
            splits.set(b, join(stats, ss, tmp));
            splits.remove(j);
            break;
          }
        }
      }
    }
  }

  public ShardSplit join(FieldStatsInfo stats, ShardSplit lhs, ShardSplit rhs) {
    ShardSplit joined = null;
    
    if (lhs.getLowerInc() != null && rhs.getLowerInc() != null) {
      String lhsLowerInc = String.valueOf(lhs.getLowerInc());
      String rhsLowerInc = String.valueOf(rhs.getLowerInc());
      ShardSplit<T> small = (rhsLowerInc.compareTo(lhsLowerInc) < 0) ? rhs : lhs;
      ShardSplit<T> big = (small == rhs) ? lhs : rhs;
      if (small.getUpper().equals(big.getLowerInc())) {
        // these splits are adjacent
        joined = createShardSplit(lhs.getQuery(), lhs.getShardUrl(), lhs.getSplitFieldName(), stats, small.getLowerInc(), big.getUpper());
      }
    }

    if (joined == null) {
      String fq = lhs.getSplitFilterQuery() + " OR "+ rhs.getSplitFilterQuery();
      joined = new FqSplit(lhs.getQuery(), lhs.getShardUrl(), lhs.getSplitFieldName(), fq);
    }

    joined.setNumHits(lhs.getNumHits() + rhs.getNumHits());

    return joined;
  }

  // used internally for splits that just have an explicit FQ,
  // such as one created by OR'ing together two non-adjacent splits
  class FqSplit extends AbstractShardSplit<String> {
    FqSplit(SolrQuery query, String shardUrl, String rangeField, String fq) {
      super(query, shardUrl, rangeField, fq);
    }

    @Override
    public String nextUpper(String lower, long increment) {
      return null;
    }

    @Override
    public long getRange() {
      return 0;
    }
  }

  protected abstract ShardSplit<T> createShardSplit(SolrQuery query,
                                                    String shardUrl,
                                                    String rangeField,
                                                    FieldStatsInfo stats,
                                                    T lowerInc,
                                                    T upper);
}
