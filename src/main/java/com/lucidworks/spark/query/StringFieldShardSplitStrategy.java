package com.lucidworks.spark.query;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FieldStatsInfo;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class StringFieldShardSplitStrategy extends AbstractFieldShardSplitStrategy<String> implements Serializable {

  public static Logger log = Logger.getLogger(StringFieldShardSplitStrategy.class);

  public static final char[] supportedChars =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();
  static {
    // do a lexical sort on the chars in the alpha set
    Arrays.sort(supportedChars);
  }

  @Override
  protected ShardSplit<String> createShardSplit(SolrQuery query, String shardUrl, String rangeField, FieldStatsInfo stats, String lowerInc, String upper) {

    String min = (String)stats.getMin();
    String max = (String)stats.getMax();

    if (lowerInc == null) {
      lowerInc = min;
    }

    if (upper == null) {
      upper = max;
    }

    int minFC = -1;
    int maxFC = -1;
    for (int i=0; i < supportedChars.length; i++) {
      if (supportedChars[i] == min.charAt(0)) {
        minFC = i;
        break;
      }
    }
    if (minFC == -1)
      throw new IllegalArgumentException("Cannot split the "+rangeField+
          " field because min value '"+min+"' starts with "+min.charAt(0));

    for (int i=0; i < supportedChars.length; i++) {
      if (supportedChars[i] == max.charAt(0)) {
        maxFC = i;
        break;
      }
    }
    if (maxFC == -1)
      throw new IllegalArgumentException("Cannot split the "+rangeField+
          " field because max value '"+max+"' starts with "+max.charAt(0));

    char[] alpha = Arrays.copyOfRange(supportedChars, minFC, maxFC+1);

    return new StringShardSplit(alpha, query, shardUrl, rangeField, min, max, lowerInc, upper);
  }

  protected static String getKeyAt(char[] alpha, long at) {

    int floor = (int)Math.floor((double)at/alpha.length);
    String key = String.valueOf(alpha[floor]);

    long tmp = floor * alpha.length;
    long diff = at - tmp;

    key += alpha[(int)diff];

    return key;
  }

  protected static long getIndex(char[] alpha, String str) {
    long at = 0;
    for (int i=0; i < alpha.length; i++) {
      if (alpha[i] == str.charAt(0)) {
        // found index of first char
        for (int j=0; j < alpha.length; j++) {
          if (alpha[j] == str.charAt(1)) {
            // found index of second char
            at = i * alpha.length + j;
            return at;
          }
        }
      }
    }
    return at;
  }

  class StringShardSplit extends AbstractShardSplit<String> {

    protected char[] alpha;

    StringShardSplit(char[] alpha, SolrQuery query, String shardUrl, String rangeField, String min, String max, String lowerInc, String upper) {
      super(query, shardUrl, rangeField, min, max, lowerInc, upper);
      this.alpha = alpha;
    }

    public List<ShardSplit> reSplit(SolrClient solrClient, long docsPerSplit) throws IOException, SolrServerException {
      List<ShardSplit> splits = new ArrayList<ShardSplit>();

      long upperIndex = getIndex(alpha, upper);
      long lowerIndex = getIndex(alpha, lowerInc);
      long range = (upperIndex - lowerIndex);
      if (range <= 0) {
        splits.add(this); // nothing to split
        return splits;
      }

      int numSplits = Math.round(getNumHits() / docsPerSplit);
      if (numSplits == 1 && getNumHits() > docsPerSplit)
        numSplits = 2;

      long bucketSize = Math.round(range / numSplits);
      String lowerBound = lowerInc;

      for (int b = 0; b < numSplits; b++) {
        String upperBound = (b < numSplits-1) ? getKeyAt(alpha, lowerIndex + bucketSize) : upper;
        StringShardSplit sub =
            new StringShardSplit(alpha, query, shardUrl, rangeField, min, max, lowerBound, upperBound);
        sub.fetchNumHits(solrClient);
        splits.add(sub);
        lowerBound = upperBound;
        lowerIndex = getIndex(alpha, lowerBound);
      }

      return splits;
    }
  }
}
