package com.lucidworks.spark.query;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.FieldStatsInfo;

import java.io.Serializable;
import java.util.Arrays;

public class StringFieldShardSplitStrategy extends AbstractFieldShardSplitStrategy<String> implements Serializable {

  public static Logger log = Logger.getLogger(StringFieldShardSplitStrategy.class);

  public static final char[] alpha =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();
  static {
    // do a lexical sort on the chars in the alpha set
    Arrays.sort(alpha);
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

    return new StringShardSplit(query, shardUrl, rangeField, min, max, lowerInc, upper);
  }

  class StringShardSplit extends AbstractShardSplit<String> {

    StringShardSplit(SolrQuery query, String shardUrl, String rangeField, String min, String max, String lowerInc, String upper) {
      super(query, shardUrl, rangeField, min, max, lowerInc, upper);
    }

    @Override
    public String nextUpper(String lower, long increment) {
      long lowerIndex = getIndex(lower);
      String nextUpper = getKeyAt(lowerIndex + increment);
      // don't go beyond upper though
      return nextUpper.compareTo(upper) < 0 ? nextUpper : upper;
    }

    @Override
    public long getRange() {
      long upperIndex = getIndex(upper);
      long lowerIndex = getIndex(lowerInc);
      return (upperIndex - lowerIndex);
    }

    protected String getKeyAt(long at) {
      // if the "at" is greater than the length of the alpha, then we know we have a two-char key
      if (at < alpha.length)
        throw new IllegalArgumentException("Requested key index must be greater than the length of the alphabet!");

      long rem = at % alpha.length;
      int i = (int)((at - rem) / alpha.length);
      if (i > alpha.length)
        throw new IllegalArgumentException("Key index ("+at+") is out of range! max key is: "+(alpha.length*alpha.length));

      return Character.toString(alpha[i - 1]) + alpha[(int)rem];
    }

    protected long getIndex(String str) {
      long at = 0;
      for (int i=0; i < alpha.length; i++) {
        if (alpha[i] == str.charAt(0)) {
          // found index of first char
          for (int j=0; j < alpha.length; j++) {
            if (alpha[j] == str.charAt(1)) {
              // found index of second char
              at = ((i+1) * alpha.length) + j;
              return at;
            }
          }
        }
      }
      return at;
    }
  }
}
