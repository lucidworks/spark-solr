package com.lucidworks.spark.query;

import com.lucidworks.spark.SolrSupport;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FieldStatsInfo;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class StringFieldShardSplitStrategy extends AbstractFieldShardSplitStrategy implements Serializable {

  public static Logger log = Logger.getLogger(StringFieldShardSplitStrategy.class);

  public static final String alphaChars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  public List<ShardSplit> getSplits(String shardUrl, SolrQuery query, String splitFieldName, int numSplits)
      throws IOException, SolrServerException
  {
    List<StringShardSplit> splits = new ArrayList<StringShardSplit>(numSplits);

    SolrClient solrClient = SolrSupport.getHttpSolrClient(shardUrl);
    FieldStatsInfo fsi = getFieldStatsInfo(solrClient, splitFieldName);
    log.info("Using stats: "+fsi);

    String min = (String)fsi.getMin();
    String max = (String)fsi.getMax();

    char[] alpha = alphaChars.toCharArray();
    // do a lexical sort on the chars in the alpha set
    Arrays.sort(alpha);

    int minFC = -1;
    int maxFC = -1;
    for (int i=0; i < alpha.length; i++) {
      if (alpha[i] == min.charAt(0)) {
        minFC = i;
        break;
      }
    }
    if (minFC == -1)
      throw new IllegalArgumentException("Cannot split the "+splitFieldName+
          " field because min value '"+min+"' starts with "+min.charAt(0));

    for (int i=0; i < alpha.length; i++) {
      if (alpha[i] == max.charAt(0)) {
        maxFC = i;
        break;
      }
    }
    if (maxFC == -1)
      throw new IllegalArgumentException("Cannot split the "+splitFieldName+
          " field because max value '"+max+"' starts with "+max.charAt(0));

    alpha = Arrays.copyOfRange(alpha, minFC, maxFC+1);

    long minAt = getIndex(alpha, min);
    long maxAt = getIndex(alpha, max);

    long span = (maxAt - minAt);
    long bucketSize = Math.round(span / numSplits);

    long lowerIndex = minAt;
    String lowerInc = min;
    for (int b = 0; b < numSplits; b++) {
      String upper = (b < numSplits-1) ? getKeyAt(alpha, lowerIndex + bucketSize) : max;
      boolean upperExc = !upper.equals(max);
      StringShardSplit ss = new StringShardSplit(query, shardUrl, splitFieldName, lowerInc, upper, upperExc);
      fetchNumHits(solrClient, ss);
      splits.add(ss);
      lowerInc = upper;
      lowerIndex = getIndex(alpha, lowerInc);
    }

    if (fsi.getMissing() > 0) {
      // missing values split
      splits.add(new StringShardSplit(query, shardUrl, splitFieldName, null, null, false));
    }

    List<ShardSplit> retVal = new ArrayList<ShardSplit>();
    retVal.addAll(splits);
    return retVal;
  }

  protected String getKeyAt(char[] alpha, long at) {

    int floor = (int)Math.floor((double)at/alpha.length);
    String key = String.valueOf(alpha[floor]);

    long tmp = floor * alpha.length;
    long diff = at - tmp;

    key += alpha[(int)diff];

    return key;
  }
  
  protected long getIndex(char[] alpha, String str) {
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

  class StringShardSplit implements ShardSplit, Serializable {
    protected SolrQuery query;
    protected String shardUrl;
    protected String rangeField;
    protected String lowerInc;
    protected String upper;
    protected Long numHits = null;
    protected String fq;
    protected boolean upperExc;

    StringShardSplit(SolrQuery query, String shardUrl, String rangeField, String lowerInc, String upper, boolean upperExc) {
      this.query = query;
      this.shardUrl = shardUrl;
      this.rangeField = rangeField;
      this.lowerInc = lowerInc;
      this.upper = upper;
      this.upperExc = upperExc;
      this.fq = buildSplitFq();
    }

    StringShardSplit(SolrQuery query, String shardUrl, String rangeField, String fq) {
      this.query = query;
      this.shardUrl = shardUrl;
      this.rangeField = rangeField;
      this.fq = fq;
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

    protected String buildSplitFq() {
      StringBuilder sb = new StringBuilder();
      if (lowerInc != null) {
        String exc = upperExc ? "}" : "]";
        sb.append(rangeField).append(":[").append(lowerInc).append(" TO ").append(upper).append(exc);
      } else {
        // split criteria to catch missing values
        sb.append("-").append(rangeField).append(":[* TO *]");
      }
      return sb.toString();
    }

    public SolrQuery getSplitQuery() {
      SolrQuery splitQuery = query.getCopy();
      splitQuery.addFilterQuery(fq);
      return splitQuery;
    }

    public StringShardSplit join(StringShardSplit other) {
      if (lowerInc == null || other.lowerInc == null) {
        throw new IllegalStateException("Cannot join missing values split with other splits!");
      }

      if (!rangeField.equals(other.rangeField)) {
        throw new IllegalStateException("Cannot join splits for different fields! "+rangeField+" != "+other.rangeField);
      }

      StringShardSplit joined = null;
      String lowerStr = (String)lowerInc;
      String otherLower = (String)other.lowerInc;
      if (otherLower.compareTo(lowerStr) < 0) {
        joined = new StringShardSplit(query, shardUrl, rangeField, other.lowerInc, this.upper, this.upperExc);
      } else {
        joined = new StringShardSplit(query, shardUrl, rangeField, this.lowerInc, other.upper, other.upperExc);
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
