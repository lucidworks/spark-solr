package com.lucidworks.spark.query;

import org.apache.spark.mllib.feature.HashingTF;
import org.junit.Test;

import static org.junit.Assert.*;

public class SolrTermVectorTest {

  @Test
  public void testTextAnalysis() throws Exception {
    HashingTF hashingTF = new HashingTF(100);
    String rawText = "the quick brown fox jumped over the lazy dog";
    SolrTermVector tv = SolrTermVector.newInstance("1", hashingTF, rawText);
    assertNotNull(tv);
    assertTrue("Expected 7 terms but found "+tv.terms.length, tv.terms.length == 7);
    assertEquals("quick", tv.terms[0]);
    assertEquals("dog", tv.terms[6]);
  }
}
