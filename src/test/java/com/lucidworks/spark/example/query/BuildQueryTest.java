package com.lucidworks.spark.example.query;

import com.lucidworks.spark.util.SolrQuerySupport;
import org.apache.solr.client.solrj.SolrQuery;
import org.junit.Test;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static com.lucidworks.spark.util.QueryConstants.*;

public class BuildQueryTest {

  @Test
  public void testQueryBuilder() {
    SolrQuery q = null;

    q = SolrQuerySupport.toQuery(null);
    assertEquals("*:*", q.getQuery());
    assertEquals(new Integer(DEFAULT_PAGE_SIZE()), q.getRows());

    q = SolrQuerySupport.toQuery("q=*:*") ;
    assertEquals("*:*", q.getQuery());
    assertEquals(new Integer(DEFAULT_PAGE_SIZE()), q.getRows());

    q = SolrQuerySupport.toQuery("q={!geofilt sfield=geo_location pt=44.9609,-93.2642 d=50}") ;
    assertEquals("{!geofilt sfield=geo_location pt=44.9609,-93.2642 d=50}", q.getQuery());

    q = SolrQuerySupport.toQuery("{!geofilt sfield=geo_location pt=44.9609,-93.2642 d=50}") ;
    assertEquals("{!geofilt sfield=geo_location pt=44.9609,-93.2642 d=50}", q.getQuery());

    String qs = "text:hello";
    String fq = "price:[100 TO *]";
    String sort = "id";
    q = SolrQuerySupport.toQuery("q="+encode(qs)+"&fq="+encode(fq)+"&sort="+sort);
    assertEquals(qs, q.getQuery());
    assertEquals(new Integer(DEFAULT_PAGE_SIZE()), q.getRows());
    assertTrue(q.getFilterQueries().length == 1);
    assertEquals(fq, q.getFilterQueries()[0]);
    List<SolrQuery.SortClause> sorts = q.getSorts();
    assertNotNull(sorts);
    assertTrue(sorts.size() == 1);
    SolrQuery.SortClause sortClause = sorts.get(0);
    assertEquals(SolrQuery.SortClause.create("id","asc"), sortClause);
  }

  private String encode(String val) {
    try {
      return URLEncoder.encode(val, StandardCharsets.UTF_8.name());
    } catch (java.io.UnsupportedEncodingException uee) {
      throw new RuntimeException(uee);
    }
  }
}
