package com.lucidworks.spark;


import com.lucidworks.spark.rdd.SolrRDD;
import com.lucidworks.spark.util.ScalaUtil;
import com.lucidworks.spark.util.SolrQuerySupport;
import com.lucidworks.spark.util.SolrSchemaUtil;
import com.lucidworks.spark.util.SolrSupport;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import static com.lucidworks.spark.util.ConfigurationConstants.SOLR_COLLECTION_PARAM;
import static com.lucidworks.spark.util.ConfigurationConstants.SOLR_ZK_HOST_PARAM;

public class SolrRelation extends BaseRelation implements Serializable, TableScan, PrunedFilteredScan, InsertableRelation {

  public static Logger log = Logger.getLogger(SolrRelation.class);

  protected transient SolrQuery solrQuery;
  protected SolrRDD solrRDD;
  protected StructType baseSchema;
  protected StructType schema;
  protected transient SQLContext sqlContext;
  protected transient SparkContext sc;
  protected transient final SolrConf solrConf;

  public SolrRelation(SQLContext sqlContext, scala.collection.immutable.Map<String, String> config) throws Exception {
    this(sqlContext, config, null);
  }

  public SolrRelation(SQLContext sqlContext, scala.collection.immutable.Map<String, String> config, DataFrame dataFrame) throws Exception {
    if (sqlContext == null)
      throw new IllegalArgumentException("SQLContext cannot be null!");

    this.sqlContext = sqlContext;
    this.sc = sqlContext.sparkContext();
    this.solrConf = new SolrConf(sc.getConf(), config);

    String zkHost = ScalaUtil.requiredParam(config, SOLR_ZK_HOST_PARAM);
    String collection = ScalaUtil.requiredParam(config, SOLR_COLLECTION_PARAM);

    solrRDD = new SolrRDD(zkHost, collection, sc);

    this.baseSchema = SolrSchemaUtil.getBaseSchema(zkHost, collection, solrConf.escapeFieldNames());
    this.solrQuery = buildQuery();

    if (dataFrame != null) {
      this.schema = dataFrame.schema();
    } else if (solrQuery.getFields() != null && solrQuery.getFields().length() > 0) {
      this.schema = SolrSchemaUtil.deriveQuerySchema(solrQuery.getFields().split(","), this.baseSchema);
    } else {
      this.schema = SolrSchemaUtil.getQuerySchema(solrQuery, this.baseSchema);
    }
  }

  private SolrQuery buildQuery() {
    SolrQuery query = SolrQuerySupport.toQuery(solrConf.getQuery());
    String[] fieldList = solrConf.getFieldList();

    if (fieldList.length > 0) {
      query.setFields(fieldList);
    } else {
      SolrSchemaUtil.applyDefaultFields(baseSchema, query);
    }

    query.setRows(solrConf.getRows());
    query.add(solrConf.getSolrParams());
    query.set("collection", solrRDD.getCollection());

    return query;
  }

  @Override
  public RDD<Row> buildScan() {
    return buildScan(null, null);
  }

  @Override
  public RDD<Row> buildScan(String[] fields, Filter[] filters) {
    // Schema preserving data frames return all fields by default
    if (solrConf.preserveSchema()) {
      SolrSchemaUtil.applyDefaultFields(baseSchema, solrQuery);
    } else {
      log.info("Building Solr scan using fields=" + (fields != null ? Arrays.asList(fields).toString() : ""));
      if (fields != null && fields.length > 0) {
        solrQuery.setFields(fields);
      }
    }

    // clear all existing filters
    if (filters != null && filters.length > 0) {
      solrQuery.remove("fq");
      log.info("Building SolrQuery using filters: " + Arrays.asList(filters));
      for (Filter filter : filters)
        SolrSchemaUtil.applyFilter(filter, solrQuery, baseSchema);
    }

    if (log.isInfoEnabled())
      log.info("Constructed SolrQuery: " + solrQuery);

    RDD<Row> rows = null;
    try {
      StructType querySchema = (fields != null && fields.length > 0) ? SolrSchemaUtil.deriveQuerySchema(fields, baseSchema) : schema;
      JavaRDD<SolrDocument> docs = solrRDD.queryShards(solrQuery);
  //    log.info("The docs are " + docs.collect());
      rows = SolrSchemaUtil.toRows(querySchema, docs).rdd();
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      else {
        throw new RuntimeException(e);
      }
    }

    return rows;
  }

  @Override
  public SQLContext sqlContext() {
    return sqlContext;
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public void insert(DataFrame df, boolean overwrite) {
    JavaRDD<SolrInputDocument> docs = null;
    docs = df.javaRDD().map(new Function<Row, SolrInputDocument>() {
      public SolrInputDocument call(Row row) throws Exception {
        StructType schema = row.schema();
        SolrInputDocument doc = new SolrInputDocument();
        for (StructField f : schema.fields()) {
          String fname = f.name();
          if (fname.equals("_version_"))
            continue;

          int fieldIndex = row.fieldIndex(fname);
          Object val = row.isNullAt(fieldIndex) ? null : row.get(fieldIndex);
          if (val != null) {
            if (val instanceof Collection) {
              Collection c = (Collection) val;
              Iterator i = c.iterator();
              while (i.hasNext())
                doc.addField(fname, i.next());
            } else if (val instanceof scala.collection.mutable.ArrayBuffer) {
              scala.collection.Iterator iter =
                ((scala.collection.mutable.ArrayBuffer)val).iterator();
              while (iter.hasNext())
                doc.addField(fname, iter.next());
            } else if (val instanceof scala.collection.mutable.WrappedArray) {
              scala.collection.Iterator iter =
                ((scala.collection.mutable.WrappedArray) val).iterator();
              while (iter.hasNext())
                doc.addField(fname, iter.next());
            } else {
              doc.setField(fname, val);
            }
          }
        }
        return doc;
      }
    });

    SolrSupport.indexDocs(solrRDD.getZKHost(), solrRDD.getCollection(), 100, docs);
  }
}
