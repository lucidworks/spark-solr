package com.lucidworks.spark;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;

public class SolrRelation extends BaseRelation implements Serializable, TableScan, PrunedFilteredScan, InsertableRelation {

  public static Logger log = Logger.getLogger(SolrRelation.class);

  public static String SOLR_ZK_HOST_PARAM = "zkhost";
  public static String SOLR_COLLECTION_PARAM = "collection";
  public static String SOLR_QUERY_PARAM = "query";
  public static String SOLR_FIELD_LIST_PARAM = "fields";
  public static String SOLR_ROWS_PARAM = "rows";
  public static String SOLR_SPLIT_FIELD_PARAM = "split_field";
  public static String SOLR_SPLITS_PER_SHARD_PARAM = "splits_per_shard";

  public static String SOLR_PARALLEL_SHARDS = "parallel_shards";

  protected String[] fieldList;
  public static String PRESERVE_SCHEMA = "preserveschema";
  protected Boolean preserveSchema = false;
  protected String splitFieldName;
  protected int splitsPerShard = 1;
  protected SolrQuery solrQuery;
  protected SolrRDD solrRDD;
  protected StructType schema;
  protected ModifiableSolrParams addlSolrParams;
  protected boolean parallelShards = true;
  protected transient SQLContext sqlContext;

  protected Integer rows = null;

  protected transient JavaSparkContext sc;


  public SolrRelation() {}

  public SolrRelation(SQLContext sqlContext, Map<String,String> config) throws Exception {
    this(sqlContext, config, null);
  }

  public SolrRelation(SQLContext sqlContext, Map<String,String> config, DataFrame dataFrame) throws Exception {

    if (sqlContext == null)
      throw new IllegalArgumentException("SQLContext cannot be null!");

    this.sqlContext = sqlContext;
    this.sc =  new JavaSparkContext(sqlContext.sparkContext());
    String preserveSch = optionalParam(config, PRESERVE_SCHEMA, "N");
    if ("Y".equals(preserveSch) || Boolean.parseBoolean(preserveSch)) {
      preserveSchema = true;
    };
    String zkHost = requiredParam(config, SOLR_ZK_HOST_PARAM);
    String collection = requiredParam(config, SOLR_COLLECTION_PARAM);
    String query = optionalParam(config, SOLR_QUERY_PARAM, "*:*");
    String fieldListParam = optionalParam(config, SOLR_FIELD_LIST_PARAM, null);
    if (fieldListParam != null) {
      this.fieldList = fieldListParam.split(",");
    } else {
      this.fieldList = null;
    }
    String rowsParam = optionalParam(config, SOLR_ROWS_PARAM, null);
    if (rowsParam != null) {
      this.rows = new Integer(rowsParam);
    }

    parallelShards = Boolean.parseBoolean(optionalParam(config, SOLR_PARALLEL_SHARDS, "true"));

    splitFieldName = optionalParam(config, SOLR_SPLIT_FIELD_PARAM, null);
    if (splitFieldName != null)
      splitsPerShard = Integer.parseInt(optionalParam(config, SOLR_SPLITS_PER_SHARD_PARAM, "20"));

    if (!preserveSchema) {
      solrRDD = new SolrRDD(zkHost, collection);
    }
    else {
      solrRDD = new SchemaPreservingSolrRDD(zkHost, collection);
      solrRDD.setSc(sc);
    }

    solrQuery = SolrRDD.toQuery(query);

    if (fieldList != null) {
      solrQuery.setFields(fieldList);
    } else {
      solrQuery.remove("fl");
    }

    if (rows != null) {
      solrQuery.setRows(rows);
    }

    // iterate over the config object to pull out any params that
    // start with solr. to pass to the query as additional params,
    // but skip the q and fl as they are first-class options for this relation
    addlSolrParams = new ModifiableSolrParams();
    java.util.Map<String,String> configMap = JavaConverters.asJavaMapConverter(config).asJava();
    for (String key : configMap.keySet()) {
      if (key.startsWith("solr.")) {
        String param = key.substring(5);
        if ("q".equals(param) || "fl".equals(param) || "rows".equals(param))
          continue;

        String val = configMap.get(key);
        if (val != null) {
          addlSolrParams.add(param, val);
        }
      }
    }
    solrQuery.add(addlSolrParams);

    solrQuery.set("collection", collection);
    if (dataFrame != null) {
      schema = dataFrame.schema();
    } else {
      schema = solrRDD.getQuerySchema(solrQuery);
    }
  }

  protected String optionalParam(Map<String,String> config, String param, String defaultValue) {
    Option opt = config.get(param);
    String val = (opt != null && !opt.isEmpty()) ? (String)opt.get() : null;
    return (val == null || val.trim().isEmpty()) ? defaultValue : val;
  }

  protected String requiredParam(Map<String,String> config, String param) {
    String val = optionalParam(config, param, null);
    if (val == null) throw new IllegalArgumentException(param+" parameter is required!");
    return val;
  }

  public SolrQuery getQuery() {
    return solrQuery;
  }

  @Override
  public SQLContext sqlContext() {
    return sqlContext;
  }

  @Override
  public StructType schema() {
    return schema;
  }

  public RDD<Row> buildScan() {
    return buildScan(null, null);
  }

  public synchronized RDD<Row> buildScan(String[] fields, Filter[] filters) {

    // SchemaPreserviing dataframes returns all fields by default
    if (!preserveSchema) {
      log.info("Building Solr scan using fields=" + (fields != null ? Arrays.asList(fields).toString() : ""));

      if (fields != null && fields.length > 0) {
        solrQuery.setFields(fields);
      } else {
        if (this.fieldList != null) {
          solrQuery.setFields(this.fieldList);
        } else {
          solrQuery.remove("fl");
        }
      }
    }
    else {
      solrQuery.remove("fl");
    }

    // clear all existing filters
    solrQuery.remove("fq");
    if (filters != null && filters.length > 0) {
    log.info("Building SolrQuery using filters: " + Arrays.asList(filters));
    for (Filter filter : filters)
      applyFilter(filter);
    }

    solrQuery.add(addlSolrParams);

    if (log.isInfoEnabled())
      log.info("Constructed SolrQuery: " + solrQuery);

    RDD<Row> rows = null;
    try {

      // build the schema based on the desired fields - applicable only for non-schemapreserving dataframes in solr
      StructType querySchema = (fields != null && fields.length > 0 && !preserveSchema) ? deriveQuerySchema(fields) : schema;
      JavaRDD<SolrDocument> docs = parallelShards ?
              solrRDD.queryShards(sc, solrQuery, splitFieldName, splitsPerShard) : solrRDD.queryDeep(sc, solrQuery);
      rows = solrRDD.toRows(querySchema, docs).rdd();
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

  // derive a schema for a specific query from the full collection schema
  protected StructType deriveQuerySchema(String[] fields) {
    java.util.Map<String,StructField> fieldMap = new HashMap<String,StructField>();
    for (StructField f : schema.fields()) fieldMap.put(f.name(), f);
    List<StructField> listOfFields = new ArrayList<StructField>();
    for (String field : fields) listOfFields.add(fieldMap.get(field));
    return DataTypes.createStructType(listOfFields);
  }

  protected void applyFilter(Filter filter) {
    if (filter instanceof And) {
      And and = (And)filter;
      solrQuery.addFilterQuery(fq(and.left()));
      solrQuery.addFilterQuery(fq(and.right()));
    } else if (filter instanceof Or) {
      Or f = (Or)filter;
      solrQuery.addFilterQuery("(" + fq(f.left())+" OR " + fq(f.right())+")");
    } else if (filter instanceof Not) {
      Not not = (Not)filter;
      solrQuery.addFilterQuery("NOT "+fq(not.child()));
    } else {
      solrQuery.addFilterQuery(fq(filter));
    }
  }

  protected String fq(Filter f) {
    String negate = "";
    String crit = null;
    String attr = null;
    if (f instanceof EqualTo) {
      EqualTo eq = (EqualTo)f;
      attr = eq.attribute();
      crit = String.valueOf(eq.value());
    } else if (f instanceof GreaterThan) {
      GreaterThan gt = (GreaterThan)f;
      attr = gt.attribute();
      crit = "{"+gt.value()+" TO *]";
    } else if (f instanceof GreaterThanOrEqual) {
      GreaterThanOrEqual gte = (GreaterThanOrEqual)f;
      attr = gte.attribute();
      crit = "["+gte.value()+" TO *]";
    } else if (f instanceof LessThan) {
      LessThan lt = (LessThan)f;
      attr = lt.attribute();
      crit = "[* TO "+lt.value()+"}";
    } else if (f instanceof LessThanOrEqual) {
      LessThanOrEqual lte = (LessThanOrEqual)f;
      attr = lte.attribute();
      crit = "[* TO "+lte.value()+"]";
    } else if (f instanceof In) {
      In inf = (In)f;
      attr = inf.attribute();
      StringBuilder sb = new StringBuilder();
      sb.append("(");
      Object[] vals = inf.values();
      for (int v=0; v < vals.length; v++) {
        if (v > 0) sb.append(" ");
        sb.append(String.valueOf(vals[v]));
      }
      sb.append(")");
      crit = sb.toString();
    } else if (f instanceof IsNotNull) {
      IsNotNull inn = (IsNotNull)f;
      attr = inn.attribute();
      crit = "[* TO *]";
    } else if (f instanceof IsNull) {
      IsNull isn = (IsNull)f;
      attr = isn.attribute();
      crit = "[* TO *]";
      negate = "-";
    } else if (f instanceof StringContains) {
      StringContains sc = (StringContains)f;
      attr = sc.attribute();
      crit = "*"+sc.value()+"*";
    } else if (f instanceof StringEndsWith) {
      StringEndsWith sew = (StringEndsWith)f;
      attr = sew.attribute();
      crit = sew.value()+"*";
    } else if (f instanceof StringStartsWith) {
      StringStartsWith ssw = (StringStartsWith)f;
      attr = ssw.attribute();
      crit = "*"+ssw.value();
    } else {
      throw new IllegalArgumentException("Filters of type '"+f+" ("+f.getClass().getName()+")' not supported!");
    }
    return negate+attr+":"+crit;
  }

  public void insert(final DataFrame df, boolean overwrite) {

    JavaRDD<SolrInputDocument> docs = null;
    if (!preserveSchema) {
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
    }
    else{
      docs = ((SchemaPreservingSolrRDD) solrRDD).convertToSolrDocuments(df, new HashMap<String, Object>());
    }

    SolrSupport.indexDocs(solrRDD.zkHost, solrRDD.collection, 100, docs);
  }

}

