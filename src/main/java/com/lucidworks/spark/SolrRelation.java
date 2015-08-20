package com.lucidworks.spark;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;

import scala.Option;
import scala.collection.immutable.Map;

public class SolrRelation extends BaseRelation implements Serializable, TableScan, PrunedFilteredScan, InsertableRelation {

  public static Logger log = Logger.getLogger(SolrRelation.class);

  public static String SOLR_ZK_HOST_PARAM = "zkhost";
  public static String SOLR_COLLECTION_PARAM = "collection";
  public static String SOLR_QUERY_PARAM = "query";
  public static String SOLR_SPLIT_FIELD_PARAM = "split_field";
  public static String SOLR_SPLITS_PER_SHARD_PARAM = "splits_per_shard";

  protected String splitFieldName;
  protected int splitsPerShard = 1;
  protected SolrQuery solrQuery;
  protected SolrRDD solrRDD;
  protected StructType schema;
  protected transient SQLContext sqlContext;

  public SolrRelation() {}

  public SolrRelation(SQLContext sqlContext, Map<String,String> config) throws Exception {
    this(sqlContext, config, null);
  }

  public SolrRelation(SQLContext sqlContext, Map<String,String> config, DataFrame dataFrame) throws Exception {

    if (sqlContext == null)
      throw new IllegalArgumentException("SQLContext cannot be null!");

    this.sqlContext = sqlContext;

    String zkHost = requiredParam(config, SOLR_ZK_HOST_PARAM);
    String collection = requiredParam(config, SOLR_COLLECTION_PARAM);
    String query = optionalParam(config, SOLR_QUERY_PARAM, "*:*");
    splitFieldName = optionalParam(config, SOLR_SPLIT_FIELD_PARAM, null);
    if (splitFieldName != null)
      splitsPerShard = Integer.parseInt(optionalParam(config, SOLR_SPLITS_PER_SHARD_PARAM, "10"));
    solrRDD = new SolrRDD(zkHost, collection);
    solrQuery = SolrRDD.toQuery(query);
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
    if (fields != null && fields.length > 0)
      solrQuery.setFields(fields);
    else
      solrQuery.remove("fl");

    // clear all existing filters
    solrQuery.remove("fq");
    if (filters != null && filters.length > 0) {
      log.info("Building SolrQuery using filters: "+Arrays.asList(filters));
      for (Filter filter : filters)
        applyFilter(filter);
    }

    if (log.isInfoEnabled())
      log.info("Constructed SolrQuery: "+solrQuery);

    RDD<Row> rows = null;
    try {
      // build the schema based on the desired fields
      StructType querySchema =
        (fields != null && fields.length > 0) ? deriveQuerySchema(fields) : schema;
      JavaSparkContext jsc = new JavaSparkContext(sqlContext.sparkContext());
      JavaRDD<SolrDocument> docs =
        solrRDD.queryShards(jsc, solrQuery, splitFieldName, splitsPerShard);
      rows = solrRDD.toRows(querySchema, docs).rdd();
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException)e;
      } else {
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
    JavaRDD<SolrInputDocument> docs = df.javaRDD().map(new Function<Row, SolrInputDocument>() {
      public SolrInputDocument call(Row row) throws Exception {
        StructType schema = row.schema();
        SolrInputDocument doc = new SolrInputDocument();
        for (StructField f : schema.fields()) {
          String fname = f.name();
          if (fname.equals("_version_"))
            continue;

          int fieldIndex = row.fieldIndex(fname);
          Object val = row.isNullAt(fieldIndex) ? null : row.get(fieldIndex);
          if (val != null)
            doc.setField(fname, val);
        }
        return doc;
      }
    });
    SolrSupport.indexDocs(solrRDD.zkHost, solrRDD.collection, 100, docs);
  }


  public void sendToSolr(JavaRDD<SolrInputDocument> dfrdd){
    SolrSupport.indexDocs(solrRDD.zkHost, solrRDD.collection, 100, dfrdd);
  }
  public JavaRDD<SolrInputDocument> convertToSolrDocuments(DataFrame df, final HashMap<String,Object> uniqueIdentifier) {

    SolrInputDocument s = new SolrInputDocument();
    final StructType styp = df.schema();
    int level = 0;
    Iterator it = uniqueIdentifier.entrySet().iterator();
    while (it.hasNext()) {
      java.util.Map.Entry pair = (java.util.Map.Entry)it.next();
      s.addField(pair.getKey().toString(), pair.getValue());
    }
    if(!s.containsKey("id")){
      String id = java.util.UUID.randomUUID().toString();
      s.addField("id",id);
    }
    s.addField("__lwroot_s", "root");
    s.addField("__lwcategory_s", "schema");
    recurseWriteSchema(styp, s, level);
    ArrayList<SolrInputDocument> a = new ArrayList<SolrInputDocument>();
    JavaSparkContext sc = new JavaSparkContext(sqlContext.sparkContext());
    a.add(s);
    JavaRDD<SolrInputDocument> rdd1 = sc.parallelize(a);
    JavaRDD<SolrInputDocument> a1 = df.javaRDD().map(new Function<Row, SolrInputDocument>() {
      public SolrInputDocument call(Row r) throws Exception {
        SolrInputDocument solrDocument = new SolrInputDocument();
        Iterator it = uniqueIdentifier.entrySet().iterator();
        while (it.hasNext()) {
          java.util.Map.Entry pair = (java.util.Map.Entry)it.next();
          solrDocument.addField(pair.getKey().toString(), pair.getValue());
        }
        if(!solrDocument.containsKey("id")) {
          String idData = java.util.UUID.randomUUID().toString();
          solrDocument.addField("id", idData);
        }
        solrDocument.addField("__lwroot_s", "root");
        solrDocument.addField("__lwcategory_s", "data");
        List<org.apache.spark.sql.Row> r1 = new ArrayList<Row>();
        r1.add(r);
        return recurseWriteData(styp, solrDocument, r, 0);
      }
    });
    JavaRDD<SolrInputDocument> finalrdd = rdd1.union(a1);
    return finalrdd;
  }

  public static void recurseWriteSchema(StructType st, SolrInputDocument s, int l){
    scala.collection.Iterator x = st.iterator();
    int linkCount = 0;
    while (x.hasNext()) {
      StructField sf = (StructField) x.next();
      if (sf.dataType().typeName().toString().toLowerCase().equals("struct")){
        linkCount = linkCount + 1;
        SolrInputDocument sc = new SolrInputDocument();
        String id = java.util.UUID.randomUUID().toString();
        sc.addField("id",id);
        s.addField("links"+linkCount +"_s", id);
        l = l + 1;
        sc.addField("__lwchilddocname_s",sf.name());
        sc.addField("__lwcategory_s","schema");
        recurseWriteSchema((StructType) sf.dataType(), sc, l);
        s.addChildDocument(sc);
      }
      else {
        if (!sf.dataType().typeName().toLowerCase().equals("array")) {
          s.addField(sf.name() + "_s", sf.dataType().typeName());
        }
        else {
          s.addField(sf.name() + "_s", getArraySchema(sf.dataType()));
        }
      }
    }
  }

  public SolrInputDocument recurseWriteData(StructType st,SolrInputDocument solrDocument, org.apache.spark.sql.Row df, int counter) {
    scala.collection.Iterator x = st.iterator();
    int linkCount = 0;
    while (x.hasNext()) {
      StructField sf = (StructField) x.next();
      if (sf.dataType().typeName().toString().toLowerCase().equals("struct")) {
        linkCount = linkCount + 1;
        SolrInputDocument solrDocument1 = new SolrInputDocument();
        String idChild = java.util.UUID.randomUUID().toString();
        solrDocument1.addField("id", idChild);
        solrDocument.addField("links" + linkCount + "_s", idChild);
        solrDocument1.addField("__lwchilddocname_s", sf.name());
        solrDocument1.addField("__lwcategory_s", "data");
        org.apache.spark.sql.Row df1 = (org.apache.spark.sql.Row) df.get(counter);
        solrDocument.addChildDocument(recurseWriteData((StructType) sf.dataType(), solrDocument1, df1, 0));

      }
      else {
        if (df != null) {
          if (sf.dataType().typeName().equals("array")) {
            solrDocument.addField(sf.name() + "_s", getArrayToString(sf.dataType(), df.get(counter)));
          }
          else if (sf.dataType().typeName().equals("matrix")) {
            org.apache.spark.mllib.linalg.Matrix m = (org.apache.spark.mllib.linalg.Matrix) df.get(counter);
            solrDocument.addField(sf.name() + "_s", m.numRows() + ":" + m.numCols() + ":" + Arrays.toString(m.toArray()));
          } else {
            if (df.get(counter) != null) {
              solrDocument.addField(sf.name() + "_s", df.get(counter).toString());
            } else {
              solrDocument.addField(sf.name() + "_s", "0");
            }
          }
        }

      }
      counter = counter + 1;
    }
  return solrDocument;
}

  public static Object getArrayToString(org.apache.spark.sql.types.DataType dataType, Object value) {
    if (dataType.typeName().equals("array")) {
      org.apache.spark.sql.types.ArrayType a = (org.apache.spark.sql.types.ArrayType) dataType;
      org.apache.spark.sql.types.DataType e = a.elementType();
      scala.collection.mutable.ArrayBuffer ab = (scala.collection.mutable.ArrayBuffer) value;
      Object[] d ;
      if (ab.size() > 0) {
        d = new Object[ab.size()];
        for (int i = 0; i < ab.array().length; i++) {
          if (e.typeName().equals("array")) {
            d[i] = getArrayToString(e, ab.array()[i]);
          }
          else {
            d[i] = (Double) ab.array()[i];
          }
        }
      }
      else {
        d = new Double[]{};
      }
      return Arrays.toString(d);
    }
    return "";
  }

  public static String getArraySchema(org.apache.spark.sql.types.DataType dType) {
    if (((org.apache.spark.sql.types.ArrayType) dType).elementType().typeName().equals("array")) {
      return dType.typeName() + ":" + getArraySchema(((org.apache.spark.sql.types.ArrayType) dType).elementType());
    }
    else {
      return dType.typeName() + ":" + ((org.apache.spark.sql.types.ArrayType) dType).elementType().typeName();
    }
  }

}
