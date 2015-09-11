package com.lucidworks.spark;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.MatrixUDT;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import scala.Option;
import scala.collection.immutable.Map;

public class SolrRelation extends BaseRelation implements Serializable, TableScan, PrunedFilteredScan, InsertableRelation {

  public static Logger log = Logger.getLogger(SolrRelation.class);

  public static String SOLR_ZK_HOST_PARAM = "zkhost";
  public static String SOLR_COLLECTION_PARAM = "collection";
  public static String SOLR_QUERY_PARAM = "query";
  public static String SOLR_SPLIT_FIELD_PARAM = "split_field";
  public static String SOLR_SPLITS_PER_SHARD_PARAM = "splits_per_shard";
  public static String PRESERVE_SCHEMA = "preserveschema";
  protected String preserveSchema;
  protected String splitFieldName;
  protected int splitsPerShard = 1;
  protected SolrQuery solrQuery;
  protected SolrRDD solrRDD;
  protected StructType schema;
  protected transient SQLContext sqlContext;
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
    preserveSchema = optionalParam(config, PRESERVE_SCHEMA, "N");
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
    }
    else {
      if(!preserveSchema.equals("Y")) {
        schema = solrRDD.getQuerySchema(solrQuery);
      }
      else{
        SolrQuery schemaQuery = solrQuery.getCopy();
        schemaQuery.addFilterQuery("__lwcategory_s:schema AND __lwroot_s:root");
        JavaRDD<SolrDocument> rdd1 = solrRDD.queryShards(sc, schemaQuery);
        schema = readSchema(rdd1.collect().get(0), solrRDD.getSolrClient(solrRDD.zkHost) , solrRDD.collection);
      }
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
    if(!preserveSchema.equals("Y")) {
      if (fields != null && fields.length > 0)
        solrQuery.setFields(fields);
      else
        solrQuery.remove("fl");

      // clear all existing filters
      solrQuery.remove("fq");
      if (filters != null && filters.length > 0) {
        log.info("Building SolrQuery using filters: " + Arrays.asList(filters));
        for (Filter filter : filters)
          applyFilter(filter);
      }

      if (log.isInfoEnabled())
        log.info("Constructed SolrQuery: " + solrQuery);

      RDD<Row> rows = null;
      try {
        // build the schema based on the desired fields
        StructType querySchema = (fields != null && fields.length > 0) ? deriveQuerySchema(fields) : schema;
        JavaSparkContext jsc = new JavaSparkContext(sqlContext.sparkContext());
        JavaRDD<SolrDocument> docs = solrRDD.queryShards(jsc, solrQuery, splitFieldName, splitsPerShard);
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
    else {
      solrQuery.remove("fq");
      if (filters != null && filters.length > 0) {
        log.info("Building SolrQuery using filters: " + Arrays.asList(filters));
        for (Filter filter : filters)
          applyFilter(filter);
      }
      JavaRDD<Row> rows = null;
      try {
        rows = readDataFrameCustom(sc, sqlContext);
      }
      catch (Exception e) {
        if (e instanceof RuntimeException) {
          throw (RuntimeException) e;
        }
        else {
          throw new RuntimeException(e);
        }
      }
      return rows.rdd();
    }
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
    if(!preserveSchema.equals("Y")) {
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
            if (val != null)
              doc.setField(fname, val);
          }
          return doc;
        }
      });
    }
    else{
      docs = convertToSolrDocuments(df, new HashMap<String,Object>());
    }

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
  public JavaRDD<Row> readDataFrameCustom(JavaSparkContext sc, SQLContext sqlCtx) {
    SolrQuery dataQuery = solrQuery.getCopy();
    dataQuery.addFilterQuery("__lwcategory_s:data AND __lwroot_s:root");
    try {
      JavaRDD<SolrDocument> rdd2 = solrRDD.queryShards(sc, dataQuery);
      JavaRDD<Row> rows = rdd2.map(new Function<SolrDocument, Row>() {
        @Override
        public Row call(SolrDocument solrDocument) throws Exception {
          Row ret =  readData(solrDocument, solrRDD.getSolrClient(solrRDD.zkHost) ,  schema, solrRDD.collection);
          return ret;
        }
      });
      //return rows;
      return sqlCtx.applySchema(rows, schema).rdd().toJavaRDD();
    } catch (SolrServerException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static StructType readSchema(SolrDocument doc, SolrClient Solr, String collection) throws IOException, SolrServerException {
    List<StructField> fields = new ArrayList<StructField>();
    StructType st= (StructType) recurseReadSchema(doc, Solr, fields, collection).dataType();
    return st;
  }

  public static Row readData(SolrDocument doc, SolrClient solr, StructType st, String collection) throws IOException, SolrServerException {
    ArrayList<Object> str = new ArrayList<Object>();
    return recurseDataRead(doc, solr, str, st, collection);
  }

  public static StructField recurseReadSchema(SolrDocument doc, SolrClient solr, List<StructField> fldr, String collection) throws IOException, SolrServerException {
    Boolean recurse = true;
    String finalName = null;
    for (java.util.Map.Entry<String, Object> field : doc.entrySet()) {
      String name = field.getKey();
      Object value = field.getValue();
      if (name.startsWith("links")) {
        String id = doc.get(name).toString();
        if (id != null) {
          SolrQuery q1 = new SolrQuery("id:" + id);
          QueryResponse rsp1 = null;
          try {
            rsp1 = solr.query(collection,q1);
          } catch (Exception E) {
            log.error(E.toString());
            recurse = false;
          }
          if (recurse) {
            SolrDocumentList docs1 = rsp1.getResults();
            List<StructField> fld1 = new ArrayList<StructField>();
            fldr.add(recurseReadSchema(docs1.get(0), solr, fld1, collection));
          }
        }
      }
      if (name.substring(name.length()-2,name.length()).equals("_s")  && !name.equals("__lwroot_s") && !name.startsWith("links") && !name.equals("__lwcategory_s")) {
        if (name.substring(0, name.length() - 2).equals("__lwchilddocname")) {
          finalName = field.getValue().toString();
        } else {
          fldr.add(new StructField(name.substring(0, name.length() - 2), getsqlDataType(field.getValue().toString()), true, Metadata.empty()));
        }
      }

    }
    StructField[] farr = new StructField[fldr.size()];
    farr = fldr.toArray(farr);
    StructType st2 = new StructType(farr);
    if (finalName == null) {
      finalName = "root";
    }
    return new StructField(finalName, st2, true,  Metadata.empty());
  }

  public static DataType getsqlDataType(String s) {
    if (s.toLowerCase().equals("double")) {
      return DataTypes.DoubleType;
    }
    if (s.toLowerCase().equals("byte")) {
      return DataTypes.ByteType;
    }
    if (s.toLowerCase().equals("short")) {
      return DataTypes.ShortType;
    }
    if (((s.toLowerCase().equals("int")) || (s.toLowerCase().equals("integer")))) {
      return DataTypes.IntegerType;
    }
    if (s.toLowerCase().equals("long")) {
      return DataTypes.LongType;
    }
    if (s.toLowerCase().equals("String")) {
      return DataTypes.StringType;
    }
    if (s.toLowerCase().equals("boolean")) {
      return DataTypes.BooleanType;
    }
    if (s.toLowerCase().equals("timestamp")) {
      return DataTypes.TimestampType;
    }
    if (s.toLowerCase().equals("date")) {
      return DataTypes.DateType;
    }
    if (s.toLowerCase().equals("vector")) {
      return new VectorUDT();
    }
    if (s.toLowerCase().equals("matrix")) {
      return new MatrixUDT();
    }
    if (s.contains(":") && s.split(":")[0].toLowerCase().equals("array")) {
      return getArrayTypeRecurse(s,0);
    }
    return DataTypes.StringType;
  }

  public static DataType getArrayTypeRecurse(String s, int fromIdx) {
    if (s.contains(":") && s.split(":")[1].toLowerCase().equals("array")) {
      fromIdx = s.indexOf(":", fromIdx);
      s = s.substring(fromIdx+1, s.length());
      return DataTypes.createArrayType(getArrayTypeRecurse(s,fromIdx));
    }
    return DataTypes.createArrayType(getsqlDataType(s.split(":")[1]));
  }

  public static Row recurseDataRead(SolrDocument doc, SolrClient solr, ArrayList<Object> x, StructType st, String collection) {
    Boolean recurse = true;
    java.util.Map<String, Object> x1 = doc.getFieldValueMap();
    Object[] x2 = x1.keySet().toArray();
    for (int i = 0; i < x2.length; i++) {
      if (x2[i].toString().startsWith("links")) {
        String id = doc.get(x2[i].toString()).toString();
        if (id != null) {
          SolrQuery q1 = new SolrQuery("id:" + id);
          QueryResponse rsp1 = null;
          try {
            rsp1 = solr.query(collection, q1);
          } catch (Exception E) {
            recurse = false;
          }
          if (recurse) {
            //l = l + 1;
            SolrDocumentList docs1 = rsp1.getResults();
            ArrayList<Object> str1 = new ArrayList<Object>();
            x.add(recurseDataRead(docs1.get(0), solr, str1, st, collection));
          }
        }
      }
      if (x2[i].toString().substring(x2[i].toString().length() - 2, x2[i].toString().length()).equals("_s") && !x2[i].toString().startsWith("__lw") && !x2[i].toString().startsWith("links")) {
        String type = getFieldTypeMapping(st,x2[i].toString().substring(0,x2[i].toString().length()-2));
        if (!type.equals("")) {
          if (type.equals("integer")) {
            x.add(convertToInteger(x1.get(x2[i]).toString()));
          }
          else if (type.equals("double")) {
            x.add(convertToDouble(x1.get(x2[i]).toString()));
          }
          else if (type.equals("float")) {
            x.add(convertToFloat(x1.get(x2[i]).toString()));
          }
          else if (type.equals("short")) {
            x.add(convertToShort(x1.get(x2[i]).toString()));
          }
          else if (type.equals("long")) {
            x.add(convertToLong(x1.get(x2[i]).toString()));
          }
          else if (type.equals("decimal")) {
            x.add(convertToDecimal(x1.get(x2[i]).toString()));
          }
          else if (type.equals("boolean")) {
            x.add(convertToBoolean(x1.get(x2[i]).toString()));
          }
          else if (type.equals("timestamp")) {
          }
          else if (type.equals("date")) {
          }
          else if (type.equals("vector")) {
            x.add(convertToVector(x1.get(x2[i]).toString()));
          }
          else if (type.equals("matrix")) {
            x.add(convertToMatrix(x1.get(x2[i]).toString()));
          }
          else if (type.contains(":")) {
            //List<Object> debug = Arrays.asList(getArrayFromString(type, x1.get(x2[i]).toString(), 0, new ArrayList<Object[]>()));
            x.add(getArrayFromString(type, x1.get(x2[i]).toString(), 0, new ArrayList<Object[]>()));
          }
          else {
            x.add(x1.get(x2[i]));
          }
        }
        else {
          x.add(x1.get(x2[i]));
        }
      }
    }
    if (x.size()>0) {
      Object[] array = new Object[x.size()];
      x.toArray(array);
      return RowFactory.create(array);
    }
  return null;
  }

  public static String getFieldTypeMapping(StructType s, String fieldName) {
    scala.collection.Iterator x = s.iterator();
    while (x.hasNext()) {
      StructField f = (StructField) x.next();
      if (f.name().equals(fieldName) && !f.dataType().typeName().toString().toLowerCase().equals("struct")) {
        if(f.dataType().typeName().toLowerCase().equals("array")) {
          if (((ArrayType) f.dataType()).elementType().typeName().toLowerCase().equals("array")) {
            return (f.dataType().typeName() + ":" + (getFieldTypeMapping((ArrayType) (((ArrayType) f.dataType()).elementType()), fieldName)));
          }
          else {
            return (f.dataType().typeName() + ":" + ((ArrayType) f.dataType()).elementType().typeName());
          }
        }
        else {
          return f.dataType().typeName();
        }
      }
      else {
        if (f.dataType().typeName().toString().toLowerCase().equals("struct")) {
          String fieldType = getFieldTypeMapping((StructType) f.dataType(), fieldName);
          if (!fieldType.equals("")) {
            return fieldType;
          }
        }

      }
    }
    return "";
  }

  public static String getFieldTypeMapping(ArrayType d, String fieldName) {
    if (d.elementType().typeName().toLowerCase().equals("array")) {
      getFieldTypeMapping((ArrayType) d.elementType(), fieldName);
    }
    return (d.typeName() + ":" + d.elementType().typeName());
  }
  public static Integer convertToInteger(String s) {
    return Integer.parseInt(s);
  }

  public static Double convertToDouble(String s) {
    return Double.parseDouble(s);
  }

  public static Float convertToFloat(String s) {
    return Float.parseFloat(s);
  }

  public static Short convertToShort(String s) {
    return Short.parseShort(s);
  }

  public static Long convertToLong(String s) {
    return Long.parseLong(s);
  }

  public static Decimal convertToDecimal(String s) {
    return Decimal.apply(s);
  }

  public static Boolean convertToBoolean(String s) {
    return Boolean.parseBoolean(s);
  }

  public static Integer[] convertToIntegerArray(String[] s) {
    Integer[] results = new Integer[s.length];
    for (int i = 0; i < s.length; i++) {
      try {
        results[i] = Integer.parseInt(s[i]);
      } catch (NumberFormatException nfe) {
        log.error("Unable to convert String array to integer array");
      };
    }
    return results;
  }

  public static Double[] convertToDoubleArray(String[] s) {
    Double[] results = new Double[s.length];
    for (int i = 0; i < s.length; i++) {
      try {
        results[i] = Double.parseDouble(s[i]);
      } catch (NumberFormatException nfe) {
        log.error("Unable to convert String array to double array");
      };
    }
    return results;
  }

  public static Float[] convertToFloatArray(String[] s) {
    Float[] results = new Float[s.length];
    for (int i = 0; i < s.length; i++) {
      try {
        results[i] = Float.parseFloat(s[i]);
      } catch (NumberFormatException nfe) {
        log.error("Unable to convert String array to float array");
      };
    }
    return results;
  }

  public static Short[] convertToShortArray(String[] s) {
    Short[] results = new Short[s.length];
    for (int i = 0; i < s.length; i++) {
      try {
        results[i] = Short.parseShort(s[i]);
      } catch (NumberFormatException nfe) {
        log.error("Unable to convert String array to short array");
      };
    }
    return results;
  }

  public static Long[] convertToLongArray(String[] s) {
    Long[] results = new Long[s.length];
    for (int i = 0; i < s.length; i++) {
      try {
        results[i] = Long.parseLong(s[i]);
      }
      catch (NumberFormatException nfe) {
        log.error("Unable to convert string array to long array");
      };
    }
    return results;
  }

  public static Boolean[] convertToBooleanArray(String[] s) {
    Boolean[] results = new Boolean[s.length];
    for (int i = 0; i < s.length; i++) {
      try {
        results[i] = Boolean.parseBoolean(s[i]);
      } catch (NumberFormatException nfe) {
        log.error("Unable to convert string array to boolean array");
      };
    }
    return results;
  }

  public static org.apache.spark.mllib.linalg.Vector convertToVector(String s) {
    return Vectors.parse(s);
  }

  public static org.apache.spark.mllib.linalg.Matrix convertToMatrix(String s) {
    String[] data = s.split(":");
    String dataArray = data[2];
    String[] items = dataArray.replaceFirst("\\[", "").substring(0,dataArray.replaceFirst("\\[", "").lastIndexOf("]")).split(",");
    double[] doubleArray = new double[items.length];
    for (int i = 0; i<items.length; i++) {
      doubleArray[i] = Double.parseDouble(items[i]);
    }
    return Matrices.dense(Integer.parseInt(data[0]), Integer.parseInt(data[1]), doubleArray);
  }

  public static Object[] getArrayFromString(String type, String s, int fromIdx, ArrayList<Object[]> ret) {
    if(type.contains(":") && type.split(":")[1].equals("array")) {
      fromIdx = type.indexOf(":", fromIdx);
      type = type.substring(fromIdx+1, type.length());
      String[] items = s.replaceFirst("\\[", "").substring(0,s.replaceFirst("\\[", "").lastIndexOf("]")).split("\\],");
      ArrayList<Object[]> ret1 = new ArrayList<Object[]>();
      for (int i=0; i<items.length; i++) {
        if (i == items.length -1 ) {
          ret1.add(getArrayFromString(type, items[i], fromIdx, ret1));
        }
        else {
          ret1.add(getArrayFromString(type, items[i] + "]", fromIdx, ret1));
        }
      }
      ret.add(ret1.toArray());
      return ret1.toArray();
    }
    String[] items = s.replaceFirst("\\[", "").substring(0,s.replaceFirst("\\[", "").lastIndexOf("]")).split(",");
    if (type.split(":")[1].equals("integer")) {
      return convertToIntegerArray(items);
    }
    else if(type.split(":")[1].equals("double")) {
      return convertToDoubleArray(items);
    }
    else if(type.split(":")[1].equals("float")) {
      return convertToFloatArray(items);
    }
    else if(type.split(":")[1].equals("short")) {
      return convertToShortArray(items);
    }
    else if(type.split(":")[1].equals("long")) {
      return convertToLongArray(items);
    }
    else {
      return items;
    }
  }

}

