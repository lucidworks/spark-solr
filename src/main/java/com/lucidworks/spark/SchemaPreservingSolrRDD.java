package com.lucidworks.spark;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.RowFactory;

import org.apache.spark.sql.types.*;
import org.apache.spark.sql.Row;
import scala.Tuple2;


public class SchemaPreservingSolrRDD extends SolrRDD {
  public static Logger log = Logger.getLogger(SolrRDD.class);

  public SchemaPreservingSolrRDD(String collection) {
    super("localhost:9983", collection); // assume local embedded ZK if not supplied
  }

  public SchemaPreservingSolrRDD(String zkHost, String collection) {
    super(zkHost, collection);
  }

  @Override
  public StructType getQuerySchema(SolrQuery query) throws Exception {
    query.addFilterQuery("__lwcategory_s:schema AND __lwroot_s:root");
    JavaRDD<SolrDocument> rdd1 = queryShards(sc, query);
    return readSchema(rdd1.collect().get(0), getSolrClient(zkHost) , collection);
  }

  public static StructType readSchema(SolrDocument doc, SolrClient Solr, String collection) throws IOException, SolrServerException {
    List<StructField> fields = new ArrayList<StructField>();
    StructType st= (StructType) recurseReadSchema(doc, Solr, fields, collection).dataType();
    return st;
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

  @Override
  public JavaRDD<SolrDocument> queryShards(JavaSparkContext jsc, final SolrQuery origQuery, final String splitFieldName, final int splitsPerShard) throws SolrServerException {
    origQuery.addFilterQuery("__lwcategory_s:data");
    return super.queryShards(jsc, origQuery, splitFieldName, splitsPerShard);
  }

  @Override
  public JavaRDD<Row> toRows(final StructType schema, JavaRDD<SolrDocument> docs) {
    JavaRDD<SolrDocument> rootdocs = docs.filter(new Function<SolrDocument, Boolean>() {
        @Override
        public Boolean call(SolrDocument solrDocument) throws Exception {
            if (solrDocument.containsKey("__lwroot_s")) {
                return true;
            }
            return false;
        }
    });
    JavaPairRDD<String, SolrDocument> childdocs = docs.filter(new Function<SolrDocument, Boolean>() {
      @Override
      public Boolean call(SolrDocument solrDocument) throws Exception {
        if (solrDocument.containsKey("__lwroot_s")) {
          return false;
        }
        return true;
      }
    }).mapToPair(new PairFunction<SolrDocument, String, SolrDocument>() {
      @Override
      public Tuple2<String, SolrDocument> call(SolrDocument solrDocument){
        return new Tuple2<String, SolrDocument>(solrDocument.get("id").toString(), solrDocument);
      }
    });
    final Map<String, SolrDocument> childMap = childdocs.collectAsMap();
    JavaRDD<Row> rows = rootdocs.map(new Function<SolrDocument, Row>() {
      @Override
      public Row call(SolrDocument solrDocument) throws Exception {
        Row ret =  readData(solrDocument,  schema, collection, childMap);
        return ret;
      }
    });
    return rows;
  }

  public Row readData(SolrDocument doc, StructType st, String collection, Map<String, SolrDocument> childMap) throws IOException, SolrServerException {
    ArrayList<Object> str = new ArrayList<Object>();
    return recurseDataRead(doc , str, st, collection, childMap);
  }

  public Row recurseDataRead(SolrDocument doc, ArrayList<Object> x, StructType st, String collection, Map<String, SolrDocument> childMap) {
    Boolean recurse = true;
    java.util.Map<String, Object> x1 = doc.getFieldValueMap();
    String[] x3 = st.fieldNames();
    Object[] x2 = x1.keySet().toArray();
    for (int i = 0; i < x2.length; i++) {
      String x2Key = x2[i].toString();
      if (x2Key.startsWith("links")) {
        final String id = doc.get(x2Key).toString();
        if (id != null) {
          SolrDocument childDoc = childMap.get(id);
          if (childDoc == null){
            recurse = false;
          }
          if (recurse) {
            //l = l + 1;
            ArrayList<Object> str1 = new ArrayList<Object>();
            x.add(recurseDataRead(childDoc, str1, (StructType) st.fields()[x.size()].dataType(), collection, childMap));
          }
        }
      }
      if (x3.length > x.size() && x1.get(x3[x.size()]+"_s") == null && !st.fields()[x.size()].dataType().typeName().equals("struct")){
        x.add(null);
      }
      if ((x2Key.substring(x2Key.length() - 2, x2Key.length()).equals("_s") && !x2Key.startsWith("__lw") && !x2Key.startsWith("links"))) {
        String type = getFieldTypeMapping(st,x2Key.substring(0, x2Key.length() - 2));
        if (!type.equals("")) {
          String x2Val = x1.get(x2[i]).toString();
          if (type.equals("integer")) {
            x.add(convertToInteger(x2Val));
          } else if (type.equals("double")) {
            x.add(convertToDouble(x2Val));
          } else if (type.equals("float")) {
            x.add(convertToFloat(x2Val));
          } else if (type.equals("short")) {
            x.add(convertToShort(x2Val));
          } else if (type.equals("long")) {
            x.add(convertToLong(x2Val));
          } else if (type.equals("decimal")) {
            x.add(convertToDecimal(x2Val));
          } else if (type.equals("boolean")) {
            x.add(convertToBoolean(x2Val));
          } else if (type.equals("timestamp")) {
            x.add(convertToTimestamp(x2Val));
          } else if (type.equals("date")) {
            x.add(convertToDate(x2Val));
          } else if (type.equals("vector")) {
            x.add(convertToVector(x2Val));
          } else if (type.equals("matrix")) {
            x.add(convertToMatrix(x2Val));
          } else if (type.contains(":")) {
            //List<Object> debug = Arrays.asList(getArrayFromString(type, x1.get(x2[i]).toString(), 0, new ArrayList<Object[]>()));
            x.add(getArrayFromString(type, x2Val, 0, new ArrayList<Object[]>()));
          } else {
            x.add(x1.get(x2[i]));
          }
        }
        else {
          x.add(x1.get(x2[i]));
        }
      }
      if (x3.length > x.size() && x1.get(x3[x.size()]+"_s") == null && !st.fields()[x.size()].dataType().typeName().equals("struct")){
        x.add(null);
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
        if (f.dataType().typeName().toLowerCase().equals("array")) {
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

  public static Timestamp convertToTimestamp(String s){
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
    Timestamp timestamp = null;
    try {
      timestamp = new Timestamp(dateFormat.parse(s).getTime());
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return timestamp;
  }

  public static Date convertToDate(String s){
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
    Date date = null;
    try {
      date = dateFormat.parse(s);
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return date;
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
    if (type.contains(":") && type.split(":")[1].equals("array")) {
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
    else if (type.split(":")[1].equals("double")) {
      return convertToDoubleArray(items);
    }
    else if (type.split(":")[1].equals("float")) {
      return convertToFloatArray(items);
    }
    else if (type.split(":")[1].equals("short")) {
      return convertToShortArray(items);
    }
    else if (type.split(":")[1].equals("long")) {
      return convertToLongArray(items);
    }
    else {
      return items;
    }
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

  public JavaRDD<SolrInputDocument> convertToSolrDocuments(DataFrame df, final HashMap<String,Object> uniqueIdentifier) {

    SolrInputDocument s = new SolrInputDocument();
    final StructType styp = df.schema();
    int level = 0;
    Iterator it = uniqueIdentifier.entrySet().iterator();
    while (it.hasNext()) {
      java.util.Map.Entry pair = (java.util.Map.Entry)it.next();
      s.addField(pair.getKey().toString(), pair.getValue());
    }
    if (!s.containsKey("id")){
      String id = java.util.UUID.randomUUID().toString();
      s.addField("id",id);
    }
    s.addField("__lwroot_s", "root");
    s.addField("__lwcategory_s", "schema");
    recurseWriteSchema(styp, s, level);
    ArrayList<SolrInputDocument> a = new ArrayList<SolrInputDocument>();
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
        if (!solrDocument.containsKey("id")) {
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
      } else {
        if (!sf.dataType().typeName().toLowerCase().equals("array")) {
          s.addField(sf.name() + "_s", sf.dataType().typeName());
        } else {
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
      } else {
          if (df != null) {
            if (sf.dataType().typeName().equals("array")) {
              solrDocument.addField(sf.name() + "_s", getArrayToString(sf.dataType(), df.get(counter)));
            } else if (sf.dataType().typeName().equals("matrix")) {
                org.apache.spark.mllib.linalg.Matrix m = (org.apache.spark.mllib.linalg.Matrix) df.get(counter);
                solrDocument.addField(sf.name() + "_s", m.numRows() + ":" + m.numCols() + ":" + Arrays.toString(m.toArray()));
              } else {
                if (df.get(counter) != null) {
                  solrDocument.addField(sf.name() + "_s", df.get(counter).toString());
                } else {
                  solrDocument.addField(sf.name() + "_s", null);
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
      int arraysize = 0;
      Object[] ab1 = new Object[arraysize];
      if (value instanceof  scala.collection.mutable.WrappedArray) {
        scala.collection.mutable.WrappedArray ab = (scala.collection.mutable.WrappedArray) value;
        arraysize = ab.size();
        ab1 = new Object[ab.size()];
        ab.deep().copyToArray(ab1);
      }
      if (value instanceof  scala.collection.mutable.ArrayBuffer) {
        scala.collection.mutable.ArrayBuffer ab = (scala.collection.mutable.ArrayBuffer) value;
        arraysize = ab.size();
        //ab1 = new Object[ab.size()];
        ab1 = ab.array();
      }
      Object[] d;
      if (arraysize > 0) {
        d = new Object[arraysize];
        for (int i = 0; i < ab1.length; i++) {
          if (e.typeName().equals("array")) {
            d[i] = getArrayToString(e, ab1[i]);
          } else {
            d[i] = ab1[i];
          }
        }
      } else {
        d = new String[]{};
      }
      return Arrays.toString(d);
    }
    return "";
  }

  public static String getArraySchema(org.apache.spark.sql.types.DataType dType) {
    if (((org.apache.spark.sql.types.ArrayType) dType).elementType().typeName().equals("array")) {
      return dType.typeName() + ":" + getArraySchema(((org.apache.spark.sql.types.ArrayType) dType).elementType());
    } else {
      return dType.typeName() + ":" + ((org.apache.spark.sql.types.ArrayType) dType).elementType().typeName();
    }
  }

}
