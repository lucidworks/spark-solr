package com.lucidworks.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.sql.RowFactory;

import org.apache.spark.sql.types.*;
import org.apache.spark.sql.Row;

/**
 * Created by ganeshkumar on 9/29/15.
 */
public class SchemaPreservingSolrRDD extends SolrRDD {
  public static Logger log = Logger.getLogger(SolrRDD.class);

  public SchemaPreservingSolrRDD(String collection) {
    super("localhost:9983", collection); // assume local embedded ZK if not supplied
  }

  public SchemaPreservingSolrRDD(String zkHost, String collection) {
    super(zkHost, collection);
  }

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

  public JavaRDD<SolrDocument> queryShards(JavaSparkContext jsc, final SolrQuery origQuery, final String splitFieldName, final int splitsPerShard) throws SolrServerException {
    origQuery.addFilterQuery("__lwcategory_s:data AND __lwroot_s:root");
    return super.queryShards(jsc, origQuery, splitFieldName, splitsPerShard);
  }

  public JavaRDD<Row> toRows(final StructType schema, JavaRDD<SolrDocument> docs) {
    JavaRDD<Row> rows = docs.map(new Function<SolrDocument, Row>() {
      @Override
      public Row call(SolrDocument solrDocument) throws Exception {
        Row ret =  readData(solrDocument, getSolrClient(zkHost) ,  schema, collection);
        return ret;
      }
    });
    return rows;
  }

  public Row readData(SolrDocument doc, SolrClient solr, StructType st, String collection) throws IOException, SolrServerException {
    ArrayList<Object> str = new ArrayList<Object>();
    return recurseDataRead(doc, solr, str, st, collection);
  }

  public Row recurseDataRead(SolrDocument doc, SolrClient solr, ArrayList<Object> x, StructType st, String collection) {
    Boolean recurse = true;
    java.util.Map<String, Object> x1 = doc.getFieldValueMap();
    String[] x3 = st.fieldNames();
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
            x.add(recurseDataRead(docs1.get(0), solr, str1, (StructType) st.fields()[x.size()].dataType(), collection));
          }
        }
      }
      if(x3.length > x.size() && x1.get(x3[x.size()]+"_s") == null && !st.fields()[x.size()].dataType().typeName().equals("struct")){
        x.add(null);
      }
      if ((x2[i].toString().substring(x2[i].toString().length() - 2, x2[i].toString().length()).equals("_s") && !x2[i].toString().startsWith("__lw") && !x2[i].toString().startsWith("links"))) {
        String type = getFieldTypeMapping(st,x2[i].toString().substring(0,x2[i].toString().length()-2));
        if (!type.equals("")) {
          if (type.equals("integer")) {
            x.add(convertToInteger(x1.get(x2[i]).toString()));
          } else if (type.equals("double")) {
            x.add(convertToDouble(x1.get(x2[i]).toString()));
          } else if (type.equals("float")) {
            x.add(convertToFloat(x1.get(x2[i]).toString()));
          } else if (type.equals("short")) {
            x.add(convertToShort(x1.get(x2[i]).toString()));
          } else if (type.equals("long")) {
            x.add(convertToLong(x1.get(x2[i]).toString()));
          } else if (type.equals("decimal")) {
            x.add(convertToDecimal(x1.get(x2[i]).toString()));
          } else if (type.equals("boolean")) {
            x.add(convertToBoolean(x1.get(x2[i]).toString()));
          } else if (type.equals("timestamp")) {
          } else if (type.equals("date")) {
          } else if (type.equals("vector")) {
            x.add(convertToVector(x1.get(x2[i]).toString()));
          } else if (type.equals("matrix")) {
            x.add(convertToMatrix(x1.get(x2[i]).toString()));
          } else if (type.contains(":")) {
            //List<Object> debug = Arrays.asList(getArrayFromString(type, x1.get(x2[i]).toString(), 0, new ArrayList<Object[]>()));
            x.add(getArrayFromString(type, x1.get(x2[i]).toString(), 0, new ArrayList<Object[]>()));
          } else {
            x.add(x1.get(x2[i]));
          }
        }
        else {
          x.add(x1.get(x2[i]));
        }
      }
      if(x3.length > x.size() && x1.get(x3[x.size()]+"_s") == null && !st.fields()[x.size()].dataType().typeName().equals("struct")){
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

}
