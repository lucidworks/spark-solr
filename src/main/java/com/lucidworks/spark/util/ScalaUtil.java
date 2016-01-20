package com.lucidworks.spark.util;

import org.apache.log4j.Logger;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

public class ScalaUtil implements Serializable {

  public static Logger log = Logger.getLogger(ScalaUtil.class);

  public static String optionalParam(scala.collection.immutable.Map<String,String> config, String param, String defaultValue) {
    scala.Option<String> opt = config.get(param);
    String val = (opt != null && !opt.isEmpty()) ? (String)opt.get() : null;
    return (val == null || val.trim().isEmpty()) ? defaultValue : val;
  }

  public static String requiredParam(scala.collection.immutable.Map<String,String> config, String param) {
    String val = optionalParam(config, param, null);
    if (val == null) throw new IllegalArgumentException(param+" parameter is required!");
    return val;
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
}
