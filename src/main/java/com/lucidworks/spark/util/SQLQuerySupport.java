package com.lucidworks.spark.util;

import org.apache.spark.mllib.linalg.MatrixUDT;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;


public class SQLQuerySupport {
  public static DataType getsqlDataType(String s) {
    if (s.equalsIgnoreCase("double")) {
      return DataTypes.DoubleType;
    }
    if (s.equalsIgnoreCase("byte")) {
      return DataTypes.ByteType;
    }
    if (s.equalsIgnoreCase("short")) {
      return DataTypes.ShortType;
    }
    if (((s.equalsIgnoreCase("int")) || (s.equalsIgnoreCase("integer")))) {
      return DataTypes.IntegerType;
    }
    if (s.equalsIgnoreCase("long")) {
      return DataTypes.LongType;
    }
    if (s.equalsIgnoreCase("String")) {
      return DataTypes.StringType;
    }
    if (s.equalsIgnoreCase("boolean")) {
      return DataTypes.BooleanType;
    }
    if (s.equalsIgnoreCase("timestamp")) {
      return DataTypes.TimestampType;
    }
    if (s.equalsIgnoreCase("date")) {
      return DataTypes.DateType;
    }
    if (s.equalsIgnoreCase("vector")) {
      return new VectorUDT();
    }
    if (s.equalsIgnoreCase("matrix")) {
      return new MatrixUDT();
    }
    if (s.contains(":") && s.split(":")[0].equalsIgnoreCase("array")) {
      return getArrayTypeRecurse(s,0);
    }
    return DataTypes.StringType;
  }

  public static DataType getArrayTypeRecurse(String s, int fromIdx) {
    if (s.contains(":") && s.split(":")[1].equalsIgnoreCase("array")) {
      fromIdx = s.indexOf(":", fromIdx);
      s = s.substring(fromIdx+1, s.length());
      return DataTypes.createArrayType(getArrayTypeRecurse(s,fromIdx));
    }
    return DataTypes.createArrayType(getsqlDataType(s.split(":")[1]));
  }
}
