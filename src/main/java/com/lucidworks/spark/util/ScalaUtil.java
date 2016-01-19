package com.lucidworks.spark.util;

public class ScalaUtil {

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
}
