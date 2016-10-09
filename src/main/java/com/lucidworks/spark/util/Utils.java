package com.lucidworks.spark.util;

import scala.collection.JavaConverters$;

public class Utils {
  /**
   * <p>
   *   Gets the class for the given name using the Context ClassLoader on this thread or,
   *   if not present, the ClassLoader that loaded Spark Solr.
   * </p>
   * <p>Copied from Spark org.apache.spark.util.Utils.scala</p>
   */
  public static Class<?> classForName(String className) throws ClassNotFoundException {
    return Class.forName(className, true, getContextOrSparkSolrClassLoader());
  }

  /**
   * <p>
   *   Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
   *   loaded spark-solr.
   * </p>
   * <p>
   *   This should be used whenever passing a ClassLoader to Class.forName or finding the currently
   *   active loader when setting up ClassLoader delegation chains.
   * </p>
   * <p>Copied from Spark org.apache.spark.util.Utils.scala</p>
   */
  public static ClassLoader getContextOrSparkSolrClassLoader() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    return classLoader == null ? getSparkSolrClassLoader() : classLoader;
  }

  /** Get the ClassLoader that loaded spark-solr. */
  public static ClassLoader getSparkSolrClassLoader() {
    return Utils.class.getClassLoader();
  }

  public static <K, V> scala.collection.immutable.Map<K, V> convertJavaMapToScalaImmmutableMap(final java.util.Map<K, V> m) {
    return JavaConverters$.MODULE$.mapAsScalaMapConverter(m).asScala().toMap(scala.Predef$.MODULE$.<scala.Tuple2<K, V>>conforms());
  }
}