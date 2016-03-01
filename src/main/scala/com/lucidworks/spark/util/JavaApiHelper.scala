package com.lucidworks.spark.util

import scala.reflect.ClassTag

object JavaApiHelper {

  // Copied from {@JavaApiHelper} in spark-cassandra-connector project
  /** Returns a `ClassTag` of a given runtime class. */
  def getClassTag[T](clazz: Class[T]): ClassTag[T] = ClassTag(clazz)
}
