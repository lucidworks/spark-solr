/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lucidworks.spark.ml.param

import collection.JavaConverters._
import org.apache.spark.ml.param._


/**
 * Specialized version of [[Param[Map[String,String]]]] for Java.
 */
class StringStringMapParam
(parent: Params, name: String, doc: String, isValid: Map[String,String] => Boolean)
  extends Param[Map[String,String]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidation.alwaysTrue)

  /** Creates a param pair with a [[java.util.Map]] value (for Java and Python). */
  def w(value: java.util.Map[String,String]): ParamPair[Map[String,String]]
  = w(value.asScala.toMap)
}

/**
 * Specialized version of [[Param[Array[Map[String,String]]]]] for Java.
 */
class StringStringMapArrayParam
(parent: Params, name: String, doc: String, isValid: Array[Map[String,String]] => Boolean)
  extends Param[Array[Map[String,String]]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidation.alwaysTrue)

  /** Creates a param pair with a [[java.util.List]] of [[java.util.Map]] values
    * (for Java and Python). */
  def w(value: java.util.List[java.util.Map[String,String]]): ParamPair[Array[Map[String,String]]]
  = w(value.asScala.map(_.asScala.toMap).toArray)
}

/**
 * Factory methods for common validation functions for [[Param.isValid]].
 *
 * Copied (and renamed) from Spark's private ml.param.ParamValidators, for the alwaysTrue def
 */
object ParamValidation {
  /**
   * (private[param]) Default validation always return true
   *
   * Copied from Spark's ml.param.ParamValidators because it's inaccessible from here.
   */
  private[param] def alwaysTrue[T]: T => Boolean = (_: T) => true
}
