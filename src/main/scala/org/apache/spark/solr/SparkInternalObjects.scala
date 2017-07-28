package org.apache.spark.solr

import org.apache.spark.util.{AccumulatorContext, AccumulatorV2}

object SparkInternalObjects {

  def getAccumulatorByName(name: String): Option[AccumulatorV2[_, _]] = {
    AccumulatorContext.lookForAccumulatorByName(name)
  }

}
