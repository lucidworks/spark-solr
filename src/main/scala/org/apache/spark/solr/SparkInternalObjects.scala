package org.apache.spark.solr

import org.apache.spark.util.{AccumulatorContext, AccumulatorV2}

object SparkInternalObjects {

  def getAccumulatorById(id: Long): Option[AccumulatorV2[_, _]] = {
    AccumulatorContext.get(id)
  }

}
