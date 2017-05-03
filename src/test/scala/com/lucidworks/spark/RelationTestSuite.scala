package com.lucidworks.spark

import java.util.UUID

import com.lucidworks.spark.util.ConfigurationConstants._
import com.lucidworks.spark.util.SolrCloudUtil
import com.typesafe.scalalogging.LazyLogging
import org.apache.solr.client.solrj.request.UpdateRequest
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.sql.types.{TimestampType, StringType, LongType, DoubleType}

class RelationTestSuite extends TestSuiteBuilder with LazyLogging {

  test("exclude_fields param") {
    val ratingsCollection = "ratings" + UUID.randomUUID().toString.replace('-','_')
    val numRatings = buildRatingsCollection(ratingsCollection)

    val ratingsDF = sparkSession.read.format("solr").options(
      Map("zkhost" -> zkHost, "collection" -> ratingsCollection, "exclude_fields" -> "id,*_timestamp,user*")).load
    val schema = ratingsDF.schema
    assert(schema.fields.length == 2)
    assert(schema.fields(0).name == "movie_id")
    assert(schema.fields(0).dataType == StringType)
    assert(schema.fields(1).name == "rating")
    assert(schema.fields(1).dataType == LongType)
    SolrCloudUtil.deleteCollection(ratingsCollection, cluster)
  }

  test("Unknown params") {
    val paramsToCheck = Set(SOLR_ZK_HOST_PARAM, SOLR_COLLECTION_PARAM, SOLR_QUERY_PARAM, ESCAPE_FIELDNAMES_PARAM, "fl", "q")
    val unknownParams = SolrRelation.checkUnknownParams(paramsToCheck)
    assert(unknownParams.size == 2)
    assert(unknownParams("q"))
    assert(unknownParams("fl"))
  }

  test("Test Long Field List with select handler") {
    val fl = "the_abcdefghijklmnopqrstuvwxyz_field_01,the_abcdefghijklmnopqrstuvwxyz_field_02,the_abcdefghijklmnopqrstuvwxyz_field_03,the_abcdefghijklmnopqrstuvwxyz_field_04,the_abcdefghijklmnopqrstuvwxyz_field_05,the_abcdefghijklmnopqrstuvwxyz_field_06,the_abcdefghijklmnopqrstuvwxyz_field_07,the_abcdefghijklmnopqrstuvwxyz_field_08,the_abcdefghijklmnopqrstuvwxyz_field_09,the_abcdefghijklmnopqrstuvwxyz_field_10,the_abcdefghijklmnopqrstuvwxyz_field_11,the_abcdefghijklmnopqrstuvwxyz_field_12,the_abcdefghijklmnopqrstuvwxyz_field_13,the_abcdefghijklmnopqrstuvwxyz_field_14,the_abcdefghijklmnopqrstuvwxyz_field_15,the_abcdefghijklmnopqrstuvwxyz_field_16,the_abcdefghijklmnopqrstuvwxyz_field_17,the_abcdefghijklmnopqrstuvwxyz_field_18,the_abcdefghijklmnopqrstuvwxyz_field_19,the_abcdefghijklmnopqrstuvwxyz_field_20,the_abcdefghijklmnopqrstuvwxyz_field_21,the_abcdefghijklmnopqrstuvwxyz_field_22,the_abcdefghijklmnopqrstuvwxyz_field_23,the_abcdefghijklmnopqrstuvwxyz_field_24,the_abcdefghijklmnopqrstuvwxyz_field_25,the_abcdefghijklmnopqrstuvwxyz_field_26,the_abcdefghijklmnopqrstuvwxyz_field_27,the_abcdefghijklmnopqrstuvwxyz_field_28,the_abcdefghijklmnopqrstuvwxyz_field_29,the_abcdefghijklmnopqrstuvwxyz_field_30,the_abcdefghijklmnopqrstuvwxyz_field_31,the_abcdefghijklmnopqrstuvwxyz_field_32,the_abcdefghijklmnopqrstuvwxyz_field_33,the_abcdefghijklmnopqrstuvwxyz_field_34,the_abcdefghijklmnopqrstuvwxyz_field_35,the_abcdefghijklmnopqrstuvwxyz_field_36,the_abcdefghijklmnopqrstuvwxyz_field_37,the_abcdefghijklmnopqrstuvwxyz_field_38,the_abcdefghijklmnopqrstuvwxyz_field_39,the_abcdefghijklmnopqrstuvwxyz_field_40,the_abcdefghijklmnopqrstuvwxyz_field_41,the_abcdefghijklmnopqrstuvwxyz_field_42,the_abcdefghijklmnopqrstuvwxyz_field_43,the_abcdefghijklmnopqrstuvwxyz_field_44,the_abcdefghijklmnopqrstuvwxyz_field_45,the_abcdefghijklmnopqrstuvwxyz_field_46,the_abcdefghijklmnopqrstuvwxyz_field_47,the_abcdefghijklmnopqrstuvwxyz_field_48,the_abcdefghijklmnopqrstuvwxyz_field_49,the_abcdefghijklmnopqrstuvwxyz_field_50,the_abcdefghijklmnopqrstuvwxyz_field_51,the_abcdefghijklmnopqrstuvwxyz_field_52,the_abcdefghijklmnopqrstuvwxyz_field_53,the_abcdefghijklmnopqrstuvwxyz_field_54,the_abcdefghijklmnopqrstuvwxyz_field_55,the_abcdefghijklmnopqrstuvwxyz_field_56,the_abcdefghijklmnopqrstuvwxyz_field_57,the_abcdefghijklmnopqrstuvwxyz_field_58,the_abcdefghijklmnopqrstuvwxyz_field_59,the_abcdefghijklmnopqrstuvwxyz_field_60,the_abcdefghijklmnopqrstuvwxyz_field_101,the_abcdefghijklmnopqrstuvwxyz_field_102,the_abcdefghijklmnopqrstuvwxyz_field_103,the_abcdefghijklmnopqrstuvwxyz_field_104,the_abcdefghijklmnopqrstuvwxyz_field_105,the_abcdefghijklmnopqrstuvwxyz_field_106,the_abcdefghijklmnopqrstuvwxyz_field_107,the_abcdefghijklmnopqrstuvwxyz_field_108,the_abcdefghijklmnopqrstuvwxyz_field_109,the_abcdefghijklmnopqrstuvwxyz_field_110,the_abcdefghijklmnopqrstuvwxyz_field_111,the_abcdefghijklmnopqrstuvwxyz_field_112,the_abcdefghijklmnopqrstuvwxyz_field_113,the_abcdefghijklmnopqrstuvwxyz_field_114,the_abcdefghijklmnopqrstuvwxyz_field_115,the_abcdefghijklmnopqrstuvwxyz_field_116,the_abcdefghijklmnopqrstuvwxyz_field_117,the_abcdefghijklmnopqrstuvwxyz_field_118,the_abcdefghijklmnopqrstuvwxyz_field_119,the_abcdefghijklmnopqrstuvwxyz_field_120,the_abcdefghijklmnopqrstuvwxyz_field_121,the_abcdefghijklmnopqrstuvwxyz_field_122,the_abcdefghijklmnopqrstuvwxyz_field_123,the_abcdefghijklmnopqrstuvwxyz_field_124,the_abcdefghijklmnopqrstuvwxyz_field_125,the_abcdefghijklmnopqrstuvwxyz_field_126,the_abcdefghijklmnopqrstuvwxyz_field_127,the_abcdefghijklmnopqrstuvwxyz_field_128,the_abcdefghijklmnopqrstuvwxyz_field_129,the_abcdefghijklmnopqrstuvwxyz_field_130,the_abcdefghijklmnopqrstuvwxyz_field_131,the_abcdefghijklmnopqrstuvwxyz_field_132,the_abcdefghijklmnopqrstuvwxyz_field_133,the_abcdefghijklmnopqrstuvwxyz_field_134,the_abcdefghijklmnopqrstuvwxyz_field_135,the_abcdefghijklmnopqrstuvwxyz_field_136,the_abcdefghijklmnopqrstuvwxyz_field_137,the_abcdefghijklmnopqrstuvwxyz_field_138,the_abcdefghijklmnopqrstuvwxyz_field_139,the_abcdefghijklmnopqrstuvwxyz_field_140,the_abcdefghijklmnopqrstuvwxyz_field_141,the_abcdefghijklmnopqrstuvwxyz_field_142,the_abcdefghijklmnopqrstuvwxyz_field_143,the_abcdefghijklmnopqrstuvwxyz_field_144,the_abcdefghijklmnopqrstuvwxyz_field_145,the_abcdefghijklmnopqrstuvwxyz_field_146,the_abcdefghijklmnopqrstuvwxyz_field_147,the_abcdefghijklmnopqrstuvwxyz_field_148,the_abcdefghijklmnopqrstuvwxyz_field_149,the_abcdefghijklmnopqrstuvwxyz_field_150,the_abcdefghijklmnopqrstuvwxyz_field_151,the_abcdefghijklmnopqrstuvwxyz_field_152,the_abcdefghijklmnopqrstuvwxyz_field_153,the_abcdefghijklmnopqrstuvwxyz_field_154,the_abcdefghijklmnopqrstuvwxyz_field_155,the_abcdefghijklmnopqrstuvwxyz_field_156,the_abcdefghijklmnopqrstuvwxyz_field_157,the_abcdefghijklmnopqrstuvwxyz_field_158,the_abcdefghijklmnopqrstuvwxyz_field_159,the_abcdefghijklmnopqrstuvwxyz_field_160,the_abcdefghijklmnopqrstuvwxyz_field_201,the_abcdefghijklmnopqrstuvwxyz_field_202,the_abcdefghijklmnopqrstuvwxyz_field_203,the_abcdefghijklmnopqrstuvwxyz_field_204,the_abcdefghijklmnopqrstuvwxyz_field_205,the_abcdefghijklmnopqrstuvwxyz_field_206,the_abcdefghijklmnopqrstuvwxyz_field_207,the_abcdefghijklmnopqrstuvwxyz_field_208,the_abcdefghijklmnopqrstuvwxyz_field_209,the_abcdefghijklmnopqrstuvwxyz_field_210,the_abcdefghijklmnopqrstuvwxyz_field_211,the_abcdefghijklmnopqrstuvwxyz_field_212,the_abcdefghijklmnopqrstuvwxyz_field_213,the_abcdefghijklmnopqrstuvwxyz_field_214,the_abcdefghijklmnopqrstuvwxyz_field_215,the_abcdefghijklmnopqrstuvwxyz_field_216,the_abcdefghijklmnopqrstuvwxyz_field_217,the_abcdefghijklmnopqrstuvwxyz_field_218,the_abcdefghijklmnopqrstuvwxyz_field_219,the_abcdefghijklmnopqrstuvwxyz_field_220,the_abcdefghijklmnopqrstuvwxyz_field_221,the_abcdefghijklmnopqrstuvwxyz_field_222,the_abcdefghijklmnopqrstuvwxyz_field_223,the_abcdefghijklmnopqrstuvwxyz_field_224,the_abcdefghijklmnopqrstuvwxyz_field_225,the_abcdefghijklmnopqrstuvwxyz_field_226,the_abcdefghijklmnopqrstuvwxyz_field_227,the_abcdefghijklmnopqrstuvwxyz_field_228,the_abcdefghijklmnopqrstuvwxyz_field_229,the_abcdefghijklmnopqrstuvwxyz_field_230,the_abcdefghijklmnopqrstuvwxyz_field_231,the_abcdefghijklmnopqrstuvwxyz_field_232,the_abcdefghijklmnopqrstuvwxyz_field_233,the_abcdefghijklmnopqrstuvwxyz_field_234,the_abcdefghijklmnopqrstuvwxyz_field_235,the_abcdefghijklmnopqrstuvwxyz_field_236,the_abcdefghijklmnopqrstuvwxyz_field_237,the_abcdefghijklmnopqrstuvwxyz_field_238,the_abcdefghijklmnopqrstuvwxyz_field_239,the_abcdefghijklmnopqrstuvwxyz_field_240,the_abcdefghijklmnopqrstuvwxyz_field_241,the_abcdefghijklmnopqrstuvwxyz_field_242,the_abcdefghijklmnopqrstuvwxyz_field_243,the_abcdefghijklmnopqrstuvwxyz_field_244,the_abcdefghijklmnopqrstuvwxyz_field_245,the_abcdefghijklmnopqrstuvwxyz_field_246,the_abcdefghijklmnopqrstuvwxyz_field_247,the_abcdefghijklmnopqrstuvwxyz_field_248,the_abcdefghijklmnopqrstuvwxyz_field_249,the_abcdefghijklmnopqrstuvwxyz_field_250,the_abcdefghijklmnopqrstuvwxyz_field_251,the_abcdefghijklmnopqrstuvwxyz_field_252,the_abcdefghijklmnopqrstuvwxyz_field_253,the_abcdefghijklmnopqrstuvwxyz_field_254,the_abcdefghijklmnopqrstuvwxyz_field_255,the_abcdefghijklmnopqrstuvwxyz_field_256,the_abcdefghijklmnopqrstuvwxyz_field_257,the_abcdefghijklmnopqrstuvwxyz_field_258,the_abcdefghijklmnopqrstuvwxyz_field_259,the_abcdefghijklmnopqrstuvwxyz_field_260,the_abcdefghijklmnopqrstuvwxyz_field_301,the_abcdefghijklmnopqrstuvwxyz_field_302,the_abcdefghijklmnopqrstuvwxyz_field_303,the_abcdefghijklmnopqrstuvwxyz_field_304,the_abcdefghijklmnopqrstuvwxyz_field_305,the_abcdefghijklmnopqrstuvwxyz_field_306,the_abcdefghijklmnopqrstuvwxyz_field_307,the_abcdefghijklmnopqrstuvwxyz_field_308,the_abcdefghijklmnopqrstuvwxyz_field_309,the_abcdefghijklmnopqrstuvwxyz_field_310,the_abcdefghijklmnopqrstuvwxyz_field_311,the_abcdefghijklmnopqrstuvwxyz_field_312,the_abcdefghijklmnopqrstuvwxyz_field_313,the_abcdefghijklmnopqrstuvwxyz_field_314,the_abcdefghijklmnopqrstuvwxyz_field_315,the_abcdefghijklmnopqrstuvwxyz_field_316,the_abcdefghijklmnopqrstuvwxyz_field_317,the_abcdefghijklmnopqrstuvwxyz_field_318,the_abcdefghijklmnopqrstuvwxyz_field_319,the_abcdefghijklmnopqrstuvwxyz_field_320,the_abcdefghijklmnopqrstuvwxyz_field_321,the_abcdefghijklmnopqrstuvwxyz_field_322,the_abcdefghijklmnopqrstuvwxyz_field_323,the_abcdefghijklmnopqrstuvwxyz_field_324,the_abcdefghijklmnopqrstuvwxyz_field_325,the_abcdefghijklmnopqrstuvwxyz_field_326,the_abcdefghijklmnopqrstuvwxyz_field_327,the_abcdefghijklmnopqrstuvwxyz_field_328,the_abcdefghijklmnopqrstuvwxyz_field_329,the_abcdefghijklmnopqrstuvwxyz_field_330,the_abcdefghijklmnopqrstuvwxyz_field_331,the_abcdefghijklmnopqrstuvwxyz_field_332,the_abcdefghijklmnopqrstuvwxyz_field_333,the_abcdefghijklmnopqrstuvwxyz_field_334,the_abcdefghijklmnopqrstuvwxyz_field_335,the_abcdefghijklmnopqrstuvwxyz_field_336,the_abcdefghijklmnopqrstuvwxyz_field_337,the_abcdefghijklmnopqrstuvwxyz_field_338,the_abcdefghijklmnopqrstuvwxyz_field_339,the_abcdefghijklmnopqrstuvwxyz_field_340,the_abcdefghijklmnopqrstuvwxyz_field_341,the_abcdefghijklmnopqrstuvwxyz_field_342,the_abcdefghijklmnopqrstuvwxyz_field_343,the_abcdefghijklmnopqrstuvwxyz_field_344,the_abcdefghijklmnopqrstuvwxyz_field_345,the_abcdefghijklmnopqrstuvwxyz_field_346,the_abcdefghijklmnopqrstuvwxyz_field_347,the_abcdefghijklmnopqrstuvwxyz_field_348,the_abcdefghijklmnopqrstuvwxyz_field_349,the_abcdefghijklmnopqrstuvwxyz_field_350,the_abcdefghijklmnopqrstuvwxyz_field_351,the_abcdefghijklmnopqrstuvwxyz_field_352,the_abcdefghijklmnopqrstuvwxyz_field_353,the_abcdefghijklmnopqrstuvwxyz_field_354,the_abcdefghijklmnopqrstuvwxyz_field_355,the_abcdefghijklmnopqrstuvwxyz_field_356,the_abcdefghijklmnopqrstuvwxyz_field_357,the_abcdefghijklmnopqrstuvwxyz_field_358,the_abcdefghijklmnopqrstuvwxyz_field_359,the_abcdefghijklmnopqrstuvwxyz_field_360"
    val fields = fl.split(",")
    val collectionName = "testLongFieldList" + UUID.randomUUID().toString
    SolrCloudUtil.buildCollection(zkHost, collectionName, 1, 1, cloudClient, sc)
    try {
      // add some docs
      val updateRequest = new UpdateRequest()
      for (i <- 0 until 100) {
        val doc = new SolrInputDocument()
        doc.setField("id", "id-"+i)
        fields.foreach(fname => doc.setField(fname, "X"))
        updateRequest.add(doc)
        doc
      }
      updateRequest.process(cloudClient, collectionName)
      updateRequest.commit(cloudClient, collectionName)

      val testDF = sparkSession.read.format("solr").options(
        Map("zkhost" -> zkHost, "collection" -> collectionName,
          "query" -> "the_abcdefghijklmnopqrstuvwxyz_field_01:[* TO *]", "fields" -> fl, "request_handler" -> "/select", "splits" -> "true")).load
      //testDF.printSchema
      val schema = testDF.schema
      assert(schema != null)
      fields.foreach(_ => schema.fieldIndex(_)) // throws exception if not found
      assert(testDF.collectAsList().size == 100)

      val searchExpr = s"""
                          |search(${collectionName},
                          |           q="the_abcdefghijklmnopqrstuvwxyz_field_01:[* TO *]",
                          |           fl="${fl}",
                          |           sort="the_abcdefghijklmnopqrstuvwxyz_field_01 asc",
                          |           qt="/export")
      """.stripMargin
      val exprDF = sparkSession.read.format("solr").options(
        Map("zkhost" -> zkHost, "collection" -> collectionName, "expr" -> searchExpr)).load
      val exprSchema = exprDF.schema
      //exprDF.printSchema
      fields.foreach(_ => exprSchema.fieldIndex(_)) // throws exception if not found
      assert(exprDF.collectAsList().size == 100)
    } finally {
      SolrCloudUtil.deleteCollection(collectionName, cluster)
    }
  }

  test("Test Long Field List") {
    val fl = "the_abcdefghijklmnopqrstuvwxyz_field_01,the_abcdefghijklmnopqrstuvwxyz_field_02,the_abcdefghijklmnopqrstuvwxyz_field_03,the_abcdefghijklmnopqrstuvwxyz_field_04,the_abcdefghijklmnopqrstuvwxyz_field_05,the_abcdefghijklmnopqrstuvwxyz_field_06,the_abcdefghijklmnopqrstuvwxyz_field_07,the_abcdefghijklmnopqrstuvwxyz_field_08,the_abcdefghijklmnopqrstuvwxyz_field_09,the_abcdefghijklmnopqrstuvwxyz_field_10,the_abcdefghijklmnopqrstuvwxyz_field_11,the_abcdefghijklmnopqrstuvwxyz_field_12,the_abcdefghijklmnopqrstuvwxyz_field_13,the_abcdefghijklmnopqrstuvwxyz_field_14,the_abcdefghijklmnopqrstuvwxyz_field_15,the_abcdefghijklmnopqrstuvwxyz_field_16,the_abcdefghijklmnopqrstuvwxyz_field_17,the_abcdefghijklmnopqrstuvwxyz_field_18,the_abcdefghijklmnopqrstuvwxyz_field_19,the_abcdefghijklmnopqrstuvwxyz_field_20,the_abcdefghijklmnopqrstuvwxyz_field_21,the_abcdefghijklmnopqrstuvwxyz_field_22,the_abcdefghijklmnopqrstuvwxyz_field_23,the_abcdefghijklmnopqrstuvwxyz_field_24,the_abcdefghijklmnopqrstuvwxyz_field_25,the_abcdefghijklmnopqrstuvwxyz_field_26,the_abcdefghijklmnopqrstuvwxyz_field_27,the_abcdefghijklmnopqrstuvwxyz_field_28,the_abcdefghijklmnopqrstuvwxyz_field_29,the_abcdefghijklmnopqrstuvwxyz_field_30,the_abcdefghijklmnopqrstuvwxyz_field_31,the_abcdefghijklmnopqrstuvwxyz_field_32,the_abcdefghijklmnopqrstuvwxyz_field_33,the_abcdefghijklmnopqrstuvwxyz_field_34,the_abcdefghijklmnopqrstuvwxyz_field_35,the_abcdefghijklmnopqrstuvwxyz_field_36,the_abcdefghijklmnopqrstuvwxyz_field_37,the_abcdefghijklmnopqrstuvwxyz_field_38,the_abcdefghijklmnopqrstuvwxyz_field_39,the_abcdefghijklmnopqrstuvwxyz_field_40,the_abcdefghijklmnopqrstuvwxyz_field_41,the_abcdefghijklmnopqrstuvwxyz_field_42,the_abcdefghijklmnopqrstuvwxyz_field_43,the_abcdefghijklmnopqrstuvwxyz_field_44,the_abcdefghijklmnopqrstuvwxyz_field_45,the_abcdefghijklmnopqrstuvwxyz_field_46,the_abcdefghijklmnopqrstuvwxyz_field_47,the_abcdefghijklmnopqrstuvwxyz_field_48,the_abcdefghijklmnopqrstuvwxyz_field_49,the_abcdefghijklmnopqrstuvwxyz_field_50,the_abcdefghijklmnopqrstuvwxyz_field_51,the_abcdefghijklmnopqrstuvwxyz_field_52,the_abcdefghijklmnopqrstuvwxyz_field_53,the_abcdefghijklmnopqrstuvwxyz_field_54,the_abcdefghijklmnopqrstuvwxyz_field_55,the_abcdefghijklmnopqrstuvwxyz_field_56,the_abcdefghijklmnopqrstuvwxyz_field_57,the_abcdefghijklmnopqrstuvwxyz_field_58,the_abcdefghijklmnopqrstuvwxyz_field_59,the_abcdefghijklmnopqrstuvwxyz_field_60,the_abcdefghijklmnopqrstuvwxyz_field_101,the_abcdefghijklmnopqrstuvwxyz_field_102,the_abcdefghijklmnopqrstuvwxyz_field_103,the_abcdefghijklmnopqrstuvwxyz_field_104,the_abcdefghijklmnopqrstuvwxyz_field_105,the_abcdefghijklmnopqrstuvwxyz_field_106,the_abcdefghijklmnopqrstuvwxyz_field_107,the_abcdefghijklmnopqrstuvwxyz_field_108,the_abcdefghijklmnopqrstuvwxyz_field_109,the_abcdefghijklmnopqrstuvwxyz_field_110,the_abcdefghijklmnopqrstuvwxyz_field_111,the_abcdefghijklmnopqrstuvwxyz_field_112,the_abcdefghijklmnopqrstuvwxyz_field_113,the_abcdefghijklmnopqrstuvwxyz_field_114,the_abcdefghijklmnopqrstuvwxyz_field_115,the_abcdefghijklmnopqrstuvwxyz_field_116,the_abcdefghijklmnopqrstuvwxyz_field_117,the_abcdefghijklmnopqrstuvwxyz_field_118,the_abcdefghijklmnopqrstuvwxyz_field_119,the_abcdefghijklmnopqrstuvwxyz_field_120,the_abcdefghijklmnopqrstuvwxyz_field_121,the_abcdefghijklmnopqrstuvwxyz_field_122,the_abcdefghijklmnopqrstuvwxyz_field_123,the_abcdefghijklmnopqrstuvwxyz_field_124,the_abcdefghijklmnopqrstuvwxyz_field_125,the_abcdefghijklmnopqrstuvwxyz_field_126,the_abcdefghijklmnopqrstuvwxyz_field_127,the_abcdefghijklmnopqrstuvwxyz_field_128,the_abcdefghijklmnopqrstuvwxyz_field_129,the_abcdefghijklmnopqrstuvwxyz_field_130,the_abcdefghijklmnopqrstuvwxyz_field_131,the_abcdefghijklmnopqrstuvwxyz_field_132,the_abcdefghijklmnopqrstuvwxyz_field_133,the_abcdefghijklmnopqrstuvwxyz_field_134,the_abcdefghijklmnopqrstuvwxyz_field_135,the_abcdefghijklmnopqrstuvwxyz_field_136,the_abcdefghijklmnopqrstuvwxyz_field_137,the_abcdefghijklmnopqrstuvwxyz_field_138,the_abcdefghijklmnopqrstuvwxyz_field_139,the_abcdefghijklmnopqrstuvwxyz_field_140,the_abcdefghijklmnopqrstuvwxyz_field_141,the_abcdefghijklmnopqrstuvwxyz_field_142,the_abcdefghijklmnopqrstuvwxyz_field_143,the_abcdefghijklmnopqrstuvwxyz_field_144,the_abcdefghijklmnopqrstuvwxyz_field_145,the_abcdefghijklmnopqrstuvwxyz_field_146,the_abcdefghijklmnopqrstuvwxyz_field_147,the_abcdefghijklmnopqrstuvwxyz_field_148,the_abcdefghijklmnopqrstuvwxyz_field_149,the_abcdefghijklmnopqrstuvwxyz_field_150,the_abcdefghijklmnopqrstuvwxyz_field_151,the_abcdefghijklmnopqrstuvwxyz_field_152,the_abcdefghijklmnopqrstuvwxyz_field_153,the_abcdefghijklmnopqrstuvwxyz_field_154,the_abcdefghijklmnopqrstuvwxyz_field_155,the_abcdefghijklmnopqrstuvwxyz_field_156,the_abcdefghijklmnopqrstuvwxyz_field_157,the_abcdefghijklmnopqrstuvwxyz_field_158,the_abcdefghijklmnopqrstuvwxyz_field_159,the_abcdefghijklmnopqrstuvwxyz_field_160,the_abcdefghijklmnopqrstuvwxyz_field_201,the_abcdefghijklmnopqrstuvwxyz_field_202,the_abcdefghijklmnopqrstuvwxyz_field_203,the_abcdefghijklmnopqrstuvwxyz_field_204,the_abcdefghijklmnopqrstuvwxyz_field_205,the_abcdefghijklmnopqrstuvwxyz_field_206,the_abcdefghijklmnopqrstuvwxyz_field_207,the_abcdefghijklmnopqrstuvwxyz_field_208,the_abcdefghijklmnopqrstuvwxyz_field_209,the_abcdefghijklmnopqrstuvwxyz_field_210,the_abcdefghijklmnopqrstuvwxyz_field_211,the_abcdefghijklmnopqrstuvwxyz_field_212,the_abcdefghijklmnopqrstuvwxyz_field_213,the_abcdefghijklmnopqrstuvwxyz_field_214,the_abcdefghijklmnopqrstuvwxyz_field_215,the_abcdefghijklmnopqrstuvwxyz_field_216,the_abcdefghijklmnopqrstuvwxyz_field_217,the_abcdefghijklmnopqrstuvwxyz_field_218,the_abcdefghijklmnopqrstuvwxyz_field_219,the_abcdefghijklmnopqrstuvwxyz_field_220,the_abcdefghijklmnopqrstuvwxyz_field_221,the_abcdefghijklmnopqrstuvwxyz_field_222,the_abcdefghijklmnopqrstuvwxyz_field_223,the_abcdefghijklmnopqrstuvwxyz_field_224,the_abcdefghijklmnopqrstuvwxyz_field_225,the_abcdefghijklmnopqrstuvwxyz_field_226,the_abcdefghijklmnopqrstuvwxyz_field_227,the_abcdefghijklmnopqrstuvwxyz_field_228,the_abcdefghijklmnopqrstuvwxyz_field_229,the_abcdefghijklmnopqrstuvwxyz_field_230,the_abcdefghijklmnopqrstuvwxyz_field_231,the_abcdefghijklmnopqrstuvwxyz_field_232,the_abcdefghijklmnopqrstuvwxyz_field_233,the_abcdefghijklmnopqrstuvwxyz_field_234,the_abcdefghijklmnopqrstuvwxyz_field_235,the_abcdefghijklmnopqrstuvwxyz_field_236,the_abcdefghijklmnopqrstuvwxyz_field_237,the_abcdefghijklmnopqrstuvwxyz_field_238,the_abcdefghijklmnopqrstuvwxyz_field_239,the_abcdefghijklmnopqrstuvwxyz_field_240,the_abcdefghijklmnopqrstuvwxyz_field_241,the_abcdefghijklmnopqrstuvwxyz_field_242,the_abcdefghijklmnopqrstuvwxyz_field_243,the_abcdefghijklmnopqrstuvwxyz_field_244,the_abcdefghijklmnopqrstuvwxyz_field_245,the_abcdefghijklmnopqrstuvwxyz_field_246,the_abcdefghijklmnopqrstuvwxyz_field_247,the_abcdefghijklmnopqrstuvwxyz_field_248,the_abcdefghijklmnopqrstuvwxyz_field_249,the_abcdefghijklmnopqrstuvwxyz_field_250,the_abcdefghijklmnopqrstuvwxyz_field_251,the_abcdefghijklmnopqrstuvwxyz_field_252,the_abcdefghijklmnopqrstuvwxyz_field_253,the_abcdefghijklmnopqrstuvwxyz_field_254,the_abcdefghijklmnopqrstuvwxyz_field_255,the_abcdefghijklmnopqrstuvwxyz_field_256,the_abcdefghijklmnopqrstuvwxyz_field_257,the_abcdefghijklmnopqrstuvwxyz_field_258,the_abcdefghijklmnopqrstuvwxyz_field_259,the_abcdefghijklmnopqrstuvwxyz_field_260,the_abcdefghijklmnopqrstuvwxyz_field_301,the_abcdefghijklmnopqrstuvwxyz_field_302,the_abcdefghijklmnopqrstuvwxyz_field_303,the_abcdefghijklmnopqrstuvwxyz_field_304,the_abcdefghijklmnopqrstuvwxyz_field_305,the_abcdefghijklmnopqrstuvwxyz_field_306,the_abcdefghijklmnopqrstuvwxyz_field_307,the_abcdefghijklmnopqrstuvwxyz_field_308,the_abcdefghijklmnopqrstuvwxyz_field_309,the_abcdefghijklmnopqrstuvwxyz_field_310,the_abcdefghijklmnopqrstuvwxyz_field_311,the_abcdefghijklmnopqrstuvwxyz_field_312,the_abcdefghijklmnopqrstuvwxyz_field_313,the_abcdefghijklmnopqrstuvwxyz_field_314,the_abcdefghijklmnopqrstuvwxyz_field_315,the_abcdefghijklmnopqrstuvwxyz_field_316,the_abcdefghijklmnopqrstuvwxyz_field_317,the_abcdefghijklmnopqrstuvwxyz_field_318,the_abcdefghijklmnopqrstuvwxyz_field_319,the_abcdefghijklmnopqrstuvwxyz_field_320,the_abcdefghijklmnopqrstuvwxyz_field_321,the_abcdefghijklmnopqrstuvwxyz_field_322,the_abcdefghijklmnopqrstuvwxyz_field_323,the_abcdefghijklmnopqrstuvwxyz_field_324,the_abcdefghijklmnopqrstuvwxyz_field_325,the_abcdefghijklmnopqrstuvwxyz_field_326,the_abcdefghijklmnopqrstuvwxyz_field_327,the_abcdefghijklmnopqrstuvwxyz_field_328,the_abcdefghijklmnopqrstuvwxyz_field_329,the_abcdefghijklmnopqrstuvwxyz_field_330,the_abcdefghijklmnopqrstuvwxyz_field_331,the_abcdefghijklmnopqrstuvwxyz_field_332,the_abcdefghijklmnopqrstuvwxyz_field_333,the_abcdefghijklmnopqrstuvwxyz_field_334,the_abcdefghijklmnopqrstuvwxyz_field_335,the_abcdefghijklmnopqrstuvwxyz_field_336,the_abcdefghijklmnopqrstuvwxyz_field_337,the_abcdefghijklmnopqrstuvwxyz_field_338,the_abcdefghijklmnopqrstuvwxyz_field_339,the_abcdefghijklmnopqrstuvwxyz_field_340,the_abcdefghijklmnopqrstuvwxyz_field_341,the_abcdefghijklmnopqrstuvwxyz_field_342,the_abcdefghijklmnopqrstuvwxyz_field_343,the_abcdefghijklmnopqrstuvwxyz_field_344,the_abcdefghijklmnopqrstuvwxyz_field_345,the_abcdefghijklmnopqrstuvwxyz_field_346,the_abcdefghijklmnopqrstuvwxyz_field_347,the_abcdefghijklmnopqrstuvwxyz_field_348,the_abcdefghijklmnopqrstuvwxyz_field_349,the_abcdefghijklmnopqrstuvwxyz_field_350,the_abcdefghijklmnopqrstuvwxyz_field_351,the_abcdefghijklmnopqrstuvwxyz_field_352,the_abcdefghijklmnopqrstuvwxyz_field_353,the_abcdefghijklmnopqrstuvwxyz_field_354,the_abcdefghijklmnopqrstuvwxyz_field_355,the_abcdefghijklmnopqrstuvwxyz_field_356,the_abcdefghijklmnopqrstuvwxyz_field_357,the_abcdefghijklmnopqrstuvwxyz_field_358,the_abcdefghijklmnopqrstuvwxyz_field_359,the_abcdefghijklmnopqrstuvwxyz_field_360"
    val fields = fl.split(",")
    val collectionName = "testLongFieldList" + UUID.randomUUID().toString
    SolrCloudUtil.buildCollection(zkHost, collectionName, 1, 1, cloudClient, sc)
    try {
      // add some docs
      val updateRequest = new UpdateRequest()
      for (i <- 0 until 100) {
        val doc = new SolrInputDocument()
        doc.setField("id", "id-"+i)
        fields.foreach(fname => doc.setField(fname, "X"))
        updateRequest.add(doc)
        doc
      }
      updateRequest.process(cloudClient, collectionName)
      updateRequest.commit(cloudClient, collectionName)

      val testDF = sparkSession.read.format("solr").options(
        Map("zkhost" -> zkHost, "collection" -> collectionName,
          "query" -> "the_abcdefghijklmnopqrstuvwxyz_field_01:[* TO *]", "fields" -> fl)).load
      //testDF.printSchema
      val schema = testDF.schema
      assert(schema != null)
      fields.foreach(_ => schema.fieldIndex(_)) // throws exception if not found
      assert(testDF.collectAsList().size == 100)

      val searchExpr = s"""
          |search(${collectionName},
          |           q="the_abcdefghijklmnopqrstuvwxyz_field_01:[* TO *]",
          |           fl="${fl}",
          |           sort="the_abcdefghijklmnopqrstuvwxyz_field_01 asc",
          |           qt="/export")
      """.stripMargin
      val exprDF = sparkSession.read.format("solr").options(
        Map("zkhost" -> zkHost, "collection" -> collectionName, "expr" -> searchExpr)).load
      val exprSchema = exprDF.schema
      //exprDF.printSchema
      fields.foreach(_ => exprSchema.fieldIndex(_)) // throws exception if not found
      assert(exprDF.collectAsList().size == 100)
    } finally {
      SolrCloudUtil.deleteCollection(collectionName, cluster)
    }
  }

  test("movielens data analytics") {

    // movies: id, movie_id, title
    val moviesCollection = "movies" + UUID.randomUUID().toString.replace('-','_')
    val numMovies = buildMoviesCollection(moviesCollection)

    var moviesDF = sparkSession.read.format("solr").options(
      Map("zkhost" -> zkHost, "collection" -> moviesCollection, "fields" -> "movie_id,title")).load
    assert(moviesDF.count == numMovies)
    var schema = moviesDF.schema
    assert(schema.fields.length == 2)
    assert(schema.fields(0).name == "movie_id")
    assert(schema.fields(0).dataType == StringType)
    assert(schema.fields(1).name == "title")
    assert(schema.fields(1).dataType == StringType)

    // movie ratings: id, user_id, movie_id, rating, rating_timestamp
    val ratingsCollection = "ratings" + UUID.randomUUID().toString.replace('-','_')
    val numRatings = buildRatingsCollection(ratingsCollection)

    var ratingsDF = sparkSession.read.format("solr").options(
      Map("zkhost" -> zkHost, "collection" -> ratingsCollection, "fields" -> "user_id,movie_id,rating,rating_timestamp")).load
    assert(ratingsDF.count == numRatings)
    schema = ratingsDF.schema
    assert(schema.fields.length == 4)
    assert(schema.fields(0).name == "user_id")
    assert(schema.fields(0).dataType == StringType)
    assert(schema.fields(1).name == "movie_id")
    assert(schema.fields(1).dataType == StringType)
    assert(schema.fields(2).name == "rating")
    assert(schema.fields(2).dataType == LongType)
    assert(schema.fields(3).name == "rating_timestamp")
    assert(schema.fields(3).dataType == TimestampType)

    // perform a facet streaming operation on the ratings collection to get avg rating for each movie
    val facetExpr = s"""
      |  facet(
      |    ${ratingsCollection},
      |    q="*:*",
      |    buckets="movie_id",
      |    bucketSorts="count(*) desc",
      |    bucketSizeLimit=100,
      |    count(*),
      |    avg(rating)
      |  )
    """.stripMargin
    var ratingFacetsDF = sparkSession.read.format("solr").options(
      Map("zkhost" -> zkHost, "collection" -> ratingsCollection, "expr" -> facetExpr)).load
    assert(ratingFacetsDF.count == 2)
    schema = ratingFacetsDF.schema
    assert(schema.fields.length == 3)
    assert(schema.fieldNames(0) == "avg(rating)")
    assert(schema.fields(0).dataType == DoubleType)
    assert(schema.fieldNames(1) == "count(*)")
    assert(schema.fields(1).dataType == LongType)
    assert(schema.fieldNames(2) == "movie_id")
    assert(schema.fields(2).dataType == StringType)

    val hashJoinExpr =
      s"""
         |parallel(${ratingsCollection},
         | hashJoin(
         |    search(${ratingsCollection},
         |           q="*:*",
         |           fl="movie_id,user_id,rating",
         |           sort="movie_id asc",
         |           qt="/export",
         |           partitionKeys="movie_id"),
         |    hashed=search(${moviesCollection},
         |                  q="*:*",
         |                  fl="movie_id,title",
         |                  sort="movie_id asc",
         |                  qt="/export",
         |                  partitionKeys="movie_id"),
         |    on="movie_id"
         |  ), workers="1", sort="movie_id asc")
       """.stripMargin
    var joinDF = sparkSession.read.format("solr").options(
      Map("zkhost" -> zkHost, "collection" -> ratingsCollection, "expr" -> hashJoinExpr)).load
    assert(joinDF.count == numRatings)
    joinDF.printSchema()
    schema = joinDF.schema
    assert(schema.fields.length == 4)
    assert(schema.fields(0).name == "movie_id")
    assert(schema.fields(0).dataType == StringType)
    assert(schema.fields(1).name == "rating")
    assert(schema.fields(1).dataType == LongType)
    assert(schema.fields(2).name == "title")
    assert(schema.fields(2).dataType == StringType)
    assert(schema.fields(3).name == "user_id")
    assert(schema.fields(3).dataType == StringType)

    val sqlStmt =
      s"""
         |    SELECT movie_id, COUNT(*) as agg_count, avg(rating) as avg_rating, sum(rating) as sum_rating, min(rating) as min_rating, max(rating) as max_rating
         |      FROM ${ratingsCollection}
         |  GROUP BY movie_id
         |  ORDER BY movie_id asc
       """.stripMargin
    var sqlDF = sparkSession.read.format("solr").options(
      Map("zkhost" -> zkHost, "sql" -> sqlStmt)).load
    sqlDF.printSchema
    sqlDF.show

    assert(sqlDF.count == 2)
    var sqlSchema = sqlDF.schema
    assert(sqlSchema != null)
    assert(sqlSchema.fields.length == 6)
    assert(sqlSchema.fields(0).name == "agg_count")
    assert(sqlSchema.fields(0).dataType == LongType)
    assert(sqlSchema.fields(1).name == "avg_rating")
    assert(sqlSchema.fields(1).dataType == DoubleType)
    assert(sqlSchema.fields(2).name == "max_rating")
    assert(sqlSchema.fields(2).dataType == DoubleType)
    assert(sqlSchema.fields(3).name == "min_rating")
    assert(sqlSchema.fields(3).dataType == DoubleType)
    assert(sqlSchema.fields(4).name == "movie_id")
    assert(sqlSchema.fields(4).dataType == StringType)
    assert(sqlSchema.fields(5).name == "sum_rating")
    assert(sqlSchema.fields(5).dataType == DoubleType)
    val sqlResults = sqlDF.collectAsList()
    val row0 = sqlResults.get(0)
    assert(row0.getString(4) == "movie200")

    // proper handling of select expression decorator
    var selectExpr = s"""select(
      facet(
        ${moviesCollection},
        q="*:*",
        buckets="title",
        bucketSorts="count(*) desc",
        bucketSizeLimit=100,
        count(*)
      ),
      title,
      count(*) as the_count)"""
    var selectExprDF =
      sparkSession.read.format("solr").options(Map("zkhost" -> zkHost, "collection" -> moviesCollection, "expr" -> selectExpr)).load
    selectExprDF.printSchema()
    schema = selectExprDF.schema
    assert(schema.fields.length == 2)
    assert(schema.fields(0).name == "the_count")
    assert(schema.fields(0).dataType == LongType)
    assert(schema.fields(1).name == "title")
    assert(schema.fields(1).dataType == StringType)

    var selectExprResults = selectExprDF.collectAsList()
    assert(selectExprResults.size() == 2)
    var selectExprRow0 = selectExprResults.get(0)
    assert(selectExprRow0.getLong(0) == 1L)
    assert(selectExprRow0.getString(1) == "Moneyball")
    var selectExprRow1 = selectExprResults.get(1)
    assert(selectExprRow1.getLong(0) == 1L)
    assert(selectExprRow1.getString(1) == "The Big Short")

    selectExpr = s"""select(
      facet(
        ${ratingsCollection},
        q="*:*",
        buckets="rating",
        bucketSorts="count(*) desc",
        bucketSizeLimit=100,
        count(*),
        sum(rating),
        min(rating),
        max(rating),
        avg(rating)
      ),
      rating,
      count(*) as the_count,
      sum(rating) as the_sum,
      min(rating) as the_min,
      max(rating) as the_max,
      avg(rating) as the_avg)
    """
    selectExprDF =
      sparkSession.read.format("solr").options(Map("zkhost" -> zkHost, "collection" -> ratingsCollection, "expr" -> selectExpr)).load
    selectExprDF.printSchema()
    schema = selectExprDF.schema
    assert(schema.fields.length == 6)
    assert(schema.fields(0).name == "rating")
    assert(schema.fields(0).dataType == LongType)
    assert(schema.fields(1).name == "the_avg")
    assert(schema.fields(1).dataType == DoubleType)
    assert(schema.fields(2).name == "the_count")
    assert(schema.fields(2).dataType == LongType)
    assert(schema.fields(3).name == "the_max")
    assert(schema.fields(3).dataType == DoubleType)
    assert(schema.fields(4).name == "the_min")
    assert(schema.fields(4).dataType == DoubleType)
    assert(schema.fields(5).name == "the_sum")
    assert(schema.fields(5).dataType == DoubleType)
    selectExprResults = selectExprDF.collectAsList()
    assert(selectExprResults.size() == 4)

    selectExpr = s"""select(
                    |  facet(
                    |    ${ratingsCollection},
                    |    q="*:*",
                    |    buckets="rating",
                    |    bucketSorts="count(*) desc",
                    |    bucketSizeLimit=100,
                    |    count(*),
                    |    sum(rating),
                    |    min(rating),
                    |    max(rating),
                    |    avg(rating)
                    |  ),
                    |  rating,
                    |  count(*) as the_count,
                    |  sum(rating) as the_sum,
                    |  min(rating) as the_min,
                    |  max(rating) as the_max,
                    |  avg(rating) as the_avg,
                    |  replace(rating,5,withValue=excellent),
                    |  replace(rating,4,withValue=good),
                    |  replace(rating,3,withValue=avg),
                    |  replace(rating,2,withValue=poor),
                    |  replace(rating,1,withValue=awful)
                    |)
    """.stripMargin
    selectExprDF =
      sparkSession.read.format("solr").options(Map("zkhost" -> zkHost, "collection" -> ratingsCollection, "expr" -> selectExpr)).load
    selectExprDF.printSchema()
    schema = selectExprDF.schema
    assert(schema.fields.length == 6)
    assert(schema.fields(0).name == "rating")
    assert(schema.fields(0).dataType == StringType) // important, replace makes rating a string
    assert(schema.fields(1).name == "the_avg")
    assert(schema.fields(1).dataType == DoubleType)
    assert(schema.fields(2).name == "the_count")
    assert(schema.fields(2).dataType == LongType)
    assert(schema.fields(3).name == "the_max")
    assert(schema.fields(3).dataType == DoubleType)
    assert(schema.fields(4).name == "the_min")
    assert(schema.fields(4).dataType == DoubleType)
    assert(schema.fields(5).name == "the_sum")
    assert(schema.fields(5).dataType == DoubleType)
    selectExprResults = selectExprDF.collectAsList()
    assert(selectExprResults.size() == 4)

    // test user-supplied schema
    selectExpr = s"""select(
                    |  facet(
                    |    ${ratingsCollection},
                    |    q="*:*",
                    |    buckets="rating",
                    |    bucketSorts="count(*) desc",
                    |    bucketSizeLimit=100,
                    |    count(*),
                    |    sum(rating),
                    |    min(rating),
                    |    max(rating),
                    |    avg(rating)
                    |  ),
                    |  rating,
                    |  count(*) as the_count,
                    |  avg(rating) as the_avg,
                    |  replace(rating,5,withValue=excellent),
                    |  replace(rating,4,withValue=good),
                    |  replace(rating,3,withValue=avg),
                    |  replace(rating,2,withValue=poor),
                    |  replace(rating,1,withValue=awful)
                    |)
    """.stripMargin
    selectExprDF =
      sparkSession.read.format("solr").options(Map("zkhost" -> zkHost, "collection" -> ratingsCollection, "expr" -> selectExpr,
        "expr_schema" -> "rating:string,the_avg:double,the_count:long")).load
    selectExprDF.printSchema()
    schema = selectExprDF.schema
    assert(schema.fields.length == 3)
    assert(schema.fields(0).name == "rating")
    assert(schema.fields(0).dataType == StringType)
    assert(schema.fields(1).name == "the_avg")
    assert(schema.fields(1).dataType == DoubleType)
    assert(schema.fields(2).name == "the_count")
    assert(schema.fields(2).dataType == LongType)
    selectExprResults = selectExprDF.collectAsList()
    assert(selectExprResults.size() == 4)

    // now, verify that projections from SQL that don't match the streaming expression schema
    selectExprDF.createOrReplaceTempView("ratings_expr")
    val projectFieldsOnExprDf = sparkSession.sql("select the_count, the_avg, rating from ratings_expr")
    projectFieldsOnExprDf.printSchema()
    schema = projectFieldsOnExprDf.schema
    assert(schema.fields.length == 3)
    assert(schema.fields(0).name == "the_count")
    assert(schema.fields(0).dataType == LongType)
    assert(schema.fields(1).name == "the_avg")
    assert(schema.fields(1).dataType == DoubleType)
    assert(schema.fields(2).name == "rating")
    assert(schema.fields(2).dataType == StringType)
    selectExprResults = projectFieldsOnExprDf.collectAsList()
    assert(selectExprResults.size() == 4)

    // clean-up
    SolrCloudUtil.deleteCollection(ratingsCollection, cluster)
    SolrCloudUtil.deleteCollection(moviesCollection, cluster)
  }

  test("Skip Non DocValue fields config option") {
    val moviesCollection = "movies" + UUID.randomUUID().toString.replace('-','_')
    buildMoviesCollectionWithText(moviesCollection)

    {
      val opts = Map("zkhost" -> zkHost, "collection" -> moviesCollection, SKIP_NON_DOCVALUE_FIELDS -> "true")
      val df = sparkSession.read.format("solr").options(opts).load()
      val schema = df.schema
      assert(schema.fields.length == 3)
      assert(schema.fieldNames.toSet  == Set("id", "movie_id", "title"))
    }

    {
      val opts = Map("zkhost" -> zkHost, "collection" -> moviesCollection, SKIP_NON_DOCVALUE_FIELDS -> "false")
      val df = sparkSession.read.format("solr").options(opts).load()
      val schema = df.schema
      assert(schema.fields.length == 4)
      assert(schema.fieldNames.toSet == Set("id", "movie_id",  "title_txt", "title"))
    }

    SolrCloudUtil.deleteCollection(moviesCollection, cluster)
  }

  test("Field Aliases and Function Queries") {
    val ratingsCollection = "ratings" + UUID.randomUUID().toString.replace('-','_')
    val numRatings = buildRatingsCollection(ratingsCollection)
    var ratingsDF = sparkSession.read.format("solr").options(
      Map("zkhost" -> zkHost, "collection" -> ratingsCollection,
        "fields" -> "user:user_id,movie:movie_id,rating,ts:rating_timestamp,rord_movie:rord(movie_id),ord_user:ord(user_id):long,log_rating:log(rating):double,score,tsms:ms(NOW%2Crating_timestamp):double",
        "sort" -> "rord(user_id) asc",
        "max_rows" -> "2")).load
    assert(ratingsDF.count == 2)
    ratingsDF.printSchema()
    val schema = ratingsDF.schema
    assert(schema.fields.length == 9)
    assert(schema.fields(0).name == "user")
    assert(schema.fields(0).dataType == StringType)
    assert(schema.fields(1).name == "movie")
    assert(schema.fields(1).dataType == StringType)
    assert(schema.fields(2).name == "rating")
    assert(schema.fields(2).dataType == LongType)
    assert(schema.fields(3).name == "ts")
    assert(schema.fields(3).dataType == TimestampType)
    assert(schema.fields(4).name == "rord_movie")
    assert(schema.fields(4).dataType == LongType)
    assert(schema.fields(5).name == "ord_user")
    assert(schema.fields(5).dataType == LongType)
    assert(schema.fields(6).name == "log_rating")
    assert(schema.fields(6).dataType == DoubleType)
    assert(schema.fields(7).name == "score")
    assert(schema.fields(7).dataType == DoubleType)
    assert(schema.fields(8).name == "tsms")
    assert(schema.fields(8).dataType == DoubleType)
    ratingsDF.collectAsList()
  }

  def buildMoviesCollection(moviesCollection: String) : Int = {
    SolrCloudUtil.buildCollection(zkHost, moviesCollection, null, 1, cloudClient, sc)

    val movieDocs : Array[String] = Array(
      UUID.randomUUID().toString+",movie200,The Big Short",
      UUID.randomUUID().toString+",movie201,Moneyball"
    )
    val indexMoviesRequest = new UpdateRequest()
    movieDocs.foreach(row => {
      val fields = row.split(",")
      val doc = new SolrInputDocument()
      doc.setField("id", fields(0))
      doc.setField("movie_id", fields(1))
      doc.setField("title", fields(2))
      indexMoviesRequest.add(doc)
      doc
    })
    indexMoviesRequest.process(cloudClient, moviesCollection)
    indexMoviesRequest.commit(cloudClient, moviesCollection)

    return movieDocs.length
  }

  def buildRatingsCollection(ratingsCollection: String) : Int = {
    SolrCloudUtil.buildCollection(zkHost, ratingsCollection, null, 1, cloudClient, sc)

    val ratingDocs : Array[String] = Array(
      UUID.randomUUID().toString+",user1,movie200,3,2016-01-01T00:00:01.999Z",
      UUID.randomUUID().toString+",user1,movie201,4,2016-01-02T00:00:02.999Z",
      UUID.randomUUID().toString+",user2,movie200,5,2016-01-03T00:00:03.999Z",
      UUID.randomUUID().toString+",user2,movie201,2,2016-01-04T00:00:04.999Z"
    )
    val indexRatingsRequest = new UpdateRequest()
    ratingDocs.foreach(row => {
      val fields = row.split(",")
      val doc = new SolrInputDocument()
      doc.setField("id", fields(0))
      doc.setField("user_id", fields(1))
      doc.setField("movie_id", fields(2))
      doc.setField("rating", fields(3).toInt)
      doc.setField("rating_timestamp", fields(4))
      indexRatingsRequest.add(doc)
      doc
    })
    indexRatingsRequest.process(cloudClient, ratingsCollection)
    indexRatingsRequest.commit(cloudClient, ratingsCollection)

    return ratingDocs.length
  }


  def buildMoviesCollectionWithText(moviesCollection: String) : Int = {
    SolrCloudUtil.buildCollection(zkHost, moviesCollection, null, 1, cloudClient, sc)

    val movieDocs : Array[String] = Array(
      UUID.randomUUID().toString+",movie200,The Big Short",
      UUID.randomUUID().toString+",movie201,Moneyball"
    )
    val indexMoviesRequest = new UpdateRequest()
    movieDocs.foreach(row => {
      val fields = row.split(",")
      val doc = new SolrInputDocument()
      doc.setField("id", fields(0))
      doc.setField("movie_id", fields(1))
      doc.setField("title", fields(2))
      doc.setField("title_txt", fields(2))
      indexMoviesRequest.add(doc)
      doc
    })
    indexMoviesRequest.process(cloudClient, moviesCollection)
    indexMoviesRequest.commit(cloudClient, moviesCollection)

    return movieDocs.length
  }
}
