package com.lucidworks.spark.util

import java.sql.Timestamp
import java.util.Date

import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.SolrDocument
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.joda.time.format.ISODateTimeFormat

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object SolrRelationUtil extends Logging {

  def getBaseSchema(
      zkHost: String,
      collection: String,
      escapeFields: Boolean,
      flattenMultivalued: Boolean): StructType =
    getBaseSchema(Set.empty[String], zkHost, collection, escapeFields, flattenMultivalued)

  def getBaseSchema(
      fields: Set[String],
      zkHost: String,
      collection: String,
      escapeFields: Boolean,
      flattenMultivalued: Boolean): StructType = {
    val solrBaseUrl = SolrSupport.getSolrBaseUrl(zkHost)
    val fieldTypeMap = SolrQuerySupport.getFieldTypes(fields, solrBaseUrl, collection)
    val structFields = new ListBuffer[StructField]

    fieldTypeMap.foreach{ case(fieldName, fieldMeta) =>
      val metadata = new MetadataBuilder
      var dataType: DataType = {
        if (fieldMeta.fieldTypeClass.isDefined) {
          if (SolrQuerySupport.SOLR_DATA_TYPES.contains(fieldMeta.fieldTypeClass.get)) {
            SolrQuerySupport.SOLR_DATA_TYPES(fieldMeta.fieldTypeClass.get)
          } else {
            DataTypes.StringType
          }
        }
        else
          DataTypes.StringType
      }

      metadata.putString("name", fieldName)
      metadata.putString("type", fieldMeta.fieldType)

      if (!flattenMultivalued && fieldMeta.isMultiValued.isDefined) {
        if (fieldMeta.isMultiValued.get) {
          dataType = new ArrayType(dataType, true)
          metadata.putBoolean("multiValued", value = true)
        }
      }

      if (fieldMeta.isRequired.isDefined)
        metadata.putBoolean("required", value = fieldMeta.isRequired.get)

      if (fieldMeta.isDocValues.isDefined)
        metadata.putBoolean("docValues", value = fieldMeta.isDocValues.get)

      if (fieldMeta.isStored.isDefined)
        metadata.putBoolean("stored", value = fieldMeta.isStored.get)

      if (fieldMeta.fieldTypeClass.isDefined)
        metadata.putString("class", fieldMeta.fieldTypeClass.get)

      if (fieldMeta.dynamicBase.isDefined)
        metadata.putString("dynamicBase", fieldMeta.dynamicBase.get)

      val name = if (escapeFields) fieldName.replaceAll("\\.", "_") else fieldName

      structFields.add(DataTypes.createStructField(name, dataType, !fieldMeta.isRequired.getOrElse(false), metadata.build()))
   }

    DataTypes.createStructType(structFields.toList)
  }

  def deriveQuerySchema(fields: Array[String], schema: StructType): StructType = {
    val fieldMap = new mutable.HashMap[String, StructField]()
    for (structField <- schema.fields) fieldMap.put(structField.name, structField)

    val listOfFields = new ListBuffer[StructField]
    for (field <- fields) {
      if (fieldMap.contains(field)) {
        if (fieldMap.get(field).isDefined) {
          listOfFields.add(fieldMap.get(field).get)
        } else {
          log.info("No structField definition found for field '" + field + "'")
        }
      } else {
        log.info("Base schema does not contain field '" + field + "'")
      }
    }

    if (listOfFields.isEmpty) schema else DataTypes.createStructType(listOfFields.toList)
  }

  def applyDefaultFields(baseSchema: StructType, solrQuery: SolrQuery, flattenMultivalued: Boolean): Unit = {
    val schemaFields = baseSchema.fields
    val fieldList = new ListBuffer[String]

    for (schemaField <- schemaFields) {
      val meta = schemaField.metadata
      val isMultiValued = if (!flattenMultivalued && meta.contains("multiValued")) meta.getBoolean("multiValued") else false
      val isDocValues = if (meta.contains("docValues")) meta.getBoolean("docValues") else false
      val isStored = if (meta.contains("stored")) meta.getBoolean("stored") else false

      if (isStored || (isDocValues && !isMultiValued)) {
        fieldList.add(schemaField.name)
      }
    }
    solrQuery.setFields(fieldList.toList:_*)
  }

  def applyFilter(filter: Filter, solrQuery: SolrQuery, baseSchema: StructType) = {
   filter match {
     case f: And =>
       solrQuery.addFilterQuery(fq(f.left, baseSchema))
       solrQuery.addFilterQuery(fq(f.right, baseSchema))
     case f: Or =>
       solrQuery.addFilterQuery("(" + fq(f.left, baseSchema) + " OR " + fq(f.right, baseSchema) + ")")
     case f: Not =>
       solrQuery.addFilterQuery("NOT " + fq(f.child, baseSchema))
     case _ => solrQuery.addFilterQuery(fq(filter, baseSchema))
   }
  }

  def getFilterValue(attr: String, value: String, baseSchema: StructType) = {
    val fieldType = baseSchema(attr)
    fieldType.dataType match {
      case TimestampType => convertToISO(value)
      case _ => value
    }
  }

  def convertToISO(ts: String): String = {
    val unixSeconds = Timestamp.valueOf(ts).getTime
    val isoValue = ISODateTimeFormat.dateTime().withZoneUTC().print(unixSeconds)
    String.format("\"%s\"", isoValue)
  }

  def fq(filter: Filter, baseSchema: StructType): String = {
    var negate = ""
    var crit : Option[String] = None
    var attr: Option[String] = None

    filter match {
      case f: EqualTo =>
        attr = Some(f.attribute)
        crit = Some(getFilterValue(f.attribute, String.valueOf(f.value), baseSchema))
      case f: EqualNullSafe =>
        attr = Some(f.attribute)
        crit = Some(getFilterValue(f.attribute, String.valueOf(f.value), baseSchema))
      case f: GreaterThan =>
        attr = Some(f.attribute)
        crit = Some("{" + getFilterValue(f.attribute, String.valueOf(f.value), baseSchema)+ " TO *]")
      case f: GreaterThanOrEqual =>
        attr = Some(f.attribute)
        crit = Some("[" + getFilterValue(f.attribute, String.valueOf(f.value), baseSchema)+ " TO *]")
      case f: LessThan =>
        attr = Some(f.attribute)
        crit = Some("[* TO " + getFilterValue(f.attribute, String.valueOf(f.value), baseSchema)+ "}")
      case f: LessThanOrEqual =>
        attr = Some(f.attribute)
        crit = Some("[* TO " + getFilterValue(f.attribute, String.valueOf(f.value), baseSchema)+ "]")
      case f: In =>
        attr = Some(f.attribute)
        val sb = new StringBuilder()
        sb.append("(")
        val values = f.values
        values.zipWithIndex.foreach{case(value, i) =>
          if (i>0) sb.append(" ")
          sb.append(String.valueOf(value))
        }
        sb.append(")")
        crit = Some(sb.result())
      case f: IsNotNull =>
        attr = Some(f.attribute)
        crit = Some("[* TO *]")
      case f: IsNull =>
        attr = Some(f.attribute)
        crit = Some("[* TO *]")
        negate = "-"
      case f: StringContains =>
        attr = Some(f.attribute)
        crit = Some("*" + f.value + "*")
      case f: StringEndsWith =>
        attr = Some(f.attribute)
        crit = Some(f.value + "*")
      case f: StringStartsWith =>
        attr = Some(f.attribute)
        crit = Some("*" + f.value)
      case _ => throw new IllegalArgumentException("Filters of type '" + filter + " (" + filter.getClass.getName + ")' not supported!")
    }

    if (attr.isEmpty)
      throw new IllegalArgumentException("Could not get filter attribute for '" + filter + " (" + filter.getClass.getName + ")'")
    if (attr.isEmpty)
      throw new IllegalArgumentException("Could not get filter criteria for '" + filter + " (" + filter.getClass.getName + ")'")

    negate + attributeToFieldName(attr.get, baseSchema) + ":" + crit.get
  }

  def attributeToFieldName(attr: String, baseSchema: StructType): String = {
    val fieldMap = new mutable.HashMap[String, StructField]()
    for (schemaField <- baseSchema.fields) fieldMap.put(schemaField.name, schemaField)

    if (fieldMap.contains(attr)) {
      val structField = fieldMap.get(attr.replaceAll("`", ""))
      if (structField.isDefined) {
        val meta = structField.get.metadata
        if (meta.contains("name")) meta.getString("name") else structField.get.name
      } else {
        attr
      }
    } else {
      attr
    }
  }

  def setAliases(fields: Array[String], solrQuery: SolrQuery, schema: StructType) = {
    val fieldMap  = new mutable.HashMap[String, StructField]()
    for (structField <- schema.fields) fieldMap.put(structField.name, structField)

    val fieldList = new ListBuffer[String]
    for (field <- fields) {
      if (fieldMap.contains(field)) {
        if (fieldMap.get(field).isDefined) {
          val structField = fieldMap.get(field).get
          val metadata = structField.metadata
          val fieldName = if (metadata.contains("name"))  metadata.getString("name") else field
          val isMultiValued = if (metadata.contains("multiValued")) metadata.getBoolean("multiValued") else false
          val isDocValues = if (metadata.contains("docValues")) metadata.getBoolean("docValues") else false
          val isStored = if (metadata.contains("stored")) metadata.getBoolean("stored") else false

          if (!isStored && isDocValues && !isMultiValued) {
            fieldList.add(structField.name + ":field(" + fieldName + ")")
          } else {
            fieldList.add(structField.name + ":" + fieldName)
          }
        } else {
          fieldList.add(field)
          log.info("StructField def. not found for field '" + field + "' in the base schema")
        }
      } else {
        fieldList.add(field)
        log.info("Field '" + field + "' not found in the schema")
      }
    }
    solrQuery.setFields(fieldList.toList:_*)
  }

  //TODO: Full on testing with schemaless, multi-valued arrays etc...
  def toRows(schema: StructType, docs: RDD[SolrDocument]): RDD[Row] = {
    val fields = schema.fields

    val rows = docs.map(solrDocument => {
      val values = new ListBuffer[AnyRef]
      for (field <- fields) {
        val metadata = field.metadata
        val isMultiValued = if (metadata.contains("multiValued")) metadata.getBoolean("multiValued") else false
        if (isMultiValued) {
          val fieldValues = solrDocument.getFieldValues(field.name)
          if (fieldValues != null) {
            val iterableValues = fieldValues.iterator().map {
              case d: Date => new Timestamp(d.getTime)
              case i: java.lang.Integer => new java.lang.Long(i.longValue())
              case f: java.lang.Float => new java.lang.Double(f.doubleValue())
              case a => a
            }
            values.add(iterableValues.toArray)
          } else {
            values.add(null)
          }

        } else {
          val fieldValue = solrDocument.getFieldValue(field.name)
          fieldValue match {
            case f: String => values.add(f)
            case f: Date => values.add(new Timestamp(f.getTime))
            case i: java.lang.Integer => values.add(new java.lang.Long(i.longValue()))
            case f: java.lang.Float => values.add(new java.lang.Double(f.doubleValue()))
            case f: java.util.ArrayList[_] =>
              val jlist = f.iterator.map {
                case d: Date => new Timestamp(d.getTime)
                case i: java.lang.Integer => new java.lang.Long(i.longValue())
                case f: java.lang.Float => new java.lang.Double(f.doubleValue())
                case v: Any => v
              }
              val arr = jlist.toArray
              if (arr.length >= 1) {
                values.add(arr(0).asInstanceOf[AnyRef])
              }
            case f: Iterable[_] =>
              val iterableValues = f.iterator.map {
                case d: Date => new Timestamp(d.getTime)
                case i: java.lang.Integer => new java.lang.Long(i.longValue())
                case f: java.lang.Float => new java.lang.Double(f.doubleValue())
                case v: Any => v
              }
              val arr = iterableValues.toArray
              if (!arr.isEmpty) {
                values.add(arr(0).asInstanceOf[AnyRef])
              }
            case f: Any => values.add(f)
            case f => values.add(f)
          }
        }
      }
      Row(values.toArray:_*)
    })
    rows
  }
}
