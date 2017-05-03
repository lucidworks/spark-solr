package com.lucidworks.spark.util

import java.lang.Float
import java.net.URLDecoder
import java.sql.Timestamp
import java.util
import java.util.Date

import com.lucidworks.spark.rdd.{SelectSolrRDD, StreamingSolrRDD}
import com.typesafe.scalalogging.LazyLogging
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.SolrDocument
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class QueryField(name:String, alias: Option[String] = None, funcReturnType: Option[DataType] = None) {
  def fl() : String = {
    if (alias.isDefined) s"${alias.get}:${name}" else name
  }
}

object SolrRelationUtil extends LazyLogging {

  val dynamicExtensionSuffixes = mutable.Seq("_i", "_s", "_l", "_b", "_f",
    "_d", "_tdt", "_tdts", "_ss", "_ii", "_txt", "_txt_en", "_ls").seq

  def isValidDynamicFieldName(fieldName: String): Boolean = {
    dynamicExtensionSuffixes.foreach(ext => {
      if (fieldName.endsWith(ext)) return true
    })
    false
  }

  def parseQueryFields(solrFields: Array[String]) : Array[QueryField] = {
    solrFields.map(f => {
      val colonAt = f.indexOf(':') // there can be multiple colons, so just split on the first for now
      if (colonAt != -1) {
        val alias = f.substring(0,colonAt)
        var field = f.substring(colonAt+1)
        var funcReturnType : Option[DataType] = None
        if (field.indexOf('(') != -1 && field.indexOf(')') != -1) {
          // this is a Solr function query
          val lix = field.lastIndexOf(':')
          if (lix != -1) {
            funcReturnType = Some(DataType.fromJson("\""+field.substring(lix+1)+"\""))
            field = field.substring(0,lix) // strip off the additional type info from the function query
          } else {
            funcReturnType = Some(LongType)
          }
          logger.debug(s"Found a Solr function query: ${field} with return type: ${funcReturnType}")
        }
        QueryField(URLDecoder.decode(field, "UTF-8"), Some(alias), funcReturnType)
      } else {
        QueryField(f)
      }
    })
  }

  def getBaseSchema(
      zkHost: String,
      collection: String,
      escapeFields: Boolean,
      flattenMultivalued: Boolean,
      skipNonDocValueFields: Boolean): StructType =
    getBaseSchema(Set.empty[String], zkHost, collection, escapeFields, flattenMultivalued, skipNonDocValueFields)

  def getBaseSchema(
      fields: Set[String],
      zkHost: String,
      collection: String,
      escapeFields: Boolean,
      flattenMultivalued: Boolean,
      skipNonDocValueFields: Boolean): StructType = {
    // If the collection is empty (no documents), return an empty schema
    if (SolrQuerySupport.getNumDocsFromSolr(collection, zkHost, None) == 0)
      return new StructType()

    val solrBaseUrl = SolrSupport.getSolrBaseUrl(zkHost)
    val solrUrl = solrBaseUrl + collection + "/"
    val fieldsFromLuke = SolrQuerySupport.getFieldsFromLuke(solrUrl)
    logger.debug("Fields from luke handler: {}", fieldsFromLuke.mkString(","))
    if (fieldsFromLuke.isEmpty)
      return new StructType()

    val fieldTypeMap =
      if (fields.isEmpty)
        SolrQuerySupport.getFieldTypes(fieldsFromLuke, solrUrl)
      else
        SolrQuerySupport.getFieldTypes(fields, solrUrl)
    logger.debug("Fields from schema handler: {}", fieldTypeMap.keySet.mkString(","))
    val structFields = new ListBuffer[StructField]

    // Retain the keys that are present in the Luke handler
    fieldTypeMap.filterKeys(f => fieldsFromLuke.contains(f)).foreach{ case(fieldName, fieldMeta) =>
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

      val structField = DataTypes.createStructField(name, dataType, !fieldMeta.isRequired.getOrElse(false), metadata.build())
      if (skipNonDocValueFields) {
        if (structField.metadata.contains("docValues") && structField.metadata.getBoolean("docValues")) {
          structFields.add(structField)
        }
      } else {
        structFields.add(structField)
      }

   }

    DataTypes.createStructType(structFields.toList)
  }

  def deriveQuerySchema(fields: Array[QueryField], schema: StructType): StructType = {
    val fieldMap = new mutable.HashMap[String, StructField]()
    for (structField <- schema.fields) fieldMap.put(structField.name, structField)

    val listOfFields = new ListBuffer[StructField]
    for (field <- fields) {
      if (field.funcReturnType.isDefined) {
        listOfFields.add(DataTypes.createStructField(field.alias.get, field.funcReturnType.get, false, Metadata.empty))
      } else {
        val fieldName = field.name
        if (fieldMap.contains(fieldName)) {
          if (fieldMap.get(fieldName).isDefined) {
            val structField = fieldMap.get(fieldName).get
            if (field.alias.isDefined) {
              // have to use the alias here!!
              listOfFields.add(DataTypes.createStructField(field.alias.get, structField.dataType, structField.nullable, structField.metadata))
            } else {
              listOfFields.add(structField)
            }
          } else {
            logger.info("No structField definition found for field '" + fieldName + "'")
          }
        } else {
          if (fieldName == "score") {
            listOfFields.add(DataTypes.createStructField("score", DataTypes.DoubleType, false, Metadata.empty))
          } else {
            logger.info("Base schema does not contain field '" + field + "'")
          }
        }
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
          logger.info("StructField def. not found for field '" + field + "' in the base schema")
        }
      } else {
        fieldList.add(field)
        logger.info("Field '" + field + "' not found in the schema")
      }
    }
    solrQuery.setFields(fieldList.toList:_*)
  }

  def toRows(schema: StructType, docs:RDD[_]): RDD[Row] = {
    docs match {
      case streamingRDD: StreamingSolrRDD => solrDocToRows[util.Map[_,_]](schema, streamingRDD)
      case selectRDD: SelectSolrRDD => solrDocToRows[SolrDocument](schema, selectRDD)
      case _ => throw new Exception("Unknown SolrRDD type")
    }
  }

  def processFieldValue(fieldValue: Object, fieldType: DataType, multiValued: Boolean): Any = {
    fieldValue match {
      case d: Date => new Timestamp(d.getTime)
      case s: String =>
        // This is a workaround. When date fields are streamed through export handler, they are represented with String class type
        if (fieldType.eq(TimestampType)) new Timestamp(DateTime.parse(s).getMillis)
        else s
      case i: java.lang.Integer => {
        fieldType match {
          case it: IntegerType => i
          case lt: LongType => new java.lang.Long(i.longValue())
          case st: StringType => i.toString
          case ft: FloatType => new java.lang.Float(i.floatValue())
          case dt: DoubleType => new java.lang.Double(i.doubleValue())
          case ht: ShortType => new java.lang.Short(i.shortValue())
          case _ => throw new MatchError(s"Can't convert Integer value ${i} to ${fieldType}")
        }
      }
      case l: java.lang.Long => {
        fieldType match {
          case lt: LongType => l
          case st: StringType => l.toString
          case dt: DoubleType => new java.lang.Double(l.doubleValue())
          case _ => throw new MatchError(s"Can't convert Long value ${l} to ${fieldType}")
        }
      }
      case f: java.lang.Float => {
        fieldType match {
          case ft: FloatType => f
          case st: StringType => f.toString
          case dt: DoubleType => new java.lang.Double(f.doubleValue())
          case _ => throw new MatchError(s"Can't convert Float value ${f} to ${fieldType}")
        }
      }
      case d: java.lang.Double => {
        fieldType match {
          case dt: DoubleType => d
          case st: StringType => d.toString
          case _ => throw new MatchError(s"Can't convert Double value ${d} to ${fieldType}")
        }
      }
      case n: java.lang.Number => {
        fieldType match {
          case it: IntegerType => new Integer(n.intValue())
          case lt: LongType => new java.lang.Long(n.longValue())
          case st: StringType => n.toString
          case ft: FloatType => new java.lang.Float(n.floatValue())
          case dt: DoubleType => new java.lang.Double(n.doubleValue())
          case ht: ShortType => new java.lang.Short(n.shortValue())
          case _ => throw new MatchError(s"Can't convert number value ${n} (${n.getClass.getName}) to ${fieldType}")
        }
      }
      case al: java.util.ArrayList[_] =>
        val jlist = al.iterator.map {
          case d: Date => new Timestamp(d.getTime)
          case s: String => {
            fieldType match {
              case at: ArrayType => if (at.elementType.eq(TimestampType)) new Timestamp(DateTime.parse(s).getMillis) else s
              case _: TimestampType => new Timestamp(DateTime.parse(s).getMillis)
              case _ => s
            }
          }
          case i: java.lang.Integer => new java.lang.Long(i.longValue())
          case f: java.lang.Float => new java.lang.Double(f.doubleValue())
          case v => v
        }
        val jlistArray = jlist.toArray
        if (multiValued)
          jlistArray
        else {
          if (jlistArray.nonEmpty) jlistArray(0) else null
        }
      case it : Iterable[_] =>
        val iterableValues = it.iterator.map {
          case d: Date => new Timestamp(d.getTime)
          case s: String =>
            fieldType match {
              case at: ArrayType => if (at.elementType.eq(TimestampType)) new Timestamp(DateTime.parse(s).getMillis) else s
              case _: TimestampType => new Timestamp(DateTime.parse(s).getMillis)
              case _ => s
            }
          case i: java.lang.Integer => new java.lang.Long(i.longValue())
          case f: java.lang.Float => new java.lang.Double(f.doubleValue())
          case v => v
        }
        val iterArray = iterableValues.toArray
        if (multiValued)
          iterArray
        else {
          if (iterArray.nonEmpty) iterArray(0) else null
        }
      case a => a
    }
  }

  def processMultipleFieldValues(fieldValues: util.Collection[Object], fieldType: DataType): Array[AnyRef] = {
    if (fieldValues != null) {
      val iterableValues = fieldValues.iterator().map {
        case d: Date => new Timestamp(d.getTime)
        case s: String => {
          fieldType match {
            // This is a workaround. When date fields are streamed through export handler, they are represented with String class type
            case t: ArrayType =>
              if (t.asInstanceOf[ArrayType].elementType.eq(TimestampType))
                new Timestamp(DateTime.parse(s).getMillis)
              else
                s
            case _ => s
          }
        }
        case i: java.lang.Integer => new java.lang.Long(i.longValue())
        case f: java.lang.Float => new java.lang.Double(f.doubleValue())
        case a => a
      }
      iterableValues.toArray
    } else {
       null
    }
  }

  def solrDocToRows[T](schema: StructType, docs: RDD[T]): RDD[Row] = {
    val fields = schema.fields

    val rows = docs.map(doc => {
      val values = new ListBuffer[Any]
      for (field <- fields) {
        val metadata = field.metadata
        val fieldType = schema.get(schema.indexOf(field)).dataType
        val isMultiValued = if (metadata.contains("multiValued")) metadata.getBoolean("multiValued") else false
        if (isMultiValued) {
           doc match {
            case solrDocument: SolrDocument =>
              val fieldValues = solrDocument.getFieldValues(field.name)
              values.add(processMultipleFieldValues(fieldValues, fieldType))
            case map: util.Map[_,_] =>
              val obj = map.get(field.name).asInstanceOf[Object]
              val newValue = processFieldValue(obj, fieldType, multiValued = true)
              newValue match {
                case arr: Array[_] => values.add(arr)
                case any => values.add(Array(any))
              }
          }
        } else {
          doc match {
            case solrDocument: SolrDocument =>
              val fieldValue = solrDocument.getFieldValue(field.name)
              val newValue = processFieldValue(fieldValue, fieldType, multiValued = false)
              if (metadata.contains(Constants.PROMOTE_TO_DOUBLE) && metadata.getBoolean(Constants.PROMOTE_TO_DOUBLE)) {
                newValue match {
                  case n: java.lang.Number => values.add(n.doubleValue())
                  case a => values.add(a)
                }
              } else {
                values.add(newValue)
              }
            case map: util.Map[_,_] =>
              val obj = map.get(field.name).asInstanceOf[Object]
              val newValue = processFieldValue(obj, fieldType, multiValued = false)
              if (metadata.contains(Constants.PROMOTE_TO_DOUBLE) && metadata.getBoolean(Constants.PROMOTE_TO_DOUBLE)) {
                newValue match {
                  case n: java.lang.Number => values.add(n.doubleValue())
                  case a => values.add(a)
                }
              } else {
                values.add(newValue)
              }
          }
        }
      }
      Row(values.toArray:_*)
    })
    rows
  }
}
