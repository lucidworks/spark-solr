package com.lucidworks.spark.util

import java.net.URLDecoder
import java.sql.Timestamp
import java.util
import java.util.Date

import com.lucidworks.spark.LazyLogging
import com.lucidworks.spark.rdd.{SelectSolrRDD, StreamingSolrRDD}
import org.apache.solr.client.solrj.request.GenericSolrRequest
import org.apache.solr.client.solrj.request.RequestWriter.StringPayloadContentWriter
import org.apache.solr.client.solrj.{SolrQuery, SolrRequest}
import org.apache.solr.common.SolrDocument
import org.apache.solr.common.params.CommonParams
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

  def isValidDynamicFieldName(fieldName: String, dynamicExtensionSuffixes: Set[String]): Boolean = {
    dynamicExtensionSuffixes.foreach(ext => {
      if (fieldName.startsWith(ext) || fieldName.endsWith(ext)) return true
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
        } else if (field.equals("score")) { //Support the "score" pseudo-field as a double
          funcReturnType = Some(DoubleType)
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
      numShardsToSample: Option[Int],
      escapeFields: Boolean,
      flattenMultivalued: Option[Boolean],
      skipNonDocValueFields: Boolean,
      dynamicExtensions: Set[String]): StructType =
    getBaseSchema(Set.empty[String], zkHost, collection, numShardsToSample, escapeFields, flattenMultivalued, skipNonDocValueFields, dynamicExtensions)

  def getBaseSchema(
      fields: Set[String],
      zkHost: String,
      collection: String,
      numShardsToSample: Option[Int],
      escapeFields: Boolean,
      flattenMultivalued: Option[Boolean],
      skipNonDocValueFields: Boolean,
      dynamicExtensions: Set[String]): StructType = {
    // If the collection is empty (no documents), return an empty schema
    if (SolrQuerySupport.getNumDocsFromSolr(collection, zkHost, None) == 0)
      return new StructType()

    val cloudClient = SolrSupport.getCachedCloudClient(zkHost)
    val solrBaseUrl = SolrSupport.getSolrBaseUrl(zkHost)
    val solrUrl = solrBaseUrl + collection + "/"
    val fieldTypeMap =
      if (fields.isEmpty) {
        val fieldsFromLuke = SolrQuerySupport.getFieldsFromLuke(zkHost, collection, numShardsToSample)
        logger.debug("Fields from luke handler: {}", fieldsFromLuke.mkString(","))
        if (fieldsFromLuke.isEmpty)
          return new StructType()
        // Retain the keys that are present in the Luke handler
        SolrQuerySupport.getFieldTypes(fieldsFromLuke, solrUrl, cloudClient, collection).filterKeys(fieldsFromLuke.contains)
      } else {
        SolrQuerySupport.getFieldTypes(fields, solrUrl, cloudClient, collection)
      }

    logger.debug("Fields from schema handler: {}", fieldTypeMap.keySet.mkString(","))
    val structFields = new ListBuffer[StructField]

    // Retain the keys that are present in the Luke handler
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

      val keepFieldMultivalued = if (flattenMultivalued.isEmpty) {
        SolrRelationUtil.isValidDynamicFieldName(fieldName, dynamicExtensions)
      } else {
        !flattenMultivalued.get
      }
      if (keepFieldMultivalued && fieldMeta.isMultiValued.isDefined) {
        if (fieldMeta.isMultiValued.get) {
          dataType = new ArrayType(dataType, true)
          metadata.putBoolean("multiValued", value = true)
        }
      }

      if (!keepFieldMultivalued &&
           fieldMeta.isMultiValued.isDefined &&
           fieldMeta.isMultiValued.get &&
           (dataType.isInstanceOf[StringType] ||
             dataType.isInstanceOf[LongType] ||
             dataType.isInstanceOf[IntegerType] ||
             dataType.isInstanceOf[DoubleType] ||
             dataType.isInstanceOf[FloatType])) {
        /*
        * Reset the dataType to a String if
        * we are flattening a multi-value
        * String, Long, Integer, Double or Float field.
        */

        dataType = DataTypes.StringType
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
    for (field <- fields.distinct) {
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

  def applyFilter(filter: Filter, solrQuery: SolrQuery, baseSchema: StructType): Unit = {
    filter match {
      case f: And =>
        val values = getAllFilterValues(f, baseSchema, ListBuffer.empty[String])
        values.foreach(v => solrQuery.addFilterQuery(v))
      case f: Or =>
        val values = getAllFilterValues(f, baseSchema, ListBuffer.empty[String])
        val fqStringBuilder = new StringBuilder
        for (i <- values.indices) {
          if (i == 0) fqStringBuilder.append("(")
          fqStringBuilder.append(values(i))
          if (i != values.size-1) {
            fqStringBuilder.append(" OR ")
          } else {
            fqStringBuilder.append(")")
          }
        }
        if (fqStringBuilder.nonEmpty) solrQuery.addFilterQuery(fqStringBuilder.toString())
      case f: Not =>
        f.child match {
          case c : IsNull => solrQuery.addFilterQuery(fq(IsNotNull(c.attribute), baseSchema))
          case _ => solrQuery.addFilterQuery("NOT " + fq(f.child, baseSchema))
        }
      case _ => solrQuery.addFilterQuery(fq(filter, baseSchema))
   }
  }

  def getAllFilterValues(filter: Filter, baseSchema: StructType, values: ListBuffer[String]): List[String] = {
    filter match {
      case f: And =>
        f.left match {
          case l: And => getAllFilterValues(l, baseSchema, values)
          case l: Or =>
            val nestedFqs = getAllFilterValues(l, baseSchema, ListBuffer.empty[String])
            val singleFq = s"(${nestedFqs.mkString(" OR ")})"
            values.+=(singleFq)
          case l: Not =>
            l.child match {
              case c : IsNull => values.+=(fq(IsNotNull(c.attribute), baseSchema))
              case _ => values.+=("NOT " + fq(l.child, baseSchema))
            }
          case _ => values.+=(fq(f.left, baseSchema))
        }
        f.right match {
          case r: And => getAllFilterValues(r, baseSchema, values)
          case r: Or =>
            val nestedFqs = getAllFilterValues(r, baseSchema, ListBuffer.empty[String])
            val singleFq = s"(${nestedFqs.mkString(" OR ")})"
            values.+=(singleFq)
          case r: Not =>
            r.child match {
              case c : IsNull => values.+=(fq(IsNotNull(c.attribute), baseSchema))
              case _ => values.+=("NOT " + fq(r.child, baseSchema))
            }
          case _ => values.+=(fq(f.right, baseSchema))
        }
      case f: Or =>
        f.left match {
          case l: Or => getAllFilterValues(l, baseSchema, values)
          case l: And =>
            val nestedFqs = getAllFilterValues(l, baseSchema, ListBuffer.empty[String])
            val singleFq = s"(${nestedFqs.mkString(" AND ")})"
            values.+=(singleFq)
          case l: Not =>
            l.child match {
              case c : IsNull => values.+=(fq(IsNotNull(c.attribute), baseSchema))
              case _ => values.+=("NOT " + fq(l.child, baseSchema))
            }
          case _ => values.+=(fq(f.left, baseSchema))
        }
        f.right match {
          case r: Or => getAllFilterValues(r, baseSchema, values)
          case r: And =>
            val nestedFqs = getAllFilterValues(r, baseSchema, ListBuffer.empty[String])
            val singleFq = s"(${nestedFqs.mkString(" AND ")})"
            values.+=(singleFq)
          case r: Not =>
            r.child match {
              case c : IsNull => values.+=(fq(IsNotNull(c.attribute), baseSchema))
              case _ => values.+=("NOT " + fq(r.child, baseSchema))
            }
          case _ => values.+=(fq(f.right, baseSchema))
        }
    }
    values.toList
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
        val equalToValue: String = getFilterValue(f.attribute, String.valueOf(f.value), baseSchema)
        if (equalToValue.startsWith("\"") && equalToValue.endsWith("\"")) {
          crit = Some(equalToValue)
        } else {
          // Surround the value with double quotes to escape special characters in Strings
          crit = Some(s""""$equalToValue"""")
        }
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
        crit = Some("(*" + f.value + "*)")
      case f: StringEndsWith =>
        attr = Some(f.attribute)
        crit = Some("(*" + f.value + ")")
      case f: StringStartsWith =>
        attr = Some(f.attribute)
        crit = Some("(" + f.value + "*)")
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

  def processFieldValue(fieldValue: Any, fieldType: DataType, multiValued: Boolean): Any = {
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
          case lt: LongType => new java.lang.Long(f.longValue())
          case dt: DoubleType => new java.lang.Double(f.doubleValue())
          case _ => throw new MatchError(s"Can't convert Float value ${f} to ${fieldType}")
        }
      }
      case d: java.lang.Double => {
        fieldType match {
          case dt: DoubleType => d
          case lt: LongType => new java.lang.Long(d.longValue())
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
      case i: Int => {
        fieldType match {
          case _: IntegerType => i
          case _: LongType => new java.lang.Long(i.longValue())
          case _: StringType => i.toString
          case _: FloatType => new java.lang.Float(i.floatValue())
          case _: DoubleType => new java.lang.Double(i.doubleValue())
          case _: ShortType => new java.lang.Short(i.shortValue())
          case _ => throw new MatchError(s"Can't convert Integer value ${i} to ${fieldType}")
        }
      }
      case l: Long => {
        fieldType match {
          case _: LongType => l
          case _: StringType => l.toString
          case _: DoubleType => new java.lang.Double(l.doubleValue())
          case _ => throw new MatchError(s"Can't convert Long value ${l} to ${fieldType}")
        }
      }
      case f: Float => {
        fieldType match {
          case _: FloatType => f
          case _: StringType => f.toString
          case _: LongType => new java.lang.Long(f.longValue())
          case _: DoubleType => new java.lang.Double(f.doubleValue())
          case _ => throw new MatchError(s"Can't convert Float value ${f} to ${fieldType}")
        }
      }
      case d: Double => {
        fieldType match {
          case _: DoubleType => d
          case _: LongType => new java.lang.Long(d.longValue())
          case _: StringType => d.toString
          case _ => throw new MatchError(s"Can't convert Double value ${d} to ${fieldType}")
        }
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

              val obj = solrDocument.get(field.name)
              val newValue =  processSingleValue(obj, fieldType)
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
              val newValue =  processSingleValue(obj, fieldType)
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

  def processSingleValue(obj: Any, fieldType: DataType): Any = {
    obj match {
      case l: java.util.List[Object] => {
        /*
        * Field is single valued in the schema but has a List of values.
        * Most likely field flattening is on.
        */
        fieldType match {
          case StringType => {
            /*
            * Field is a String so let's serialize the list to a String.
            * Numerics (int, long, float, double) will also report to be String when flattened.
            */
            getFieldValueForList (l)
          }
          case any => {
            /*
            * Not String or numeric reporting to be a String. So let's process the field
            * the default way.
            */
            processFieldValue(obj, fieldType, multiValued = false)
          }
        }
      }
      case any => {
        processFieldValue(obj, fieldType, multiValued = false)
      }
    }
  }


  def setAutoSoftCommit(zkHost: String, collection: String, softAutoCommitMs: Int): Unit = {
    val configJson = "{\"set-property\":{\"updateHandler.autoSoftCommit.maxTime\":\""+softAutoCommitMs+"\"}}";

    logger.info("POSTing: " + configJson + " to collection " + collection)
    val solrRequest = new GenericSolrRequest(SolrRequest.METHOD.POST, "/config", null)
    solrRequest.setContentWriter(new StringPayloadContentWriter(configJson, CommonParams.JSON_MIME))

    try {
      solrRequest.process(SolrSupport.getCachedCloudClient(zkHost), collection)
    } catch {
      case e: Exception => logger.error("Error setting softAutoCommit.maxTime. Exception: {}", e.getMessage)
    }
  }

  def getFieldValueForList(values: java.util.List[Object]): String = {

    if(values != null && values.size() > 0) {
      if(values.get(0).isInstanceOf[Number]) {
        values.mkString(", ")
      } else {
        values.mkString("\"", "\", \"", "\"")
      }

    } else {
      "[]"
    }
  }

  // Deal with commas inside quotes like filters=a:"b, c",d:"e",c:"e, g,h"
  def parseCommaSeparatedValuesToList(filters: String): List[String] = {
    val filterList = ListBuffer.empty[String]
    var start = 0
    var inQuotes = false
    for ((c, i) <- filters.zipWithIndex) {
      if (c == '\"') inQuotes = !inQuotes
      else if (c == ',' && !inQuotes) {
        filterList.+=(filters.substring(start, i).trim)
        start = i + 1
      }
      if (i == filters.length-1) filterList.+=(filters.substring(start).trim)
    }
    filterList.toList
  }
}
