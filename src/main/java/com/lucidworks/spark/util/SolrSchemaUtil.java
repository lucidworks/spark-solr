package com.lucidworks.spark.util;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static com.lucidworks.spark.util.SolrQuerySupport.*;

public class SolrSchemaUtil implements Serializable{

  public static Logger log = Logger.getLogger(SolrSchemaUtil.class);

   public static StructType readSchema(SolrDocument doc, SolrClient Solr, String collection) throws IOException, SolrServerException {
    List<StructField> fields = new ArrayList<>();
    StructType st= (StructType) recurseReadSchema(doc, Solr, fields, collection).dataType();
    return st;
  }

  public static StructField recurseReadSchema(SolrDocument doc, SolrClient solr, List<StructField> fldr, String collection) throws IOException, SolrServerException {
    Boolean recurse = true;
    String finalName = null;
    for (Map.Entry<String, Object> field : doc.entrySet()) {
      String name = field.getKey();
      Object value = field.getValue();
      if (name.startsWith("links")) {
        String id = doc.get(name).toString();
        if (id != null) {
          SolrQuery q1 = new SolrQuery("id:" + id);
          QueryResponse rsp1 = null;
          try {
            rsp1 = solr.query(collection,q1);
          } catch (Exception E) {
            log.error(E.toString());
            recurse = false;
          }
          if (recurse) {
            SolrDocumentList docs1 = rsp1.getResults();
            List<StructField> fld1 = new ArrayList<StructField>();
            fldr.add(recurseReadSchema(docs1.get(0), solr, fld1, collection));
          }
        }
      }
      if (name.substring(name.length()-2,name.length()).equals("_s")  && !name.equals("__lwroot_s") && !name.startsWith("links") && !name.equals("__lwcategory_s")) {
        if (name.substring(0, name.length() - 2).equals("__lwchilddocname")) {
          finalName = field.getValue().toString();
        } else {
            fldr.add(new StructField(name.substring(0, name.length() - 2), SQLQuerySupport.getsqlDataType(field.getValue().toString()), true, Metadata.empty()));
          }
      }

    }
    StructField[] farr = new StructField[fldr.size()];
    farr = fldr.toArray(farr);
    StructType st2 = new StructType(farr);
    if (finalName == null) {
      finalName = "root";
    }
    return new StructField(finalName, st2, true,  Metadata.empty());
  }

  public static void recurseWriteSchema(StructType st, SolrInputDocument s, int l){
    scala.collection.Iterator x = st.iterator();
    int linkCount = 0;
    while (x.hasNext()) {
      StructField sf = (StructField) x.next();
      if (sf.dataType().typeName().toString().toLowerCase().equals("struct")){
        linkCount = linkCount + 1;
        SolrInputDocument sc = new SolrInputDocument();
        String id = UUID.randomUUID().toString();
        sc.addField("id",id);
        s.addField("links"+linkCount +"_s", id);
        l = l + 1;
        sc.addField("__lwchilddocname_s",sf.name());
        sc.addField("__lwcategory_s","schema");
        recurseWriteSchema((StructType) sf.dataType(), sc, l);
        s.addChildDocument(sc);
      } else {
        if (!sf.dataType().typeName().toLowerCase().equals("array")) {
          s.addField(sf.name() + "_s", sf.dataType().typeName());
        } else {
          s.addField(sf.name() + "_s", ScalaUtil.getArraySchema(sf.dataType()));
        }
      }
    }
  }

  public static Map<String, SolrFieldMeta> getSchemaFields(String solrBaseUrl, String collection) throws SparkException {
    String lukeUrl = solrBaseUrl + collection + "/admin/luke?numTerms=0";
    // collect mapping of Solr field to type
    Map<String,SolrFieldMeta> schemaFieldMap = new HashMap<String,SolrFieldMeta>();
    try {
      try {
        Map<String, Object> adminMeta = SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient(), lukeUrl, 2);
        Map<String, Object> fieldsMap = SolrJsonSupport.asMap("/fields", adminMeta);
        Set<String> fieldNamesSet = fieldsMap.keySet();
        schemaFieldMap = getFieldTypes(fieldNamesSet.toArray(new String[fieldNamesSet.size()]), solrBaseUrl, collection);
      } catch (SolrException solrExc) {
        log.warn("Can't get field types for " + collection + " due to: "+solrExc);
      }
    } catch (Exception exc) {
      log.warn("Can't get schema fields for " + collection + " due to: "+exc);
    }
    return schemaFieldMap;
  }

  public static StructType getBaseSchema(String zkHost, String collection, boolean escapeFields) throws Exception {
    String solrBaseUrl = SolrSupport.getSolrBaseUrl(zkHost);
    Map<String, SolrFieldMeta> fieldTypeMap = getSchemaFields(solrBaseUrl, collection);

    List<StructField> listOfFields = new ArrayList<StructField>();
    for (Map.Entry<String, SolrFieldMeta> field : fieldTypeMap.entrySet()) {
      String fieldName = field.getKey();
      SolrFieldMeta fieldMeta = field.getValue();
      MetadataBuilder metadata = new MetadataBuilder();
      metadata.putString("name", field.getKey());
      DataType dataType = (fieldMeta.fieldTypeClass != null) ? SolrQuerySupport.SOLR_DATA_TYPES.get(fieldMeta.fieldTypeClass) : null;
      if (dataType == null) dataType = DataTypes.StringType;

      if (fieldMeta.isMultiValued) {
        dataType = new ArrayType(dataType, true);
        metadata.putBoolean("multiValued", fieldMeta.isMultiValued);
      }
      if (fieldMeta.isRequired) metadata.putBoolean("required", fieldMeta.isRequired);
      if (fieldMeta.isDocValues) metadata.putBoolean("docValues", fieldMeta.isDocValues);
      if (fieldMeta.isStored) metadata.putBoolean("stored", fieldMeta.isStored);
      if (fieldMeta.fieldType != null) metadata.putString("type", fieldMeta.fieldType);
      if (fieldMeta.dynamicBase != null) metadata.putString("dynamicBase", fieldMeta.dynamicBase);
      if (fieldMeta.fieldTypeClass != null) metadata.putString("class", fieldMeta.fieldTypeClass);
      if (escapeFields) {
        fieldName = fieldName.replaceAll("\\.","_");
      }
      listOfFields.add(DataTypes.createStructField(fieldName, dataType, !fieldMeta.isRequired, metadata.build()));
    }

    return DataTypes.createStructType(listOfFields);
  }

  // derive a schema for a specific query from the full collection schema
  public static StructType deriveQuerySchema(String[] fields, StructType schema) {
    Map<String, StructField> fieldMap = new HashMap<String, StructField>();
    for (StructField f : schema.fields()) fieldMap.put(f.name(), f);
    List<StructField> listOfFields = new ArrayList<StructField>();
    for (String field : fields) listOfFields.add(fieldMap.get(field));
    return DataTypes.createStructType(listOfFields);
  }

  public static StructType getQuerySchema(SolrQuery query, StructType schema) throws Exception {
    String fieldList = query.getFields();
    if (fieldList != null && !fieldList.isEmpty()) {
      return deriveQuerySchema(fieldList.split(","), schema);
    }
    return schema;
  }

  public static void applyDefaultFields(StructType baseSchema, SolrQuery solrQuery) {
    StructField[] schemaFields = baseSchema.fields();
    List<String> fieldList = new ArrayList<String>();
    for (int sf = 0; sf < schemaFields.length; sf++) {
      StructField schemaField = schemaFields[sf];
      Metadata meta = schemaField.metadata();
      Boolean isMultiValued = meta.contains("multiValued") ? meta.getBoolean("multiValued") : false;
      Boolean isDocValues = meta.contains("docValues") ? meta.getBoolean("docValues") : false;
      Boolean isStored = meta.contains("stored") ? meta.getBoolean("stored") : false;
      if (isStored || (isDocValues && !isMultiValued)) {
        fieldList.add(schemaField.name());
      }
    }
    solrQuery.setFields(fieldList.toArray(new String[fieldList.size()]));
  }

  public static void applyFilter(Filter filter, SolrQuery solrQuery, StructType baseSchema) {
    if (filter instanceof And) {
      And and = (And)filter;
      solrQuery.addFilterQuery(fq(and.left(), baseSchema));
      solrQuery.addFilterQuery(fq(and.right(), baseSchema));
    } else if (filter instanceof Or) {
      Or f = (Or)filter;
      solrQuery.addFilterQuery("(" + fq(f.left(), baseSchema)+" OR " + fq(f.right(), baseSchema)+")");
    } else if (filter instanceof Not) {
      Not not = (Not)filter;
      solrQuery.addFilterQuery("NOT "+fq(not.child(), baseSchema));
    } else {
      solrQuery.addFilterQuery(fq(filter, baseSchema));
    }
  }

  public static String fq(Filter f, StructType baseSchema) {
    String negate = "";
    String crit = null;
    String attr = null;
    if (f instanceof EqualTo) {
      EqualTo eq = (EqualTo)f;
      attr = eq.attribute();
      crit = String.valueOf(eq.value());
    } else if (f instanceof EqualNullSafe) {
      EqualNullSafe eq = (EqualNullSafe)f;
      attr = eq.attribute();
      crit = String.valueOf(eq.value());
    } else if (f instanceof GreaterThan) {
      GreaterThan gt = (GreaterThan)f;
      attr = gt.attribute();
      crit = "{"+gt.value()+" TO *]";
    } else if (f instanceof GreaterThanOrEqual) {
      GreaterThanOrEqual gte = (GreaterThanOrEqual)f;
      attr = gte.attribute();
      crit = "["+gte.value()+" TO *]";
    } else if (f instanceof LessThan) {
      LessThan lt = (LessThan)f;
      attr = lt.attribute();
      crit = "[* TO "+lt.value()+"}";
    } else if (f instanceof LessThanOrEqual) {
      LessThanOrEqual lte = (LessThanOrEqual)f;
      attr = lte.attribute();
      crit = "[* TO "+lte.value()+"]";
    } else if (f instanceof In) {
      In inf = (In)f;
      attr = inf.attribute();
      StringBuilder sb = new StringBuilder();
      sb.append("(");
      Object[] vals = inf.values();
      for (int v=0; v < vals.length; v++) {
        if (v > 0) sb.append(" ");
        sb.append(String.valueOf(vals[v]));
      }
      sb.append(")");
      crit = sb.toString();
    } else if (f instanceof IsNotNull) {
      IsNotNull inn = (IsNotNull)f;
      attr = inn.attribute();
      crit = "[* TO *]";
    } else if (f instanceof IsNull) {
      IsNull isn = (IsNull)f;
      attr = isn.attribute();
      crit = "[* TO *]";
      negate = "-";
    } else if (f instanceof StringContains) {
      StringContains sc = (StringContains)f;
      attr = sc.attribute();
      crit = "*"+sc.value()+"*";
    } else if (f instanceof StringEndsWith) {
      StringEndsWith sew = (StringEndsWith)f;
      attr = sew.attribute();
      crit = sew.value()+"*";
    } else if (f instanceof StringStartsWith) {
      StringStartsWith ssw = (StringStartsWith)f;
      attr = ssw.attribute();
      crit = "*"+ssw.value();
    } else {
      throw new IllegalArgumentException("Filters of type '"+f+" ("+f.getClass().getName()+")' not supported!");
    }
    return negate + attributeToFieldName(attr, baseSchema) + ":" + crit;
  }

  public static String attributeToFieldName(String attribute, StructType baseSchema) {
    Map<String,StructField> fieldMap = new HashMap<String,StructField>();
    for (StructField f : baseSchema.fields()) fieldMap.put(f.name(), f);
    StructField field = fieldMap.get(attribute.replaceAll("`", ""));
    if (field != null) {
      Metadata meta = field.metadata();
      String fieldName = meta.contains("name") ? meta.getString("name") : field.name();
      return fieldName;
    }
    return attribute;
  }

  public static JavaRDD<Row> toRows(StructType schema, JavaRDD<SolrDocument> docs) {
    final StructField[] fields = schema.fields();
    JavaRDD<Row> rows = docs.map(new Function<SolrDocument, Row>() {
      public Row call(SolrDocument doc) throws Exception {
        Object[] vals = new Object[fields.length];
        for (int f = 0; f < fields.length; f++) {
          StructField field = fields[f];
          Metadata meta = field.metadata();
          Boolean isMultiValued = meta.contains("multiValued") ? meta.getBoolean("multiValued") : false;
          Object fieldValue = isMultiValued ? doc.getFieldValues(field.name()) : doc.getFieldValue(field.name());
          if (fieldValue != null) {
            if (fieldValue instanceof Collection) {
              vals[f] = ((Collection) fieldValue).toArray();
            } else if (fieldValue instanceof Date) {
              vals[f] = new java.sql.Timestamp(((Date) fieldValue).getTime());
            } else {
              vals[f] = fieldValue;
            }
          }
        }
        return RowFactory.create(vals);
      }
    });
    return rows;
  }

  public static void setAliases(String[] fields, SolrQuery solrQuery, StructType schema) {
    Map<String,StructField> fieldMap = new HashMap<String,StructField>();
    for (StructField f : schema.fields()) {
      fieldMap.put(f.name(), f);
    }
    String[] fieldList = new String[fields.length];
    for (int f = 0; f < fields.length; f++) {
      StructField field = fieldMap.get(fields[f]);
      if (field != null) {
        Metadata meta = field.metadata();
        String fieldName = meta.contains("name") ? meta.getString("name") : field.name();
        Boolean isMultiValued = meta.contains("multiValued") ? meta.getBoolean("multiValued") : false;
        Boolean isDocValues = meta.contains("docValues") ? meta.getBoolean("docValues") : false;
        Boolean isStored = meta.contains("stored") ? meta.getBoolean("stored") : false;
        if (!isStored && isDocValues && !isMultiValued) {
          fieldList[f] = field.name() + ":field("+fieldName+")";
        } else {
          fieldList[f] = field.name() + ":" + fieldName;
        }
      } else {
        fieldList[f] = fields[f];
      }
    }
    solrQuery.setFields(fieldList);
  }
}
