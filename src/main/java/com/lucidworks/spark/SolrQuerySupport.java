package com.lucidworks.spark;

import com.lucidworks.spark.query.*;
import com.lucidworks.spark.util.SolrJsonSupport;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class SolrQuerySupport implements Serializable {

  public static Logger log = Logger.getLogger(SolrQuerySupport.class);
  public static final String DEFAULT_UNIQUE_ID = "id";
  public static final int DEFAULT_PAGE_SIZE = 1000;

  public static final Map<String,DataType> SOLR_DATA_TYPES = new HashMap<String, DataType>();
  static {
    SOLR_DATA_TYPES.put("solr.StrField", DataTypes.StringType);
    SOLR_DATA_TYPES.put("solr.TextField", DataTypes.StringType);
    SOLR_DATA_TYPES.put("solr.BoolField", DataTypes.BooleanType);
    SOLR_DATA_TYPES.put("solr.TrieIntField", DataTypes.IntegerType);
    SOLR_DATA_TYPES.put("solr.TrieLongField", DataTypes.LongType);
    SOLR_DATA_TYPES.put("solr.TrieFloatField", DataTypes.FloatType);
    SOLR_DATA_TYPES.put("solr.TrieDoubleField", DataTypes.DoubleType);
    SOLR_DATA_TYPES.put("solr.TrieDateField", DataTypes.TimestampType);
  }

  public static String getUniqueKey(String zkHost, String collection) {
    try {
      String solrBaseUrl = SolrSupport.getSolrBaseUrl(zkHost);
      // Hit Solr Schema API to get base information
      String schemaUrl = solrBaseUrl + collection + "/schema";
      try {
        Map<String, Object> schemaMeta = SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient(), schemaUrl, 2);
        return SolrJsonSupport.asString("/schema/uniqueKey", schemaMeta);
      } catch (SolrException solrExc) {
        log.warn("Can't get uniqueKey for " + collection +" due to solr: "+solrExc);
      }
    } catch (Exception exc) {
      log.warn("Can't get uniqueKey for " + collection + " due to: " + exc);
    }
    return DEFAULT_UNIQUE_ID;
  }

  public static SolrQuery toQuery(String queryString, String uniqueKey) {

    if (queryString == null || queryString.length() == 0)
      queryString = "*:*";

    SolrQuery q = new SolrQuery();
    if (queryString.indexOf("=") == -1) {
      // no name-value pairs ... just assume this single clause is the q part
      q.setQuery(queryString);
    } else {
      NamedList<Object> params = new NamedList<Object>();
      for (NameValuePair nvp : URLEncodedUtils.parse(queryString, StandardCharsets.UTF_8)) {
        String value = nvp.getValue();
        if (value != null && value.length() > 0) {
          String name = nvp.getName();
          if ("sort".equals(name)) {
            if (value.indexOf(" ") == -1) {
              q.addSort(SolrQuery.SortClause.asc(value));
            } else {
              String[] split = value.split(" ");
              q.addSort(SolrQuery.SortClause.create(split[0], split[1]));
            }
          } else {
            params.add(name, value);
          }
        }
      }
      q.add(ModifiableSolrParams.toSolrParams(params));
    }

    Integer rows = q.getRows();
    if (rows == null)
      q.setRows(DEFAULT_PAGE_SIZE);

    String sorts = q.getSortField();
    if (sorts == null || sorts.isEmpty())
      q.addSort(SolrQuery.SortClause.asc(uniqueKey));

    return q;
  }

  protected static List<String> collectCursors(final SolrClient solrClient, final SolrQuery origQuery, String uniqueKey) throws SolrServerException {
    return collectCursors(solrClient, origQuery, false, uniqueKey);
  }

  protected static List<String> collectCursors(final SolrClient solrClient, final SolrQuery origQuery, final boolean distrib, String uniqueKey) throws SolrServerException {
    List<String> cursors = new ArrayList<String>();

    final SolrQuery query = origQuery.getCopy();
    // tricky - if distrib == false, then set the param, otherwise, leave it out (default is distrib=true)
    if (!distrib) {
      query.set("distrib", false);
    } else {
      query.remove("distrib");
    }
    query.setFields(uniqueKey);

    String nextCursorMark = "*";
    while (true) {
      cursors.add(nextCursorMark);
      query.set("cursorMark", nextCursorMark);

      QueryResponse resp = null;
      try {
        resp = solrClient.query(query);
      } catch (Exception exc) {
        if (exc instanceof SolrServerException) {
          throw (SolrServerException)exc;
        } else {
          throw new SolrServerException(exc);
        }
      }

      nextCursorMark = resp.getNextCursorMark();
      if (nextCursorMark == null || resp.getResults().isEmpty())
        break;
    }

    return cursors;
  }

  /**
   * Iterates over the entire results set of a query (all hits).
   */
  public static class QueryResultsIterator extends PagedResultsIterator<SolrDocument> {

    public QueryResultsIterator(SolrClient solrServer, SolrQuery solrQuery, String cursorMark, String uniqueKey, StructType schema) {
      super(solrServer, solrQuery, cursorMark, uniqueKey, schema);
    }

    protected List<SolrDocument> processQueryResponse(QueryResponse resp) {
      return resp.getResults();
    }
  }

  /**
   * Returns an iterator over TermVectors
   */
  public static class TermVectorIterator extends PagedResultsIterator<Vector> {

    private String field = null;
    private HashingTF hashingTF = null;

    public TermVectorIterator(SolrClient solrServer, SolrQuery solrQuery, String cursorMark, String field, int numFeatures, String uniqueKey, StructType schema) {
      super(solrServer, solrQuery, cursorMark, uniqueKey, schema);
      this.field = field;
      hashingTF = new HashingTF(numFeatures);
    }

    protected List<Vector> processQueryResponse(QueryResponse resp) {
      NamedList<Object> response = resp.getResponse();

      NamedList<Object> termVectorsNL = (NamedList<Object>)response.get("termVectors");
      if (termVectorsNL == null)
        throw new RuntimeException("No termVectors in response! " +
          "Please check your query to make sure it is requesting term vector information from Solr correctly.");

      List<Vector> termVectors = new ArrayList<Vector>(termVectorsNL.size());
      Iterator<Map.Entry<String, Object>> iter = termVectorsNL.iterator();
      while (iter.hasNext()) {
        Map.Entry<String, Object> next = iter.next();
        String nextKey = next.getKey();
        Object nextValue = next.getValue();
        if (nextValue instanceof NamedList) {
          NamedList nextList = (NamedList) nextValue;
          Object fieldTerms = nextList.get(field);
          if (fieldTerms != null && fieldTerms instanceof NamedList) {
            termVectors.add(SolrTermVector.newInstance(nextKey, hashingTF, (NamedList<Object>) fieldTerms));
          }
        }
      }

      SolrDocumentList docs = resp.getResults();
      totalDocs = docs.getNumFound();

      return termVectors;
    }
  }

  protected static void applyFields(String[] fields, SolrQuery solrQuery, StructType schema) {
    Map<String,StructField> fieldMap = new HashMap<String,StructField>();
    for (StructField f : schema.fields()) {
      fieldMap.put(f.name(), f);
    }
    String[] fieldList = new String[fields.length];
    for (int f = 0; f < fields.length; f++) {
      StructField field = fieldMap.get(fields[f].replaceAll("`",""));
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


  public static class SolrFieldMeta {
    String fieldType;
    String dynamicBase;
    boolean isRequired;
    boolean isMultiValued;
    boolean isDocValues;
    boolean isStored;
    String fieldTypeClass;
  }

  public static Map<String, SolrFieldMeta> getSchemaFields(String solrBaseUrl, String collection) {
      String lukeUrl = solrBaseUrl+collection+"/admin/luke?numTerms=0";
      // collect mapping of Solr field to type
      Map<String,SolrFieldMeta> schemaFieldMap = new HashMap<String,SolrFieldMeta>();
      try {
          try {
              Map<String, Object> adminMeta = SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient(), lukeUrl, 2);
              Map<String, Object> fieldsMap = SolrJsonSupport.asMap("/fields", adminMeta);
              Set<String> fieldNamesSet = fieldsMap.keySet();
              schemaFieldMap = getFieldTypes(fieldNamesSet.toArray(new String[fieldNamesSet.size()]), solrBaseUrl, collection);
          } catch (SolrException solrExc) {
              log.warn("Can't get field types for " + collection+" due to: "+solrExc);
          }
      } catch (Exception exc) {
          log.warn("Can't get schema fields for " + collection + " due to: "+exc);
      }
      return schemaFieldMap;
  }

  public static Map<String,SolrFieldMeta> getFieldTypes(String[] fields, String solrBaseUrl, String collection) {

    // specific field list
    StringBuilder sb = new StringBuilder();
    if (fields.length > 0) sb.append("&fl=");
    for (int f=0; f < fields.length; f++) {
      if (f > 0) sb.append(",");
      sb.append(fields[f]);
    }
    String fl = sb.toString();

    String fieldsUrl = solrBaseUrl+collection+"/schema/fields?showDefaults=true&includeDynamic=true"+fl;
    List<Map<String, Object>> fieldInfoFromSolr = null;
    try {
      Map<String, Object> allFields =
              SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient(), fieldsUrl, 2);
      fieldInfoFromSolr = (List<Map<String, Object>>)allFields.get("fields");
    } catch (Exception exc) {
      String errMsg = "Can't get field metadata from Solr using request "+fieldsUrl+" due to: " + exc;
      log.error(errMsg);
      if (exc instanceof RuntimeException) {
        throw (RuntimeException)exc;
      } else {
        throw new RuntimeException(errMsg, exc);
      }
    }

    // avoid looking up field types more than once
    Map<String,String> fieldTypeToClassMap = new HashMap<String,String>();

    // collect mapping of Solr field to type
    Map<String,SolrFieldMeta> fieldTypeMap = new HashMap<String,SolrFieldMeta>();
    for (String field : fields) {

      if (fieldTypeMap.containsKey(field))
        continue;

      SolrFieldMeta tvc = null;
      for (Map<String,Object> map : fieldInfoFromSolr) {
        String fieldName = (String)map.get("name");
        if (field.equals(fieldName)) {
          tvc = new SolrFieldMeta();
          tvc.fieldType = (String)map.get("type");
          Object required = map.get("required");
          if (required != null && required instanceof Boolean) {
            tvc.isRequired = ((Boolean)required).booleanValue();
          } else {
            tvc.isRequired = "true".equals(String.valueOf(required));
          }
          Object multiValued = map.get("multiValued");
          if (multiValued != null && multiValued instanceof Boolean) {
            tvc.isMultiValued = ((Boolean)multiValued).booleanValue();
          } else {
            tvc.isMultiValued = "true".equals(String.valueOf(multiValued));
          }
          Object docValues = map.get("docValues");
          if (docValues != null && docValues instanceof Boolean) {
            tvc.isDocValues = ((Boolean)docValues).booleanValue();
          } else {
            tvc.isDocValues = "true".equals(String.valueOf(docValues));
          }
          Object stored = map.get("stored");
          if (stored != null && stored instanceof Boolean) {
            tvc.isStored = ((Boolean)stored).booleanValue();
          } else {
            tvc.isStored = "true".equals(String.valueOf(stored));
          }
          Object dynamicBase = map.get("dynamicBase");
          if (dynamicBase != null && dynamicBase instanceof String) {
            tvc.dynamicBase = (String)dynamicBase;
          }
        }
      }

      if (tvc == null || tvc.fieldType == null) {
        String errMsg = "Can't figure out field type for field: " + field + ". Check you Solr schema and retry.";
        log.error(errMsg);
        throw new RuntimeException(errMsg);
      }

      String fieldTypeClass = fieldTypeToClassMap.get(tvc.fieldType);
      if (fieldTypeClass != null) {
        tvc.fieldTypeClass = fieldTypeClass;
      } else {
        String fieldTypeUrl = solrBaseUrl+collection+"/schema/fieldtypes/"+tvc.fieldType;
        try {
          Map<String, Object> fieldTypeMeta =
                  SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient(), fieldTypeUrl, 2);
          tvc.fieldTypeClass = SolrJsonSupport.asString("/fieldType/class", fieldTypeMeta);
          fieldTypeToClassMap.put(tvc.fieldType, tvc.fieldTypeClass);
        } catch (Exception exc) {
          String errMsg = "Can't get field type metadata for "+tvc.fieldType+" from Solr due to: " + exc;
          log.error(errMsg);
          if (exc instanceof RuntimeException) {
            throw (RuntimeException)exc;
          } else {
            throw new RuntimeException(errMsg, exc);
          }
        }
      }
      if (!(tvc.isStored || tvc.isDocValues)) {
        log.info("Can't retrieve an index only field: " + field);
        tvc = null;
      }
      if (tvc != null && !tvc.isStored && tvc.isMultiValued && tvc.isDocValues) {
        log.info("Can't retrieve a non stored multiValued docValues field: " + field);
        tvc = null;
      }
      if (tvc != null) {
        fieldTypeMap.put(field, tvc);
      }
    }
    if (fieldTypeMap.isEmpty()) {
      log.warn("No readable fields found!");
    }
    return fieldTypeMap;
  }

  public static QueryResponse querySolr(SolrClient solrServer, SolrQuery solrQuery, int startIndex, String cursorMark, String uniqueKey, StructType schema) throws SolrServerException {
    return querySolr(solrServer, solrQuery, startIndex, cursorMark, null, uniqueKey, schema);
  }

  public static QueryResponse querySolr(SolrClient solrServer, SolrQuery solrQuery, int startIndex, String cursorMark, StreamingResponseCallback callback, String uniqueKey, StructType schema) throws SolrServerException {
    QueryResponse resp = null;
    try {
      if (cursorMark != null) {
        solrQuery.setStart(0);
        solrQuery.set("cursorMark", cursorMark);
      } else {
        solrQuery.setStart(startIndex);
      }

      String fields = solrQuery.getFields();
      if (fields != null) {
          applyFields(fields.split(","), solrQuery, schema);
      }

      Integer rows = solrQuery.getRows();
      if (rows == null)
          solrQuery.setRows(DEFAULT_PAGE_SIZE);

      String sorts = solrQuery.getSortField();
      if (sorts == null || sorts.isEmpty())
          solrQuery.addSort(SolrQuery.SortClause.asc(uniqueKey));

      if (callback != null) {
        resp = solrServer.queryAndStreamResponse(solrQuery, callback);
      } else {
        resp = solrServer.query(solrQuery);
      }
    } catch (Exception exc) {

      log.error("Query ["+solrQuery+"] failed due to: "+exc);

      // re-try once in the event of a communications error with the server
      Throwable rootCause = SolrException.getRootCause(exc);
      boolean wasCommError =
        (rootCause instanceof ConnectException ||
          rootCause instanceof IOException ||
          rootCause instanceof SocketException);
      if (wasCommError) {
        try {
          Thread.sleep(2000L);
        } catch (InterruptedException ie) {
          Thread.interrupted();
        }

        try {
          if (callback != null) {
            resp = solrServer.queryAndStreamResponse(solrQuery, callback);
          } else {
            resp = solrServer.query(solrQuery);
          }
        } catch (Exception excOnRetry) {
          if (excOnRetry instanceof SolrServerException) {
            throw (SolrServerException)excOnRetry;
          } else {
            throw new SolrServerException(excOnRetry);
          }
        }
      } else {
        if (exc instanceof SolrServerException) {
          throw (SolrServerException)exc;
        } else {
          throw new SolrServerException(exc);
        }
      }
    }

    return resp;
  }

  public static DataType getsqlDataType(String s) {
    if (s.toLowerCase().equals("double")) {
      return DataTypes.DoubleType;
    }
    if (s.toLowerCase().equals("byte")) {
      return DataTypes.ByteType;
    }
    if (s.toLowerCase().equals("short")) {
      return DataTypes.ShortType;
    }
    if (((s.toLowerCase().equals("int")) || (s.toLowerCase().equals("integer")))) {
      return DataTypes.IntegerType;
    }
    if (s.toLowerCase().equals("long")) {
      return DataTypes.LongType;
    }
    if (s.toLowerCase().equals("String")) {
      return DataTypes.StringType;
    }
    if (s.toLowerCase().equals("boolean")) {
      return DataTypes.BooleanType;
    }
    if (s.toLowerCase().equals("timestamp")) {
      return DataTypes.TimestampType;
    }
    if (s.toLowerCase().equals("date")) {
      return DataTypes.DateType;
    }
    if (s.toLowerCase().equals("vector")) {
      return new VectorUDT();
    }
    if (s.toLowerCase().equals("matrix")) {
      return new MatrixUDT();
    }
    if (s.contains(":") && s.split(":")[0].toLowerCase().equals("array")) {
      return getArrayTypeRecurse(s,0);
    }
    return DataTypes.StringType;
  }

  public static DataType getArrayTypeRecurse(String s, int fromIdx) {
    if (s.contains(":") && s.split(":")[1].toLowerCase().equals("array")) {
      fromIdx = s.indexOf(":", fromIdx);
      s = s.substring(fromIdx+1, s.length());
      return DataTypes.createArrayType(getArrayTypeRecurse(s,fromIdx));
    }
    return DataTypes.createArrayType(getsqlDataType(s.split(":")[1]));
  }

  public static final class PivotField implements Serializable {
    public final String solrField;
    public final String prefix;
    public final String otherSuffix;
    public final int maxCols;

    public PivotField(String solrField, String prefix) {
      this(solrField, prefix, 10);
    }

    public PivotField(String solrField, String prefix, int maxCols) {
      this(solrField, prefix, maxCols, "other");
    }

    public PivotField(String solrField, String prefix, int maxCols, String otherSuffix) {
      this.solrField = solrField;
      this.prefix = prefix;
      this.maxCols = maxCols;
      this.otherSuffix = otherSuffix;
    }
  }

  public static final int[] getPivotFieldRange(StructType schema, String pivotPrefix) {
    StructField[] schemaFields = schema.fields();
    int startAt = -1;
    int endAt = -1;
    for (int f=0; f < schemaFields.length; f++) {
      String name = schemaFields[f].name();
      if (startAt == -1 && name.startsWith(pivotPrefix)) {
        startAt = f;
      }
      if (startAt != -1 && !name.startsWith(pivotPrefix)) {
        endAt = f-1; // we saw the last field in the range before this field
        break;
      }
    }
    return new int[]{startAt,endAt};
  }

  public static final void fillPivotFieldValues(String rawValue, Object[] row, StructType schema, String pivotPrefix) {
    int[] range = getPivotFieldRange(schema, pivotPrefix);
    for (int i=range[0]; i <= range[1]; i++) row[i] = 0;
    try {
      row[schema.fieldIndex(pivotPrefix+rawValue.toLowerCase())] = 1;
    } catch (IllegalArgumentException ia) {
      row[range[1]] = 1;
    }
  }

  public static StructType getBaseSchema(String zkHost, String collection, boolean escapeFields) throws Exception {
      String solrBaseUrl = SolrSupport.getSolrBaseUrl(zkHost);
      Map<String,SolrFieldMeta> fieldTypeMap = SolrQuerySupport.getSchemaFields(solrBaseUrl, collection);

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
}