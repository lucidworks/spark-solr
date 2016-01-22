package com.lw.spark.query;

import com.lucidworks.spark.SolrSupport;
import com.lucidworks.spark.query.NumberFieldShardSplitStrategy;
import com.lucidworks.spark.query.ShardSplit;
import com.lucidworks.spark.query.ShardSplitStrategy;
import com.lucidworks.spark.query.StringFieldShardSplitStrategy;
import com.lucidworks.spark.util.SolrJsonSupport;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static com.lw.spark.query.QueryConstants.*;

import java.io.IOException;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SolrQuerySupport implements Serializable {

  private static Logger log = Logger.getLogger(SolrQuerySupport.class);


  public static final Map<String,DataType> SOLR_DATA_TYPES = new HashMap<>();
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

  public static class SolrFieldMeta {
    String fieldType;
    String dynamicBase;
    boolean isRequired;
    boolean isMultiValued;
    boolean isDocValues;
    boolean isStored;
    String fieldTypeClass;
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
        log.warn("Can't get uniqueKey for " + collection + " due to solr: " + solrExc);
      }
    } catch (Exception exc) {
      log.warn("Can't get uniqueKey for " + collection + " due to: " + exc);
    }
    return DEFAULT_REQUIRED_FIELD;
  }

  public static SolrQuery toQuery(String queryString) {

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

    return q;
  }

  public static SolrQuery addDefaultSort(SolrQuery q, String uniqueKey) {
    String sorts = q.getSortField();
    if (sorts == null || sorts.isEmpty())
      q.addSort(SolrQuery.SortClause.asc(uniqueKey));

    return q;
  }

  public static QueryResponse querySolr(SolrClient solrServer, SolrQuery solrQuery, int startIndex, String cursorMark) throws SolrServerException {
    return querySolr(solrServer, solrQuery, startIndex, cursorMark, null);
  }

  public static QueryResponse querySolr(SolrClient solrServer, SolrQuery solrQuery, int startIndex, String cursorMark, StreamingResponseCallback callback) throws SolrServerException {
    QueryResponse resp = null;
    try {
      if (cursorMark != null) {
        solrQuery.setStart(0);
        solrQuery.set("cursorMark", cursorMark);
      } else {
        solrQuery.setStart(startIndex);
      }

      Integer rows = solrQuery.getRows();
      if (rows == null)
        solrQuery.setRows(DEFAULT_PAGE_SIZE);

      if (callback != null) {
        resp = solrServer.queryAndStreamResponse(solrQuery, callback);
      } else {
        resp = solrServer.query(solrQuery);
      }
    } catch (Exception exc) {

      log.error("Query [" + solrQuery + "] failed due to: " + exc);

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
            throw (SolrServerException) excOnRetry;
          } else {
            throw new SolrServerException(excOnRetry);
          }
        }
      } else {
        if (exc instanceof SolrServerException) {
          throw (SolrServerException) exc;
        } else {
          throw new SolrServerException(exc);
        }
      }
    }

    return resp;
  }

  // Query defaults when directing queries to each Shard
  public static void setQueryDefaultsForShards(SolrQuery query, String uniqueKey) {
    query.set("distrib", "false");
    query.setStart(0);
    if (query.getRows() == null) {
      query.setRows(DEFAULT_PAGE_SIZE);
    }
    SolrQuerySupport.addDefaultSort(query, uniqueKey);
  }

  // Query defaults for Term Vectors
  public static void setQueryDefaultsForTV(SolrQuery query, String field, String uniqueKey) {
     if (query.getRequestHandler() == null) {
      query.setRequestHandler("/tvrh");
    }
    query.set("shards.qt", query.getRequestHandler());

    query.set("tv.fl", field);
    query.set("fq", field + ":[* TO *]"); // terms field not null!
    query.set("tv.tf_idf", "true");

    // we'll be directing queries to each shard, so we don't want distributed
    query.set("distrib", false);
    query.setStart(0);
    if (query.getRows() == null)
      query.setRows(DEFAULT_PAGE_SIZE); // default page size

    SolrQuerySupport.addDefaultSort(query, uniqueKey);
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

  public static JavaRDD<ShardSplit> splitShard(JavaSparkContext jsc, final SolrQuery query, List<String> shards, final String splitFieldName, final int splitsPerShard, final String collection) {

    // get field type of split field
    final DataType fieldDataType;
    if ("_version_".equals(splitFieldName)) {
      fieldDataType = DataTypes.LongType;
    } else {
      Map<String, SolrFieldMeta> fieldMetaMap = SolrQuerySupport.getFieldTypes(new String[]{splitFieldName}, shards.get(0), collection);
      SolrFieldMeta solrFieldMeta = fieldMetaMap.get(splitFieldName);
      if (solrFieldMeta != null) {
        String fieldTypeClass = solrFieldMeta.fieldTypeClass;
        fieldDataType = SolrQuerySupport.SOLR_DATA_TYPES.get(fieldTypeClass);
      } else {
        log.warn("No field metadata found for " + splitFieldName + ", assuming it is a String!");
        fieldDataType = DataTypes.StringType;
      }
      if (fieldDataType == null)
        throw new IllegalArgumentException("Cannot determine DataType for split field " + splitFieldName);
    }

    JavaRDD<ShardSplit> splitsRDD = jsc.parallelize(shards, shards.size()).flatMap(new FlatMapFunction<String, ShardSplit>() {
      public Iterable<ShardSplit> call(String shardUrl) throws Exception {

        ShardSplitStrategy splitStrategy = null;
        if (fieldDataType == DataTypes.LongType || fieldDataType == DataTypes.IntegerType) {
          splitStrategy = new NumberFieldShardSplitStrategy();
        } else if (fieldDataType == DataTypes.StringType) {
          splitStrategy = new StringFieldShardSplitStrategy();
        } else {
          throw new IllegalArgumentException("Can only split shards on fields of type: long, int, or string!");
        }
        List<ShardSplit> splits =
          splitStrategy.getSplits(shardUrl, query, splitFieldName, splitsPerShard);

        log.info("Found " + splits.size() + " splits for " + splitFieldName + ": " + splits);

        return splits;
      }
    });
    return splitsRDD;
  }
}