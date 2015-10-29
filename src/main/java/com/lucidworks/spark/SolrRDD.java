package com.lucidworks.spark;

import java.io.IOException;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.lucidworks.spark.query.*;
import com.lucidworks.spark.util.SolrJsonSupport;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.*;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.MatrixUDT;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.DataFrame;

import org.apache.spark.sql.types.*;
import org.apache.spark.sql.Row;

public class SolrRDD implements Serializable {

  public static Logger log = Logger.getLogger(SolrRDD.class);

  public static final int DEFAULT_PAGE_SIZE = 1000;

  public static SolrQuery ALL_DOCS = toQuery(null);

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

    List<SolrQuery.SortClause> sorts = q.getSorts();
    if (sorts == null || sorts.isEmpty())
      q.addSort(SolrQuery.SortClause.asc("id"));

    return q;
  }

  /**
   * Iterates over the entire results set of a query (all hits).
   */
  public static class QueryResultsIterator extends PagedResultsIterator<SolrDocument> {

    public QueryResultsIterator(SolrClient solrServer, SolrQuery solrQuery, String cursorMark) {
      super(solrServer, solrQuery, cursorMark);
    }

    protected List<SolrDocument> processQueryResponse(QueryResponse resp) {
      return resp.getResults();
    }
  }

  /**
   * Returns an iterator over TermVectors
   */
  private class TermVectorIterator extends PagedResultsIterator<Vector> {

    private String field = null;
    private HashingTF hashingTF = null;

    private TermVectorIterator(SolrClient solrServer, SolrQuery solrQuery, String cursorMark, String field, int numFeatures) {
      super(solrServer, solrQuery, cursorMark);
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

  public static CloudSolrClient getSolrClient(String zkHost) {
    return SolrSupport.getSolrServer(zkHost);
  }

  protected String zkHost;
  protected String collection;
  protected transient JavaSparkContext sc;

  public void setSc(JavaSparkContext jsc){
    sc = jsc;
  }
  public JavaSparkContext getSc() {
    return sc;
  }

  public SolrRDD(String collection) {
    this("localhost:9983", collection); // assume local embedded ZK if not supplied
  }

  public SolrRDD(String zkHost, String collection) {
    this.zkHost = zkHost;
    this.collection = collection;
  }

  public String getCollection() {
    return collection;
  }

  /**
   * Get a document by ID using real-time get
   */
  public JavaRDD<SolrDocument> get(JavaSparkContext jsc, final String docId) throws SolrServerException {
    CloudSolrClient cloudSolrServer = getSolrClient(zkHost);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("collection", collection);
    params.set("qt", "/get");
    params.set("id", docId);
    QueryResponse resp = null;
    try {
      resp = cloudSolrServer.query(params);
    } catch (Exception exc) {
      if (exc instanceof SolrServerException) {
        throw (SolrServerException)exc;
      } else {
        throw new SolrServerException(exc);
      }
    }
    SolrDocument doc = (SolrDocument) resp.getResponse().get("doc");
    List<SolrDocument> list = (doc != null) ? Arrays.asList(doc) : new ArrayList<SolrDocument>();
    return jsc.parallelize(list, 1);
  }

  public JavaRDD<SolrDocument> query(JavaSparkContext jsc, final SolrQuery query, boolean useDeepPagingCursor) throws SolrServerException {
    if (useDeepPagingCursor)
      return queryDeep(jsc, query);

    query.set("collection", collection);
    CloudSolrClient cloudSolrServer = getSolrClient(zkHost);
    List<SolrDocument> results = new ArrayList<SolrDocument>();
    Iterator<SolrDocument> resultsIter = new QueryResultsIterator(cloudSolrServer, query, null);
    while (resultsIter.hasNext()) results.add(resultsIter.next());
    return jsc.parallelize(results, 1);
  }

  /**
   * Makes it easy to query from the Spark shell.
   */
  public JavaRDD<SolrDocument> query(SparkContext sc, String queryStr) throws SolrServerException {
    return queryShards(new JavaSparkContext(sc), toQuery(queryStr));
  }

  public JavaRDD<SolrDocument> query(SparkContext sc, SolrQuery solrQuery) throws SolrServerException {
    return queryShards(new JavaSparkContext(sc), solrQuery);
  }

  public JavaRDD<SolrDocument> queryShards(JavaSparkContext jsc, final SolrQuery origQuery) throws SolrServerException {
    // first get a list of replicas to query for this collection
    List<String> shards = buildShardList(getSolrClient(zkHost));

    final SolrQuery query = origQuery.getCopy();

    // we'll be directing queries to each shard, so we don't want distributed
    query.set("distrib", false);
    query.set("collection", collection);
    query.setStart(0);
    if (query.getRows() == null)
      query.setRows(DEFAULT_PAGE_SIZE); // default page size

    // parallelize the requests to the shards
    JavaRDD<SolrDocument> docs = jsc.parallelize(shards, shards.size()).flatMap(
      new FlatMapFunction<String, SolrDocument>() {
        public Iterable<SolrDocument> call(String shardUrl) throws Exception {
          return new StreamingResultsIterator(SolrSupport.getHttpSolrClient(shardUrl), query, "*");
        }
      }
    );
    return docs;
  }

  public JavaRDD<ShardSplit> splitShard(JavaSparkContext jsc, final SolrQuery origQuery, List<String> shards, final String splitFieldName, final int splitsPerShard) {
    final SolrQuery query = origQuery.getCopy();
    query.set("distrib", false);
    query.set("collection", collection);
    query.setStart(0);
    if (query.getRows() == null)
      query.setRows(DEFAULT_PAGE_SIZE); // default page size

    // get field type of split field
    final DataType fieldDataType;
    if ("_version_".equals(splitFieldName)) {
      fieldDataType = DataTypes.LongType;
    } else {
      Map<String,SolrFieldMeta> fieldMetaMap = getFieldTypes(new String[]{splitFieldName}, shards.get(0), collection);
      SolrFieldMeta solrFieldMeta = fieldMetaMap.get(splitFieldName);
      if (solrFieldMeta != null) {
        String fieldTypeClass = solrFieldMeta.fieldTypeClass;
        fieldDataType = solrDataTypes.get(fieldTypeClass);
      } else {
        log.warn("No field metadata found for "+splitFieldName+", assuming it is a String!");
        fieldDataType = DataTypes.StringType;
      }
      if (fieldDataType == null)
        throw new IllegalArgumentException("Cannot determine DataType for split field "+splitFieldName);
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

        log.info("Found "+splits.size()+" splits for "+splitFieldName+": "+splits);

        return splits;
      }
    });
    return splitsRDD;
  }

  public JavaRDD<SolrDocument> queryShards(JavaSparkContext jsc, final SolrQuery origQuery, final String splitFieldName, final int splitsPerShard) throws SolrServerException {
    // if only doing 1 split per shard, then queryShards does that already
    if (splitFieldName == null || splitsPerShard <= 1)
      return queryShards(jsc, origQuery);

    long timerDiffMs = 0L;
    long timerStartMs = 0L;

    // first get a list of replicas to query for this collection
    List<String> shards = buildShardList(getSolrClient(zkHost));

    timerStartMs = System.currentTimeMillis();

    // we'll be directing queries to each shard, so we don't want distributed
    JavaRDD<ShardSplit> splitsRDD = splitShard(jsc, origQuery, shards, splitFieldName, splitsPerShard);
    List<ShardSplit> splits = splitsRDD.collect();
    timerDiffMs = (System.currentTimeMillis() - timerStartMs);
    log.info("Collected "+splits.size()+" splits, took "+timerDiffMs+"ms");

    // parallelize the requests to the shards
    JavaRDD<SolrDocument> docs = jsc.parallelize(splits, splits.size()).flatMap(
      new FlatMapFunction<ShardSplit, SolrDocument>() {
        public Iterable<SolrDocument> call(ShardSplit split) throws Exception {
          return new StreamingResultsIterator(SolrSupport.getHttpSolrClient(split.getShardUrl()), split.getSplitQuery(), "*");
        }
      }
    );
    return docs;
  }

  public JavaRDD<Vector> queryTermVectors(JavaSparkContext jsc, final SolrQuery query, final String field, final int numFeatures) throws SolrServerException {
    // first get a list of replicas to query for this collection
    List<String> shards = buildShardList(getSolrClient(zkHost));

    if (query.getRequestHandler() == null) {
      query.setRequestHandler("/tvrh");
    }
    query.set("shards.qt", query.getRequestHandler());

    query.set("tv.fl", field);
    query.set("fq", field + ":[* TO *]"); // terms field not null!
    query.set("tv.tf_idf", "true");

    // we'll be directing queries to each shard, so we don't want distributed
    query.set("distrib", false);
    query.set("collection", collection);
    query.setStart(0);
    if (query.getRows() == null)
      query.setRows(DEFAULT_PAGE_SIZE); // default page size

    // parallelize the requests to the shards
    JavaRDD<Vector> docs = jsc.parallelize(shards, shards.size()).flatMap(
      new FlatMapFunction<String, Vector>() {
        public Iterable<Vector> call(String shardUrl) throws Exception {
          return new TermVectorIterator(SolrSupport.getHttpSolrClient(shardUrl), query, "*", field, numFeatures);
        }
      }
    );
    return docs;
  }

  // TODO: need to build up a LBSolrServer here with all possible replicas

  public List<String> buildShardList(CloudSolrClient cloudSolrServer) {
    ZkStateReader zkStateReader = cloudSolrServer.getZkStateReader();

    ClusterState clusterState = zkStateReader.getClusterState();

    String[] collections = null;
    if (clusterState.hasCollection(collection)) {
      collections = new String[]{collection};
    } else {
      // might be a collection alias?
      Aliases aliases = zkStateReader.getAliases();
      String aliasedCollections = aliases.getCollectionAlias(collection);
      if (aliasedCollections == null)
        throw new IllegalArgumentException("Collection " + collection + " not found!");
      collections = aliasedCollections.split(",");
    }

    Set<String> liveNodes = clusterState.getLiveNodes();
    Random random = new Random(5150);

    List<String> shards = new ArrayList<String>();
    for (String coll : collections) {
      for (Slice slice : clusterState.getSlices(coll)) {
        List<String> replicas = new ArrayList<String>();
        for (Replica r : slice.getReplicas()) {
          if (r.getState().equals(Replica.State.ACTIVE)) {
            ZkCoreNodeProps replicaCoreProps = new ZkCoreNodeProps(r);
            if (liveNodes.contains(replicaCoreProps.getNodeName()))
              replicas.add(replicaCoreProps.getCoreUrl());
          }
        }
        int numReplicas = replicas.size();
        if (numReplicas == 0)
          throw new IllegalStateException("Shard " + slice.getName() + " in collection "+
                  coll+" does not have any active replicas!");

        String replicaUrl = (numReplicas == 1) ? replicas.get(0) : replicas.get(random.nextInt(replicas.size()));
        shards.add(replicaUrl);
      }
    }
    return shards;
  }

  public JavaRDD<SolrDocument> queryDeep(JavaSparkContext jsc, final SolrQuery origQuery) throws SolrServerException {
    return queryDeep(jsc, origQuery, 36);
  }

  public JavaRDD<SolrDocument> queryDeep(JavaSparkContext jsc, final SolrQuery origQuery, final int maxPartitions) throws SolrServerException {

    final SolrClient solrClient = getSolrClient(zkHost);
    final SolrQuery query = origQuery.getCopy();
    query.set("collection", collection);
    query.setStart(0);
    if (query.getRows() == null)
      query.setRows(DEFAULT_PAGE_SIZE); // default page size

    long startMs = System.currentTimeMillis();
    List<String> cursors = collectCursors(solrClient, query, true);
    long tookMs = System.currentTimeMillis() - startMs;
    log.info("Took "+tookMs+"ms to collect "+cursors.size()+" cursor marks");
    int numPartitions = Math.min(maxPartitions,cursors.size());

    JavaRDD<String> cursorJavaRDD = jsc.parallelize(cursors, numPartitions);
    // now we need to execute all the cursors in parallel
    JavaRDD<SolrDocument> docs = cursorJavaRDD.flatMap(
      new FlatMapFunction<String, SolrDocument>() {
        public Iterable<SolrDocument> call(String cursorMark) throws Exception {
          return querySolr(getSolrClient(zkHost), query, 0, cursorMark).getResults();
        }
      }
    );
    return docs;
  }

  protected List<String> collectCursors(final SolrClient solrClient, final SolrQuery origQuery) throws SolrServerException {
    return collectCursors(solrClient, origQuery, false);
  }

  protected List<String> collectCursors(final SolrClient solrClient, final SolrQuery origQuery, final boolean distrib) throws SolrServerException {
    List<String> cursors = new ArrayList<String>();

    final SolrQuery query = origQuery.getCopy();
    // tricky - if distrib == false, then set the param, otherwise, leave it out (default is distrib=true)
    if (!distrib) {
      query.set("distrib", false);
    } else {
      query.remove("distrib");
    }
    query.setFields("id");

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

  private static final Map<String,DataType> solrDataTypes = new HashMap<String, DataType>();
  static {
    solrDataTypes.put("solr.StrField", DataTypes.StringType);
    solrDataTypes.put("solr.TextField", DataTypes.StringType);
    solrDataTypes.put("solr.BoolField", DataTypes.BooleanType);
    solrDataTypes.put("solr.TrieIntField", DataTypes.IntegerType);
    solrDataTypes.put("solr.TrieLongField", DataTypes.LongType);
    solrDataTypes.put("solr.TrieFloatField", DataTypes.FloatType);
    solrDataTypes.put("solr.TrieDoubleField", DataTypes.DoubleType);
    solrDataTypes.put("solr.TrieDateField", DataTypes.TimestampType);
  }

  public DataFrame asTempTable(SQLContext sqlContext, String queryString, String tempTable) throws Exception {
    SolrQuery solrQuery = toQuery(queryString);
    DataFrame rows = applySchema(sqlContext, solrQuery, query(sqlContext.sparkContext(), solrQuery));
    rows.registerTempTable(tempTable);
    return rows;
  }

  public DataFrame queryForRows(SQLContext sqlContext, String queryString) throws Exception {
    SolrQuery solrQuery = toQuery(queryString);
    return applySchema(sqlContext, solrQuery, query(sqlContext.sparkContext(), solrQuery));
  }

  public DataFrame applySchema(SQLContext sqlContext, SolrQuery query, JavaRDD<SolrDocument> docs) throws Exception {
    // now convert each SolrDocument to a Row object
    StructType schema = getQuerySchema(query);
    JavaRDD<Row> rows = toRows(schema, docs);
    return sqlContext.applySchema(rows, schema);
  }

  public JavaRDD<Row> toRows(StructType schema, JavaRDD<SolrDocument> docs) {
    final String[] queryFields = schema.fieldNames();
    JavaRDD<Row> rows = docs.map(new Function<SolrDocument, Row>() {
      public Row call(SolrDocument doc) throws Exception {
        Object[] vals = new Object[queryFields.length];
        for (int f = 0; f < queryFields.length; f++) {
          Object fieldValue = doc.getFieldValue(queryFields[f]);
          if (fieldValue != null) {
            if (fieldValue instanceof Collection) {
              vals[f] = ((Collection) fieldValue).toArray();
            } else if (fieldValue instanceof Date) {
              vals[f] = new java.sql.Timestamp(((Date)fieldValue).getTime());
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

  public StructType getQuerySchema(SolrQuery query) throws Exception {
    CloudSolrClient solrServer = getSolrClient(zkHost);
    // Build up a schema based on the fields requested
    String fieldList = query.getFields();
    String[] fields = null;
    if (fieldList != null) {
      fields = query.getFields().split(",");
    } else {
      // just go out to Solr and get 10 docs and extract a field list from that
      SolrQuery probeForFieldsQuery = query.getCopy();
      probeForFieldsQuery.remove("distrib");
      probeForFieldsQuery.set("collection", collection);
      probeForFieldsQuery.set("fl", "*");
      probeForFieldsQuery.setStart(0);
      probeForFieldsQuery.setRows(10);
      QueryResponse probeForFieldsResp = solrServer.query(probeForFieldsQuery);
      SolrDocumentList hits = probeForFieldsResp.getResults();
      Set<String> fieldSet = new TreeSet<String>();
      for (SolrDocument hit : hits)
        fieldSet.addAll(hit.getFieldNames());
      fields = fieldSet.toArray(new String[0]);
    }

    if (fields == null || fields.length == 0)
      throw new IllegalArgumentException("Query ("+query+") does not specify any fields needed to build a schema!");

    Set<String> liveNodes = solrServer.getZkStateReader().getClusterState().getLiveNodes();
    if (liveNodes.isEmpty())
      throw new RuntimeException("No live nodes found for cluster: "+zkHost);
    String solrBaseUrl = solrServer.getZkStateReader().getBaseUrlForNodeName(liveNodes.iterator().next());
    if (!solrBaseUrl.endsWith("?"))
      solrBaseUrl += "/";

    Map<String,SolrFieldMeta> fieldTypeMap = getFieldTypes(fields, solrBaseUrl, collection);
    List<StructField> listOfFields = new ArrayList<StructField>();
    for (String field : fields) {
      SolrFieldMeta fieldMeta = fieldTypeMap.get(field);
      DataType dataType = (fieldMeta != null) ? solrDataTypes.get(fieldMeta.fieldTypeClass) : null;
      if (dataType == null) dataType = DataTypes.StringType;

      if (fieldMeta != null && fieldMeta.isMultiValued) {
        dataType = new ArrayType(dataType, true);
      }

      boolean nullable = !"id".equals(field);

      listOfFields.add(DataTypes.createStructField(field, dataType, nullable));
    }

    return DataTypes.createStructType(listOfFields);
  }

  private static class SolrFieldMeta {
    String fieldType;
    boolean isMultiValued;
    String fieldTypeClass;
  }

  private static Map<String,SolrFieldMeta> getFieldTypes(String[] fields, String solrBaseUrl, String collection) {

    // collect mapping of Solr field to type
    Map<String,SolrFieldMeta> fieldTypeMap = new HashMap<String,SolrFieldMeta>();
    for (String field : fields) {

      if (fieldTypeMap.containsKey(field))
        continue;

      // Hit Solr Schema API to get field type for field
      String fieldUrl = solrBaseUrl+collection+"/schema/fields/"+field;
      try {
        SolrFieldMeta tvc = null;
        try {
          Map<String, Object> fieldMeta =
            SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient(), fieldUrl, 2);
          tvc = new SolrFieldMeta();
          tvc.fieldType = SolrJsonSupport.asString("/field/type", fieldMeta);
          tvc.isMultiValued = SolrJsonSupport.asBool("/field/multiValued", fieldMeta);
        } catch (SolrException solrExc) {
          int errCode = solrExc.code();
          if (errCode == 404) {
            int lio = field.lastIndexOf('_');
            if (lio != -1) {
              // see if the field is a dynamic field
              String dynField = "*"+field.substring(lio);

              tvc = fieldTypeMap.get(dynField);
              if (tvc == null) {
                String dynamicFieldsUrl = solrBaseUrl+collection+"/schema/dynamicfields/"+dynField;
                try {
                  Map<String, Object> dynFieldMeta =
                    SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient(), dynamicFieldsUrl, 2);
                  tvc = new SolrFieldMeta();
                  tvc.fieldType = SolrJsonSupport.asString("/dynamicField/type", dynFieldMeta);
                  tvc.isMultiValued = SolrJsonSupport.asBool("/dynamicField/multiValued", dynFieldMeta);

                  fieldTypeMap.put(dynField, tvc);
                } catch (Exception exc) {
                  // just ignore this and throw the outer exc
                  log.error("Failed to get dynamic field information for "+dynField+" due to: "+exc);
                  throw solrExc;
                }
              }
            }
          } else {
            log.warn("No field metadata from Solr Schema API for "+field+" due to: "+solrExc);
          }
        }

        if (tvc == null || tvc.fieldType == null) {
          log.warn("Can't figure out field type for field: " + field);
          continue;
        }

        String fieldTypeUrl = solrBaseUrl+collection+"/schema/fieldtypes/"+tvc.fieldType;
        Map<String, Object> fieldTypeMeta =
          SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient(), fieldTypeUrl, 2);

        tvc.fieldTypeClass = SolrJsonSupport.asString("/fieldType/class", fieldTypeMeta);

        fieldTypeMap.put(field, tvc);

      } catch (Exception exc) {
        log.warn("Can't get field type for field " + field+" due to: "+exc);
      }
    }

    return fieldTypeMap;
  }

  public Map<String,Double> getLabels(String labelField) throws SolrServerException {
    SolrQuery solrQuery = new SolrQuery("*:*");
    solrQuery.setRows(0);
    solrQuery.set("collection", collection);
    solrQuery.addFacetField(labelField);
    solrQuery.setFacetMinCount(1);
    QueryResponse qr = querySolr(getSolrClient(zkHost), solrQuery, 0, null);
    List<String> values = new ArrayList<>();
    for (FacetField.Count f : qr.getFacetField(labelField).getValues()) {
      values.add(f.getName());
    }

    Collections.sort(values);
    final Map<String,Double> labelMap = new HashMap<>();
    double d = 0d;
    for (String label : values) {
      labelMap.put(label, new Double(d));
      d += 1d;
    }

    return labelMap;
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

}
