package com.lucidworks.spark;

import com.lucidworks.spark.SolrQuerySupport.PivotField;
import com.lucidworks.spark.SolrQuerySupport.QueryResultsIterator;
import com.lucidworks.spark.SolrQuerySupport.SolrFieldMeta;
import com.lucidworks.spark.SolrQuerySupport.TermVectorIterator;
import com.lucidworks.spark.query.*;
import com.lucidworks.spark.util.ScalaUtil;
import com.lucidworks.spark.util.SolrJsonSupport;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class SolrRDD implements Serializable {

  public static Logger log = Logger.getLogger(SolrRDD.class);

  public static final int DEFAULT_PAGE_SIZE = 1000;
  public static String SOLR_ZK_HOST_PARAM = "zkhost";
  public static String SOLR_COLLECTION_PARAM = "collection";
  public static String SOLR_QUERY_PARAM = "query";
  public static String SOLR_FIELD_LIST_PARAM = "fields";
  public static String SOLR_ROWS_PARAM = "rows";
  public static String SOLR_SPLIT_FIELD_PARAM = "split_field";
  public static String SOLR_SPLITS_PER_SHARD_PARAM = "splits_per_shard";
  public static String SOLR_PARALLEL_SHARDS = "parallel_shards";
  public static String PRESERVE_SCHEMA = "preserveschema";
  public static String ESCAPE_FIELDNAMES = "escape_fieldnames";

  protected String zkHost;
  protected String collection;
  protected scala.collection.immutable.Map<String,String> config;
  protected Boolean escapeFields = false;
  protected StructType schema;
  private final String uniqueKey;
  protected transient JavaSparkContext sc;

  public SolrRDD(String collection) throws Exception {
    this("localhost:9983", collection); // assume local embedded ZK if not supplied
  }

  public SolrRDD(String zkHost, String collection) throws Exception {
      this(zkHost, collection, new scala.collection.immutable.HashMap<String,String>());
  }

  public SolrRDD(String zkHost, String collection, scala.collection.immutable.Map<String,String> config) throws Exception {
    this.zkHost = zkHost;
    this.collection = collection;
    this.config = config;
    this.escapeFields = Boolean.parseBoolean(ScalaUtil.optionalParam(config, ESCAPE_FIELDNAMES, "false"));
    this.uniqueKey = SolrQuerySupport.getUniqueKey(zkHost, collection);
    try {
      this.schema = SolrQuerySupport.getBaseSchema(zkHost, collection, escapeFields);
    } catch (Exception exc) {
      log.warn("No schema found " + exc);
      this.schema = null;
    }
  }

  public String getUniqueKey() {
    return this.uniqueKey;
  }

  public void setSc(JavaSparkContext jsc){
    sc = jsc;
  }

  public JavaSparkContext getSc() {
    return sc;
  }


  public String getCollection() {
    return collection;
  }

  public scala.collection.immutable.Map<String, String> getConfig() {
    return config;
  }

  public StructType getSchema() {
    return schema;
  }

  /**
   * Get a document by ID using real-time get
   */
  public JavaRDD<SolrDocument> get(JavaSparkContext jsc, final String docId) throws SolrServerException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("collection", collection);
    params.set("qt", "/get");
    params.set("id", docId);
    CloudSolrClient solrServer = SolrSupport.getSolrClient(zkHost);
    QueryResponse resp = null;
    try {
      resp = solrServer.query(params);
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

    CloudSolrClient solrServer = SolrSupport.getSolrClient(zkHost);
    query.set("collection", collection);
    List<SolrDocument> results = new ArrayList<SolrDocument>();
    Iterator<SolrDocument> resultsIter = new QueryResultsIterator(solrServer, query, null, uniqueKey, schema);
    while (resultsIter.hasNext()) results.add(resultsIter.next());
    return jsc.parallelize(results, 1);
  }

  /**
   * Makes it easy to query from the Spark shell.
   */
  public JavaRDD<SolrDocument> query(SparkContext sc, String queryStr) throws SolrServerException {
    return query(sc, SolrQuerySupport.toQuery(queryStr, uniqueKey));
  }

  public JavaRDD<SolrDocument> query(SparkContext sc, SolrQuery solrQuery) throws SolrServerException {
    return query(new JavaSparkContext(sc), solrQuery);
  }

  public JavaRDD<SolrDocument> query(JavaSparkContext jsc, SolrQuery solrQuery) throws SolrServerException {
      String splitFieldName = ScalaUtil.optionalParam(config, SolrRDD.SOLR_SPLIT_FIELD_PARAM, null);
      int splitsPerShard = 1;
      if (splitFieldName != null)
        splitsPerShard = Integer.parseInt(ScalaUtil.optionalParam(config, SolrRDD.SOLR_SPLITS_PER_SHARD_PARAM, "20"));
      boolean parallelShards = Boolean.parseBoolean(ScalaUtil.optionalParam(config, SolrRDD.SOLR_PARALLEL_SHARDS, "true"));
      return parallelShards ?
              queryShards(jsc, solrQuery, splitFieldName, splitsPerShard) :
              queryDeep(jsc, solrQuery);
  }

  public JavaRDD<SolrDocument> queryShards(JavaSparkContext jsc, final SolrQuery origQuery) throws SolrServerException {
    CloudSolrClient solrServer = SolrSupport.getSolrClient(zkHost);
    // first get a list of replicas to query for this collection
    List<String> shards = SolrSupport.buildShardList(solrServer, collection);

    final SolrQuery query = origQuery.getCopy();

    // we'll be directing queries to each shard, so we don't want distributed
    query.set("distrib", false);
    query.set("collection", collection);
    query.setStart(0);
    if (query.getRows() == null)
      query.setRows(DEFAULT_PAGE_SIZE); // default page size

    String sorts = query.getSortField();
    if (sorts == null || sorts.isEmpty())
        query.addSort(SolrQuery.SortClause.asc(uniqueKey));

    String fields = query.getFields();
    if (fields != null) {
        SolrQuerySupport.applyFields(fields.split(","), query, schema);
    }
    // parallelize the requests to the shards
    JavaRDD<SolrDocument> docs = jsc.parallelize(shards, shards.size()).flatMap(
      new FlatMapFunction<String, SolrDocument>() {
        public Iterable<SolrDocument> call(String shardUrl) throws Exception {
          return new StreamingResultsIterator(SolrSupport.getHttpSolrClient(shardUrl), query, "*", uniqueKey, schema);
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

    String sorts = query.getSortField();
    if (sorts == null || sorts.isEmpty())
        query.addSort(SolrQuery.SortClause.asc(uniqueKey));

    String fields = query.getFields();
    if (fields != null) {
        SolrQuerySupport.applyFields(fields.split(","), query, schema);
    }

    // get field type of split field
    final DataType fieldDataType;
    if ("_version_".equals(splitFieldName)) {
      fieldDataType = DataTypes.LongType;
    } else {
      Map<String,SolrFieldMeta> fieldMetaMap = SolrQuerySupport.getFieldTypes(new String[]{splitFieldName}, shards.get(0), collection);
      SolrFieldMeta solrFieldMeta = fieldMetaMap.get(splitFieldName);
      if (solrFieldMeta != null) {
        String fieldTypeClass = solrFieldMeta.fieldTypeClass;
        fieldDataType = SolrQuerySupport.SOLR_DATA_TYPES.get(fieldTypeClass);
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

        log.info("Found " + splits.size() + " splits for " + splitFieldName + ": " + splits);

        return splits;
      }
    });
    return splitsRDD;
  }

  public JavaRDD<SolrDocument> queryShards(JavaSparkContext jsc, final SolrQuery origQuery, final String splitFieldName, final int splitsPerShard) throws SolrServerException {
    // if only doing 1 split per shard, then queryShards does that already
    if (splitFieldName == null || splitsPerShard <= 1)
      return queryShards(jsc, origQuery);

    CloudSolrClient solrServer = SolrSupport.getSolrClient(zkHost);
    long timerDiffMs = 0L;
    long timerStartMs = 0L;

    // first get a list of replicas to query for this collection
    List<String> shards = SolrSupport.buildShardList(solrServer, collection);

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
          return new StreamingResultsIterator(SolrSupport.getHttpSolrClient(split.getShardUrl()), split.getSplitQuery(), "*", uniqueKey, schema);
        }
      }
    );
    return docs;
  }

  public JavaRDD<Vector> queryTermVectors(JavaSparkContext jsc, final SolrQuery query, final String field, final int numFeatures) throws SolrServerException {
    CloudSolrClient solrServer = SolrSupport.getSolrClient(zkHost);
    // first get a list of replicas to query for this collection
    List<String> shards = SolrSupport.buildShardList(solrServer, collection);

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

    String sorts = query.getSortField();
    if (sorts == null || sorts.isEmpty())
        query.addSort(SolrQuery.SortClause.asc(uniqueKey));

    String fields = query.getFields();
    if (fields != null) {
        SolrQuerySupport.applyFields(fields.split(","), query, schema);
    }

    // parallelize the requests to the shards
    JavaRDD<Vector> docs = jsc.parallelize(shards, shards.size()).flatMap(
      new FlatMapFunction<String, Vector>() {
        public Iterable<Vector> call(String shardUrl) throws Exception {
          return new TermVectorIterator(SolrSupport.getHttpSolrClient(shardUrl), query, "*", field, numFeatures, uniqueKey, schema);
        }
      }
    );
    return docs;
  }

  public JavaRDD<SolrDocument> queryDeep(JavaSparkContext jsc, final SolrQuery origQuery) throws SolrServerException {
    return queryDeep(jsc, origQuery, 36);
  }

  public JavaRDD<SolrDocument> queryDeep(JavaSparkContext jsc, final SolrQuery origQuery, final int maxPartitions) throws SolrServerException {

    final SolrQuery query = origQuery.getCopy();
    query.set("collection", collection);
    query.setStart(0);
    if (query.getRows() == null)
      query.setRows(DEFAULT_PAGE_SIZE); // default page size

    String sorts = query.getSortField();
    if (sorts == null || sorts.isEmpty())
        query.addSort(SolrQuery.SortClause.asc(uniqueKey));

    String fields = query.getFields();
    if (fields != null) {
        SolrQuerySupport.applyFields(fields.split(","), query, schema);
    }

    long startMs = System.currentTimeMillis();
    List<String> cursors = SolrQuerySupport.collectCursors(SolrSupport.getSolrClient(zkHost), query, true, uniqueKey);
    long tookMs = System.currentTimeMillis() - startMs;
    log.info("Took "+tookMs+"ms to collect "+cursors.size()+" cursor marks");
    int numPartitions = Math.min(maxPartitions, cursors.size());

    JavaRDD<String> cursorJavaRDD = jsc.parallelize(cursors, numPartitions);
    // now we need to execute all the cursors in parallel
    JavaRDD<SolrDocument> docs = cursorJavaRDD.flatMap(
      new FlatMapFunction<String, SolrDocument>() {
        public Iterable<SolrDocument> call(String cursorMark) throws Exception {
          return SolrQuerySupport.querySolr(SolrSupport.getSolrClient(zkHost), query, 0, cursorMark, uniqueKey, schema).getResults();
        }
      }
    );
    return docs;
  }

  public DataFrame asTempTable(SQLContext sqlContext, String queryString, String tempTable) throws Exception {
    SolrQuery solrQuery = SolrQuerySupport.toQuery(queryString, uniqueKey);
    DataFrame rows = applySchema(sqlContext, solrQuery, query(sqlContext.sparkContext(), solrQuery));
    rows.registerTempTable(tempTable);
    return rows;
  }

  public DataFrame queryForRows(SQLContext sqlContext, String queryString) throws Exception {
    SolrQuery solrQuery = SolrQuerySupport.toQuery(queryString, uniqueKey);
    return applySchema(sqlContext, solrQuery, query(sqlContext.sparkContext(), solrQuery));
  }

  public DataFrame applySchema(SQLContext sqlContext, SolrQuery query, JavaRDD<SolrDocument> docs) throws Exception {
    // now convert each SolrDocument to a Row object
    StructType schema = getQuerySchema(query);
    JavaRDD<Row> rows = toRows(schema, docs);
    return sqlContext.applySchema(rows, schema);
  }

  public JavaRDD<Row> toRows(StructType schema, JavaRDD<SolrDocument> docs) {
    final StructField[] fields = schema.fields();
    JavaRDD<Row> rows = docs.map(new Function<SolrDocument, Row>() {
      public Row call(SolrDocument doc) throws Exception {
        Object[] vals = new Object[fields.length];
        for (int f = 0; f < fields.length; f++) {
          StructField field = fields[f];
          Metadata meta = field.metadata();
          Boolean isMultiValued = meta.contains("multiValued") ? meta.getBoolean("multiValued") : false;
          Object fieldValue = isMultiValued ? doc.getFieldValues(field.name()) : doc.getFieldValue(field.name());;
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

  // derive a schema for a specific query from the full collection schema
  public StructType deriveQuerySchema(String[] fields) {
    Map<String, StructField> fieldMap = new HashMap<String, StructField>();
    for (StructField f : schema.fields()) fieldMap.put(f.name(), f);
    List<StructField> listOfFields = new ArrayList<StructField>();
    for (String field : fields) listOfFields.add(fieldMap.get(field));
    return DataTypes.createStructType(listOfFields);
  }

  public StructType getQuerySchema(SolrQuery query) throws Exception {
    String fieldList = query.getFields();
    if (fieldList != null && !fieldList.isEmpty()) {
      return deriveQuerySchema(fieldList.split(","));
    }
    return schema;
  }

  public Map<String,Double> getLabels(String labelField) throws SolrServerException {
    SolrQuery solrQuery = new SolrQuery("*:*");
    solrQuery.setRows(0);
    solrQuery.set("collection", collection);
    solrQuery.addFacetField(labelField);
    solrQuery.setFacetMinCount(1);

    QueryResponse qr = SolrQuerySupport.querySolr(SolrSupport.getSolrServer(zkHost), solrQuery, 0, null, uniqueKey, schema);
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

  /**
   * Allows you to pivot a categorical field into multiple columns that can be aggregated into counts, e.g.
   * a field holding HTTP method (http_verb=GET) can be converted into: http_method_get=1, which is a common
   * task when creating aggregations.
   */
  public DataFrame withPivotFields(final DataFrame solrData, final PivotField[] pivotFields) throws IOException, SolrServerException {

    final StructType schemaWithPivots = toPivotSchema(solrData.schema(), pivotFields);

    JavaRDD<Row> withPivotFields = solrData.javaRDD().map(new Function<Row, Row>() {
      @Override
      public Row call(Row row) throws Exception {
        Object[] fields = new Object[schemaWithPivots.size()];
        for (int c=0; c < row.length(); c++)
          fields[c] = row.get(c);

        for (PivotField pf : pivotFields)
          SolrQuerySupport.fillPivotFieldValues(row.getString(row.fieldIndex(pf.solrField)), fields, schemaWithPivots, pf.prefix);

        return RowFactory.create(fields);
      }
    });

    return solrData.sqlContext().createDataFrame(withPivotFields, schemaWithPivots);
  }

  public StructType toPivotSchema(final StructType baseSchema, final PivotField[] pivotFields) throws IOException, SolrServerException {
    List<StructField> pivotSchemaFields = new ArrayList<>();
    pivotSchemaFields.addAll(Arrays.asList(baseSchema.fields()));
    for (PivotField pf : pivotFields) {
      for (StructField sf : getPivotSchema(pf.solrField, pf.maxCols, pf.prefix, pf.otherSuffix)) {
        pivotSchemaFields.add(sf);
      }
    }
    return DataTypes.createStructType(pivotSchemaFields);
  }

  public List<StructField> getPivotSchema(String fieldName, int maxCols, String fieldPrefix, String otherName) throws IOException, SolrServerException {
    final List<StructField> listOfFields = new ArrayList<StructField>();
    SolrQuery q = new SolrQuery("*:*");
    q.set("collection", collection);
    q.setFacet(true);
    q.addFacetField(fieldName);
    q.setFacetMinCount(1);
    q.setFacetLimit(maxCols);
    q.setRows(0);
    FacetField ff = SolrQuerySupport.querySolr(SolrSupport.getSolrServer(zkHost), q, 0, null, uniqueKey, schema).getFacetField(fieldName);
    for (FacetField.Count f : ff.getValues()) {
      listOfFields.add(DataTypes.createStructField(fieldPrefix+f.getName().toLowerCase(), DataTypes.IntegerType, false));
    }
    if (otherName != null) {
      listOfFields.add(DataTypes.createStructField(fieldPrefix+otherName, DataTypes.IntegerType, false));
    }
    return listOfFields;
  }

}
