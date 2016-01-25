package com.lw.spark;

import com.lw.spark.util.SolrQuerySupport;
import com.lw.spark.rdd.SolrRDD;
import com.lw.spark.util.SolrSupport;
import com.lw.spark.util.ScalaUtil;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

import static com.lw.spark.util.QueryConstants.*;
import static com.lw.spark.util.ConfigurationConstants.*;

import java.io.Serializable;
import java.util.*;

public class SolrRelation extends BaseRelation implements Serializable, TableScan, PrunedFilteredScan, InsertableRelation {

  public static Logger log = Logger.getLogger(SolrRelation.class);

  protected String[] fieldList;
  protected Boolean preserveSchema = false;
  protected SolrQuery solrQuery;
  protected SolrRDD solrRDD;
  protected StructType schema;
  protected ModifiableSolrParams addlSolrParams;
  protected transient SQLContext sqlContext;

  protected Integer rows = null;

  protected transient JavaSparkContext jsc;

  public SolrRelation() {}

  public SolrRelation(SQLContext sqlContext, scala.collection.immutable.Map<String, String> config) throws Exception {
    this(sqlContext, config, null);
  }

  public SolrRelation(SQLContext sqlContext, scala.collection.immutable.Map<String, String> config, DataFrame dataFrame) throws Exception {

    if (sqlContext == null)
      throw new IllegalArgumentException("SQLContext cannot be null!");

    this.sqlContext = sqlContext;
    this.jsc = new JavaSparkContext(sqlContext.sparkContext());
    String preserveSch = ScalaUtil.optionalParam(config, PRESERVE_SCHEMA, "N");
    if ("Y".equals(preserveSch) || Boolean.parseBoolean(preserveSch)) {
      preserveSchema = true;
    };
    String zkHost = ScalaUtil.requiredParam(config, SOLR_ZK_HOST_PARAM);
    String collection = ScalaUtil.requiredParam(config, SOLR_COLLECTION_PARAM);
    String query = ScalaUtil.optionalParam(config, SOLR_QUERY_PARAM, "*:*");
    String fieldListParam = ScalaUtil.optionalParam(config, SOLR_FIELD_LIST_PARAM, null);
    if (fieldListParam != null) {
      this.fieldList = fieldListParam.split(",");
    } else {
      this.fieldList = null;
    }
    String rowsParam = ScalaUtil.optionalParam(config, SOLR_ROWS_PARAM, null);
    if (rowsParam != null) {
      this.rows = new Integer(rowsParam);
    }

    if (!preserveSchema) {
      solrRDD = new SolrRDD(zkHost, collection, config);
    }
    else {
      solrRDD = new SchemaPreservingSolrRDD(zkHost, collection, config);
      solrRDD.setSc(jsc);
    }

    solrQuery = SolrQuerySupport.toQuery(query, solrRDD.getUniqueKey());

    if (this.fieldList != null && this.fieldList.length > 0) {
      solrQuery.setFields(this.fieldList);
    } else {
      applyDefaultFields();
    }

    if (rows != null) {
      solrQuery.setRows(rows);
    }

    // iterate over the config object to pull out any params that
    // start with solr. to pass to the query as additional params,
    // but skip the q and fl as they are first-class options for this relation
    addlSolrParams = new ModifiableSolrParams();
    Map<String,String> configMap = JavaConverters.asJavaMapConverter(config).asJava();
    for (String key : configMap.keySet()) {
      if (key.startsWith("solr.")) {
        String param = key.substring(5);
        if ("q".equals(param) || "fl".equals(param) || "rows".equals(param))
          continue;

        String val = configMap.get(key);
        if (val != null) {
          addlSolrParams.add(param, val);
        }
      }
    }
    solrQuery.add(addlSolrParams);

    solrQuery.set("collection", collection);

    if (dataFrame != null) {
      schema = dataFrame.schema();
    } else if (this.fieldList != null && this.fieldList.length > 0) {
      schema = solrRDD.deriveQuerySchema(this.fieldList, solrRDD.schema);
    } else {
      schema = solrRDD.getQuerySchema(solrQuery);
    }
  }


  public SolrQuery getQuery() {
    return solrQuery;
  }

  @Override
  public SQLContext sqlContext() {
    return sqlContext;
  }

  @Override
  public StructType schema() {
    return schema;
  }

  public RDD<Row> buildScan() {
    return buildScan(null, null);
  }

  public synchronized RDD<Row> buildScan(String[] fields, Filter[] filters) {

    // SchemaPreserviing dataframes returns all fields by default
    if (!preserveSchema) {
      log.info("Building Solr scan using fields=" + (fields != null ? Arrays.asList(fields).toString() : ""));

      if (fields != null && fields.length > 0) {
        solrQuery.setFields(fields);
      } else {
        if (this.fieldList != null && this.fieldList.length > 0) {
          solrQuery.setFields(this.fieldList);
        } else {
          applyDefaultFields();
        }
      }
    } else {
      applyDefaultFields();
    }

    // clear all existing filters
    solrQuery.remove("fq");
    if (filters != null && filters.length > 0) {
    log.info("Building SolrQuery using filters: " + Arrays.asList(filters));
    for (Filter filter : filters)
      applyFilter(filter);
    }

    solrQuery.add(addlSolrParams);

    if (log.isInfoEnabled())
      log.info("Constructed SolrQuery: " + solrQuery);

    RDD<Row> rows = null;
    try {

      // build the schema based on the desired fields - applicable only for non-schemapreserving dataframes in solr
      StructType querySchema = (fields != null && fields.length > 0 && !preserveSchema) ? solrRDD.deriveQuerySchema(fields, solrRDD.schema) : schema;
      JavaRDD<SolrDocument> docs = solrRDD.query(jsc, solrQuery);
      rows = solrRDD.toRows(querySchema, docs).rdd();
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      else {
        throw new RuntimeException(e);
      }
    }
    return rows;
  }

  protected void applyDefaultFields() {
      StructField[] schemaFields = solrRDD.getSchema().fields();
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

  protected void applyFilter(Filter filter) {
    if (filter instanceof And) {
      And and = (And)filter;
      solrQuery.addFilterQuery(fq(and.left()));
      solrQuery.addFilterQuery(fq(and.right()));
    } else if (filter instanceof Or) {
      Or f = (Or)filter;
      solrQuery.addFilterQuery("(" + fq(f.left())+" OR " + fq(f.right())+")");
    } else if (filter instanceof Not) {
      Not not = (Not)filter;
      solrQuery.addFilterQuery("NOT "+fq(not.child()));
    } else {
      solrQuery.addFilterQuery(fq(filter));
    }
  }

  protected String attributeToFieldName(String attribute) {
      Map<String,StructField> fieldMap = new HashMap<String,StructField>();
      for (StructField f : solrRDD.getSchema().fields()) fieldMap.put(f.name(), f);
      StructField field = fieldMap.get(attribute.replaceAll("`",""));
      if (field != null) {
          Metadata meta = field.metadata();
          String fieldName = meta.contains("name") ? meta.getString("name") : field.name();
          return fieldName;
      }
      return attribute;
  }

  protected String fq(Filter f) {
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
    return negate+attributeToFieldName(attr)+":"+crit;
  }

  public void insert(final DataFrame df, boolean overwrite) {

    JavaRDD<SolrInputDocument> docs = null;
    if (!preserveSchema) {
      docs = df.javaRDD().map(new Function<Row, SolrInputDocument>() {
        public SolrInputDocument call(Row row) throws Exception {
          StructType schema = row.schema();
          SolrInputDocument doc = new SolrInputDocument();
          for (StructField f : schema.fields()) {
            String fname = f.name();
            if (fname.equals("_version_"))
              continue;

            int fieldIndex = row.fieldIndex(fname);
            Object val = row.isNullAt(fieldIndex) ? null : row.get(fieldIndex);
            if (val != null) {
              if (val instanceof Collection) {
                Collection c = (Collection) val;
                Iterator i = c.iterator();
                while (i.hasNext())
                  doc.addField(fname, i.next());
              } else if (val instanceof scala.collection.mutable.ArrayBuffer) {
                scala.collection.Iterator iter =
                            ((scala.collection.mutable.ArrayBuffer)val).iterator();
                while (iter.hasNext())
                  doc.addField(fname, iter.next());
              } else if (val instanceof scala.collection.mutable.WrappedArray) {
                scala.collection.Iterator iter =
                            ((scala.collection.mutable.WrappedArray) val).iterator();
                while (iter.hasNext())
                  doc.addField(fname, iter.next());
              } else {
                doc.setField(fname, val);
              }
            }
          }
          return doc;
        }
      });
    }
    else{
      docs = ((SchemaPreservingSolrRDD) solrRDD).convertToSolrDocuments(df, new HashMap<String, Object>());
    }

    SolrSupport.indexDocs(solrRDD.zkHost, solrRDD.collection, 100, docs);
  }

}

