package com.lw.spark;

import com.lw.spark.rdd.SolrRDD;
import com.lw.spark.util.SolrSupport;
import com.lw.spark.util.ScalaUtil;
import com.lw.spark.util.SolrSchemaUtil;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.*;

public class SchemaPreservingSolrRDD extends SolrRDD {

  public static Logger log = Logger.getLogger(SchemaPreservingSolrRDD.class);

  public SchemaPreservingSolrRDD(String collection) throws Exception {
    super("localhost:9983", collection); // assume local embedded ZK if not supplied
  }

  public SchemaPreservingSolrRDD(String zkHost, String collection) throws Exception {
    super(zkHost, collection, new scala.collection.immutable.HashMap<String, String>());
  }

  public SchemaPreservingSolrRDD(String zkHost, String collection, scala.collection.immutable.Map<String, String> config) throws Exception {
    super(zkHost, collection, config);
  }

  @Override
  public StructType getQuerySchema(SolrQuery query) throws Exception {
    query.addFilterQuery("__lwcategory_s:schema AND __lwroot_s:root");
    JavaRDD<SolrDocument> rdd1 = queryShards(sc, query);
    return SolrSchemaUtil.readSchema(rdd1.collect().get(0), SolrSupport.getSolrClient(zkHost), collection);
  }



  @Override
  public JavaRDD<SolrDocument> queryShards(JavaSparkContext jsc, final SolrQuery origQuery, final String splitFieldName, final int splitsPerShard) throws SolrServerException {
    origQuery.addFilterQuery("__lwcategory_s:data");
    return super.queryShards(jsc, origQuery, splitFieldName, splitsPerShard);
  }

  @Override
  public JavaRDD<Row> toRows(final StructType schema, JavaRDD<SolrDocument> docs) {
    JavaRDD<SolrDocument> rootdocs = docs.filter(new Function<SolrDocument, Boolean>() {
        @Override
        public Boolean call(SolrDocument solrDocument) throws Exception {
            if (solrDocument.containsKey("__lwroot_s")) {
                return true;
            }
            return false;
        }
    });
    JavaPairRDD<String, SolrDocument> childdocs = docs.filter(new Function<SolrDocument, Boolean>() {
      @Override
      public Boolean call(SolrDocument solrDocument) throws Exception {
        if (solrDocument.containsKey("__lwroot_s")) {
          return false;
        }
        return true;
      }
    }).mapToPair(new PairFunction<SolrDocument, String, SolrDocument>() {
      @Override
      public scala.Tuple2<String, SolrDocument> call(SolrDocument solrDocument){
        return new scala.Tuple2<String, SolrDocument>(solrDocument.get("id").toString(), solrDocument);
      }
    });
    final Map<String, SolrDocument> childMap = childdocs.collectAsMap();
    JavaRDD<Row> rows = rootdocs.map(new Function<SolrDocument, Row>() {
      @Override
      public Row call(SolrDocument solrDocument) throws Exception {
        Row ret =  readData(solrDocument,  schema, collection, childMap);
        return ret;
      }
    });
    return rows;
  }

  public Row readData(SolrDocument doc, StructType st, String collection, Map<String, SolrDocument> childMap) throws IOException, SolrServerException {
    ArrayList<Object> str = new ArrayList<Object>();
    return recurseDataRead(doc , str, st, collection, childMap);
  }

  public Row recurseDataRead(SolrDocument doc, ArrayList<Object> x, StructType st, String collection, Map<String, SolrDocument> childMap) {
    Boolean recurse = true;
    Map<String, Object> x1 = doc.getFieldValueMap();
    String[] x3 = st.fieldNames();
    Object[] x2 = x1.keySet().toArray();
    for (int i = 0; i < x2.length; i++) {
      String x2Key = x2[i].toString();
      if (x2Key.startsWith("links")) {
        final String id = doc.get(x2Key).toString();
        if (id != null) {
          SolrDocument childDoc = childMap.get(id);
          if (childDoc == null){
            recurse = false;
          }
          if (recurse) {
            //l = l + 1;
            ArrayList<Object> str1 = new ArrayList<Object>();
            x.add(recurseDataRead(childDoc, str1, (StructType) st.fields()[x.size()].dataType(), collection, childMap));
          }
        }
      }
      if (x3.length > x.size() && x1.get(x3[x.size()]+"_s") == null && !st.fields()[x.size()].dataType().typeName().equals("struct")){
        x.add(null);
      }
      if ((x2Key.substring(x2Key.length() - 2, x2Key.length()).equals("_s") && !x2Key.startsWith("__lw") && !x2Key.startsWith("links"))) {
        String type = ScalaUtil.getFieldTypeMapping(st,x2Key.substring(0, x2Key.length() - 2));
        if (!type.equals("")) {
          String x2Val = x1.get(x2[i]).toString();
          if (type.equals("integer")) {
            x.add(ScalaUtil.convertToInteger(x2Val));
          } else if (type.equals("double")) {
            x.add(ScalaUtil.convertToDouble(x2Val));
          } else if (type.equals("float")) {
            x.add(ScalaUtil.convertToFloat(x2Val));
          } else if (type.equals("short")) {
            x.add(ScalaUtil.convertToShort(x2Val));
          } else if (type.equals("long")) {
            x.add(ScalaUtil.convertToLong(x2Val));
          } else if (type.equals("decimal")) {
            x.add(ScalaUtil.convertToDecimal(x2Val));
          } else if (type.equals("boolean")) {
            x.add(ScalaUtil.convertToBoolean(x2Val));
          } else if (type.equals("timestamp")) {
            x.add(ScalaUtil.convertToTimestamp(x2Val));
          } else if (type.equals("date")) {
            x.add(ScalaUtil.convertToDate(x2Val));
          } else if (type.equals("vector")) {
            x.add(ScalaUtil.convertToVector(x2Val));
          } else if (type.equals("matrix")) {
            x.add(ScalaUtil.convertToMatrix(x2Val));
          } else if (type.contains(":")) {
            //List<Object> debug = Arrays.asList(getArrayFromString(type, x1.get(x2[i]).toString(), 0, new ArrayList<Object[]>()));
            x.add(ScalaUtil.getArrayFromString(type, x2Val, 0, new ArrayList<Object[]>()));
          } else {
            x.add(x1.get(x2[i]));
          }
        }
        else {
          x.add(x1.get(x2[i]));
        }
      }
      if (x3.length > x.size() && x1.get(x3[x.size()]+"_s") == null && !st.fields()[x.size()].dataType().typeName().equals("struct")){
        x.add(null);
      }
    }
    if (x.size()>0) {
      Object[] array = new Object[x.size()];
      x.toArray(array);
      return RowFactory.create(array);
    }
    return null;
  }

  public JavaRDD<SolrInputDocument> convertToSolrDocuments(DataFrame df, final HashMap<String,Object> uniqueIdentifier) {

    SolrInputDocument s = new SolrInputDocument();
    final StructType styp = df.schema();
    int level = 0;
    Iterator it = uniqueIdentifier.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry pair = (Map.Entry)it.next();
      s.addField(pair.getKey().toString(), pair.getValue());
    }
    if (!s.containsKey("id")){
      String id = UUID.randomUUID().toString();
      s.addField("id",id);
    }
    s.addField("__lwroot_s", "root");
    s.addField("__lwcategory_s", "schema");
    SolrSchemaUtil.recurseWriteSchema(styp, s, level);
    ArrayList<SolrInputDocument> a = new ArrayList<SolrInputDocument>();
    a.add(s);
    JavaRDD<SolrInputDocument> rdd1 = sc.parallelize(a);
    JavaRDD<SolrInputDocument> a1 = df.javaRDD().map(new Function<Row, SolrInputDocument>() {
      public SolrInputDocument call(Row r) throws Exception {
        SolrInputDocument solrDocument = new SolrInputDocument();
        Iterator it = uniqueIdentifier.entrySet().iterator();
        while (it.hasNext()) {
          Map.Entry pair = (Map.Entry)it.next();
          solrDocument.addField(pair.getKey().toString(), pair.getValue());
        }
        if (!solrDocument.containsKey("id")) {
          String idData = UUID.randomUUID().toString();
          solrDocument.addField("id", idData);
        }
        solrDocument.addField("__lwroot_s", "root");
        solrDocument.addField("__lwcategory_s", "data");
        List<Row> r1 = new ArrayList<Row>();
        r1.add(r);
        return recurseWriteData(styp, solrDocument, r, 0);
      }
    });
    JavaRDD<SolrInputDocument> finalrdd = rdd1.union(a1);
    return finalrdd;
  }

  public SolrInputDocument recurseWriteData(StructType st,SolrInputDocument solrDocument, Row df, int counter) {
    scala.collection.Iterator x = st.iterator();
    int linkCount = 0;
    while (x.hasNext()) {
      StructField sf = (StructField) x.next();
      if (sf.dataType().typeName().toString().toLowerCase().equals("struct")) {
        linkCount = linkCount + 1;
        SolrInputDocument solrDocument1 = new SolrInputDocument();
        String idChild = UUID.randomUUID().toString();
        solrDocument1.addField("id", idChild);
        solrDocument.addField("links" + linkCount + "_s", idChild);
        solrDocument1.addField("__lwchilddocname_s", sf.name());
        solrDocument1.addField("__lwcategory_s", "data");
        Row df1 = (Row) df.get(counter);
        solrDocument.addChildDocument(recurseWriteData((StructType) sf.dataType(), solrDocument1, df1, 0));
      } else {
          if (df != null) {
            if (sf.dataType().typeName().equals("array")) {
              solrDocument.addField(sf.name() + "_s", ScalaUtil.getArrayToString(sf.dataType(), df.get(counter)));
            } else if (sf.dataType().typeName().equals("matrix")) {
                org.apache.spark.mllib.linalg.Matrix m = (org.apache.spark.mllib.linalg.Matrix) df.get(counter);
                solrDocument.addField(sf.name() + "_s", m.numRows() + ":" + m.numCols() + ":" + Arrays.toString(m.toArray()));
              } else {
                if (df.get(counter) != null) {
                  solrDocument.addField(sf.name() + "_s", df.get(counter).toString());
                } else {
                  solrDocument.addField(sf.name() + "_s", null);
                }
              }
          }

      }
      counter = counter + 1;
    }
    return solrDocument;
  }

}
