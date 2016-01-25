package com.lucidworks.spark.util;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
}
