package com.lucidworks.spark;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.*;

import org.apache.commons.httpclient.ConnectTimeoutException;
import org.apache.commons.httpclient.NoHttpResponseException;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaDStream;

import org.apache.spark.api.java.function.*;

public class SolrSupport {

  public static Logger log = Logger.getLogger(SolrSupport.class);

  private static Map<String,SolrServer> solrServers = new HashMap<String, SolrServer>();

  public static SolrServer getSolrServer(String key) {
    SolrServer solr = null;
    synchronized (solrServers) {
      solr = solrServers.get(key);
      if (solr == null) {
        solr = new CloudSolrServer(key);
        solrServers.put(key, solr);
      }
    }
    return solr;
  }

  /**
   * Helper function for indexing a DStream of SolrInputDocuments to Solr.
   */
  public static void indexDStreamOfDocs(final String zkHost,
                                        final String collection,
                                        final int batchSize,
                                        JavaDStream<SolrInputDocument> docs)
  {
    docs.foreachRDD(
      new Function<JavaRDD<SolrInputDocument>, Void>() {
        public Void call(JavaRDD<SolrInputDocument> solrInputDocumentJavaRDD) throws Exception {
          solrInputDocumentJavaRDD.foreachPartition(
            new VoidFunction<Iterator<SolrInputDocument>>() {
              public void call(Iterator<SolrInputDocument> solrInputDocumentIterator) throws Exception {
                final SolrServer solrServer = getSolrServer(zkHost);
                List<SolrInputDocument> batch = new ArrayList<SolrInputDocument>();
                while (solrInputDocumentIterator.hasNext()) {
                  batch.add(solrInputDocumentIterator.next());
                  if (batch.size() >= batchSize)
                    sendBatchToSolr(solrServer, collection, batch);
                }
                if (!batch.isEmpty())
                  sendBatchToSolr(solrServer, collection, batch);
              }
            }
          );
          return null;
        }
      }
    );
  }

  public static void sendBatchToSolr(SolrServer solrServer, String collection, List<SolrInputDocument> batch) {
    UpdateRequest req = new UpdateRequest();
    req.setParam("collection", collection);
    req.add(batch);
    try {
      solrServer.request(req);
    } catch (Exception e) {
      if (shouldRetry(e)) {
        log.error("Send batch to collection "+collection+" failed due to "+e+"; will retry ...");
        try {
          Thread.sleep(2000);
        } catch (InterruptedException ie) {
          Thread.interrupted();
        }

        try {
          solrServer.request(req);
        } catch (Exception e1) {
          log.error("Retry send batch to collection "+collection+" failed due to: "+e1, e1);
          if (e1 instanceof RuntimeException) {
            throw (RuntimeException)e1;
          } else {
            throw new RuntimeException(e1);
          }
        }
      } else {
        log.error("Send batch to collection "+collection+" failed due to: "+e, e);
        if (e instanceof RuntimeException) {
          throw (RuntimeException)e;
        } else {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private static boolean shouldRetry(Exception exc) {
    Throwable rootCause = SolrException.getRootCause(exc);
    return (rootCause instanceof ConnectException ||
            rootCause instanceof ConnectTimeoutException ||
            rootCause instanceof NoHttpResponseException ||
            rootCause instanceof SocketException);

  }

  /**
   * Uses reflection to map bean public fields and getters to dynamic fields in Solr.
   */
  public static SolrInputDocument autoMapToSolrInputDoc(final String docId, final Object obj, final Map<String,String> dynamicFieldOverrides) {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", docId);
    if (obj == null)
      return doc;

    Class objClass = obj.getClass();
    Set<String> fields = new HashSet<String>();
    Field[] publicFields = obj.getClass().getFields();
    if (publicFields != null) {
      for (Field f : publicFields) {
        Object value = null;
        try {
          value = f.get(obj);
        } catch (IllegalAccessException e) {}

        if (value != null) {
          String fieldName = f.getName();
          fields.add(fieldName);
          addField(doc, fieldName, value, f.getType(),
            (dynamicFieldOverrides != null) ? dynamicFieldOverrides.get(fieldName) : null);
        }
      }
    }

    PropertyDescriptor[] props = null;
    try {
      BeanInfo info = Introspector.getBeanInfo(objClass);
      props = info.getPropertyDescriptors();
    } catch (IntrospectionException e) {
      log.warn("Can't get BeanInfo for class: "+objClass);
    }

    if (props != null) {
      for (PropertyDescriptor pd : props) {
        String propName = pd.getName();
        if ("class".equals(propName) || fields.contains(propName))
          continue;

        Method readMethod = pd.getReadMethod();
        if (readMethod != null) {
          Object value = null;
          try {
            value = readMethod.invoke(obj);
          } catch (Exception e) {
            log.debug("Failed to invoke read method for property '" + pd.getName() +
              "' on object of type '" + objClass.getName()+"' due to: "+e);
          }

          if (value != null) {
            fields.add(propName);
            addField(doc, propName, value, pd.getPropertyType(),
              (dynamicFieldOverrides != null) ? dynamicFieldOverrides.get(propName) : null);
          }
        }
      }
    }

    return doc;
  }

  private static void addField(SolrInputDocument doc, String fieldName, Object value, Class type, String dynamicFieldSuffix) {
    if (type.isArray())
      return; // TODO: Array types not supported yet ...

    if (dynamicFieldSuffix == null)
      dynamicFieldSuffix = getDefaultDynamicFieldMapping(type);

    if ("_s".equals(dynamicFieldSuffix) && !(value instanceof String)) {
      doc.addField(fieldName + dynamicFieldSuffix, value.toString());
    } else {
      doc.addField(fieldName + dynamicFieldSuffix, value);
    }
  }

  protected static String getDefaultDynamicFieldMapping(Class clazz) {
    if (Long.class.equals(clazz) || long.class.equals(clazz))
      return "_l";
    else if (Integer.class.equals(clazz) || int.class.equals(clazz))
      return "_i";
    else if (Double.class.equals(clazz) || double.class.equals(clazz))
      return "_d";
    else if (Float.class.equals(clazz) || float.class.equals(clazz))
      return "_f";
    else if (Boolean.class.equals(clazz) || boolean.class.equals(clazz))
      return "_b";

    return "_s"; // default is string
  }
}
