package com.lucidworks.spark;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.lucidworks.spark.filter.DocFilterContext;
import com.lucidworks.spark.util.EmbeddedSolrServerFactory;
import org.apache.commons.httpclient.ConnectTimeoutException;
import org.apache.commons.httpclient.NoHttpResponseException;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import scala.Tuple2;

/**
 * A stateless utility class that provides static method for working with the SolrJ API.
 */
public class SolrSupport implements Serializable {

  public static Logger log = Logger.getLogger(SolrSupport.class);

  private static Map<String,CloudSolrServer> solrServers = new HashMap<String, CloudSolrServer>();
  private static Map<String,ConcurrentUpdateSolrServer> leaderServers = new HashMap<String,ConcurrentUpdateSolrServer>();

  public static CloudSolrServer getSolrServer(String key) {
    CloudSolrServer solr = null;
    synchronized (solrServers) {
      solr = solrServers.get(key);
      if (solr == null) {
        solr = new CloudSolrServer(key);
        solr.connect();
        solrServers.put(key, solr);
      }
    }
    return solr;
  }

  public static void streamDocsIntoSolr(final String zkHost,
                                        final String collection,
                                        final String idField,
                                        JavaPairRDD<String,SolrInputDocument> pairs,
                                        final int queueSize,
                                        final int numRunners,
                                        final int pollQueueTime)
    throws Exception
  {
    final ShardPartitioner shardPartitioner = new ShardPartitioner(zkHost, collection);
    pairs.partitionBy(shardPartitioner).foreachPartition(new VoidFunction<Iterator<Tuple2<String, SolrInputDocument>>>() {
      public void call(Iterator<Tuple2<String, SolrInputDocument>> tupleIter) throws Exception {
        ConcurrentUpdateSolrServer cuss = null;
        while (tupleIter.hasNext()) {
          Tuple2<String, SolrInputDocument> next = tupleIter.next();
          if (cuss == null) {
            // once! all docs in this partition have the same leader!
            String shardId = shardPartitioner.getShardId(next._1);
            cuss = getCUSS(zkHost, collection, shardId, queueSize, numRunners, pollQueueTime);
          }
          SolrInputDocument doc = next._2;
          doc.setField("indexed_at_tdt", new Date());
          cuss.add(doc);
        }
      }
    });
  }

  public static ConcurrentUpdateSolrServer getCUSS(final String zkHost,
                                                      final String collection,
                                                      final String shardId,
                                                      final int queueSize,
                                                      final int numRunners,
                                                      final int pollQueueTime) throws Exception
  {
    final String leaderKey = collection + "|" + shardId;
    ConcurrentUpdateSolrServer cuss = null;

    synchronized (leaderServers) {
      cuss = leaderServers.get(leaderKey);
      if (cuss == null) {
        CloudSolrServer solrServer = getSolrServer(zkHost);
        final String leaderUrl = solrServer.getZkStateReader().getLeaderUrl(collection, shardId, 5000);
        cuss = new ConcurrentUpdateSolrServer(leaderUrl, queueSize, numRunners) {
          public void handleError(Throwable ex) {
            log.error("Request to '" + leaderUrl + "' failed due to: " + ex);
            synchronized (leaderServers) {
              leaderServers.remove(leaderKey);
            }
          }
        };
        cuss.setParser(new BinaryResponseParser());
        cuss.setRequestWriter(new BinaryRequestWriter());
        cuss.setPollQueueTime(pollQueueTime);

        leaderServers.put(leaderKey, cuss);
      }
    }
    return cuss;
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
          indexDocs(zkHost, collection, batchSize, solrInputDocumentJavaRDD);
          return null;
        }
      }
    );
  }

  public static void indexDocs(final String zkHost,
                                    final String collection,
                                    final int batchSize,
                                    JavaRDD<SolrInputDocument> docs) {

    docs.foreachPartition(
      new VoidFunction<Iterator<SolrInputDocument>>() {
        public void call(Iterator<SolrInputDocument> solrInputDocumentIterator) throws Exception {
          final SolrServer solrServer = getSolrServer(zkHost);
          List<SolrInputDocument> batch = new ArrayList<SolrInputDocument>();
          Date indexedAt = new Date();
          while (solrInputDocumentIterator.hasNext()) {
            SolrInputDocument inputDoc = solrInputDocumentIterator.next();
            inputDoc.setField("_indexed_at_tdt", indexedAt);
            batch.add(inputDoc);
            if (batch.size() >= batchSize)
              sendBatchToSolr(solrServer, collection, batch);
          }
          if (!batch.isEmpty())
            sendBatchToSolr(solrServer, collection, batch);
        }
      }
    );
  }

  public static void sendBatchToSolr(SolrServer solrServer, String collection, Collection<SolrInputDocument> batch) {
    UpdateRequest req = new UpdateRequest();
    req.setParam("collection", collection);

    if (log.isDebugEnabled())
      log.debug("Sending batch of " + batch.size() + " to collection " + collection);

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
    } finally {
      batch.clear();
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
    return autoMapToSolrInputDoc("id", docId, obj, dynamicFieldOverrides);
  }

  public static SolrInputDocument autoMapToSolrInputDoc(final String idFieldName, final String docId, final Object obj, final Map<String,String> dynamicFieldOverrides) {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField(idFieldName, docId);
    if (obj == null)
      return doc;

    Class objClass = obj.getClass();
    Set<String> fields = new HashSet<String>();
    Field[] publicFields = obj.getClass().getFields();
    if (publicFields != null) {
      for (Field f : publicFields) {
        // only non-static public
        if (Modifier.isStatic(f.getModifiers()) || !Modifier.isPublic(f.getModifiers()))
          continue;

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

    if (dynamicFieldSuffix == null) {
      dynamicFieldSuffix = getDefaultDynamicFieldMapping(type);
      // treat strings with multiple terms as text only if using the default!
      if ("_s".equals(dynamicFieldSuffix)) {
        String str = (String)value;
        if (str.indexOf(" ") != -1)
          dynamicFieldSuffix = "_t";
      }
    }

    if (dynamicFieldSuffix != null) // don't auto-map if we don't have a type
      doc.addField(fieldName + dynamicFieldSuffix, value);
  }

  protected static String getDefaultDynamicFieldMapping(Class clazz) {
    if (String.class.equals(clazz))
      return "_s";
    else if (Long.class.equals(clazz) || long.class.equals(clazz))
      return "_l";
    else if (Integer.class.equals(clazz) || int.class.equals(clazz))
      return "_i";
    else if (Double.class.equals(clazz) || double.class.equals(clazz))
      return "_d";
    else if (Float.class.equals(clazz) || float.class.equals(clazz))
      return "_f";
    else if (Boolean.class.equals(clazz) || boolean.class.equals(clazz))
      return "_b";
    else if (Date.class.equals(clazz))
      return "_tdt";
    return null; // default is don't auto-map
  }

  /**
   * Implements a basic document filtering scheme using Solr query matching against incoming documents.
   */
  public static JavaDStream<SolrInputDocument> filterDocuments(final DocFilterContext filterContext,
                                                               final String zkHost,
                                                               final String collection,
                                                               JavaDStream<SolrInputDocument> docs)
  {
    final AtomicInteger partitionIndex = new AtomicInteger(0);
    final String idFieldName = filterContext.getDocIdFieldName();

    JavaDStream<SolrInputDocument> enriched = docs.mapPartitions(
      new FlatMapFunction<Iterator<SolrInputDocument>, SolrInputDocument>() {
        public Iterable<SolrInputDocument> call(Iterator<SolrInputDocument> solrInputDocumentIterator) throws Exception {
          final long startNano = System.nanoTime();

          final int partitionId = partitionIndex.incrementAndGet();

          final String partitionFq = "docfilterid_i:" + partitionId;
          // TODO: Can this be used concurrently? probably better to have each partition check it out from a pool
          final EmbeddedSolrServer solr =
            EmbeddedSolrServerFactory.singleton.getEmbeddedSolrServer(zkHost, collection);

          // index all docs in this partition, then match queries
          int numDocs = 0;
          final Map<String, SolrInputDocument> inputDocs = new HashMap<String, SolrInputDocument>();
          while (solrInputDocumentIterator.hasNext()) {
            ++numDocs;

            SolrInputDocument doc = solrInputDocumentIterator.next();
            doc.setField("docfilterid_i", partitionId); // for clean-out
            solr.add(doc);

            inputDocs.put((String) doc.getFieldValue(idFieldName), doc);
          }
          solr.commit();

          for (SolrQuery q : filterContext.getQueries()) {
            SolrQuery query = q.getCopy();
            query.setFields(idFieldName);
            query.setRows(inputDocs.size());
            query.addFilterQuery(partitionFq);

            QueryResponse queryResponse = null;
            try {
              queryResponse = solr.query(query);
            } catch (SolrServerException e) {
              throw new RuntimeException(e);
            }

            for (SolrDocument doc : queryResponse.getResults()) {
              String docId = (String) doc.getFirstValue(idFieldName);
              SolrInputDocument inputDoc = inputDocs.get(docId);
              if (inputDoc != null)
                filterContext.onMatch(q, inputDoc);
            }
          }

          solr.deleteByQuery(partitionFq, 100); // no rush on cleaning these docs up ...

          final long durationNano = System.nanoTime() - startNano;

          if (log.isDebugEnabled())
            log.debug("Partition " + partitionId + " took " +
              TimeUnit.MILLISECONDS.convert(durationNano, TimeUnit.NANOSECONDS) + "ms to process " + numDocs + " docs");

          for (SolrInputDocument inputDoc : inputDocs.values()) {
            inputDoc.removeField("docfilterid_i"); // leave no trace of our inner-workings
          }

          return inputDocs.values();
        }
      }
    );

    return enriched;
  }

}
