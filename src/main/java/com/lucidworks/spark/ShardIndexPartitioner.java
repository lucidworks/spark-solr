package com.lucidworks.spark;

import com.lucidworks.spark.util.SolrSupport;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.*;
import org.apache.spark.Partitioner;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Partition using SolrCloud's sharding scheme.
 */
public class ShardIndexPartitioner extends Partitioner implements Serializable {

  protected String zkHost;
  protected String collection;
  protected String idField;

  protected transient CloudSolrClient cloudSolrServer = null;
  protected transient DocCollection docCollection = null;
  protected transient Map<String,Integer> shardIndexCache = null;

  public ShardIndexPartitioner(String zkHost, String collection) {
    this(zkHost, collection, "id");
  }

  public ShardIndexPartitioner(String zkHost, String collection, String idField) {
    this.zkHost = zkHost;
    this.collection = collection;
    this.idField = idField;
  }

  @Override
  public int numPartitions() {
    return getDocCollection().getActiveSlices().size();
  }

  public String getShardId(SolrInputDocument doc) {
    return getShardId((String)doc.getFieldValue(idField));
  }

  public String getShardId(String docId) {
    DocCollection dc = getDocCollection();
    Slice slice = dc.getRouter().getTargetSlice(docId, null, null, null, dc);
    return slice.getName();
  }

  @Override
  public int getPartition(Object o) {

    Object docId = null;
    if (o instanceof SolrInputDocument) {
      SolrInputDocument doc = (SolrInputDocument)o;
      docId = doc.getFieldValue(idField);
      if (docId == null)
        throw new IllegalArgumentException("SolrInputDocument must contain a non-null value for "+idField);
    } else {
      docId = o;
    }

    if (!(docId instanceof String))
      throw new IllegalArgumentException("Only String document IDs are supported by this Partitioner!");

    DocCollection dc = getDocCollection();
    Slice slice = dc.getRouter().getTargetSlice((String)docId, null, null, null, dc);
    return getShardIndex(slice.getName(), dc);
  }
  
  protected final synchronized int getShardIndex(String shardId, DocCollection dc) {
    if (shardIndexCache == null)
      shardIndexCache = new HashMap<String,Integer>(20);

    Integer idx = shardIndexCache.get(shardId);
    if (idx != null)
      return idx.intValue(); // meh auto-boxing

    int s = 0;
    for (Slice slice : dc.getSlices()) {
      if (shardId.equals(slice.getName())) {
        shardIndexCache.put(shardId, new Integer(s));
        return s;
      }
      ++s;
    }
    throw new IllegalStateException("Cannot find index of shard '"+shardId+"' in collection: "+collection);
  }

  protected final synchronized DocCollection getDocCollection() {
    if (docCollection == null) {
      ZkStateReader zkStateReader = getCloudSolrServer().getZkStateReader();
      docCollection = zkStateReader.getClusterState().getCollection(collection);

      // do basic checks once
      DocRouter docRouter = docCollection.getRouter();
      if (docRouter instanceof ImplicitDocRouter)
        throw new IllegalStateException("Implicit document routing not supported by this Partitioner!");
      Collection<Slice> shards = getDocCollection().getSlices();
      if (shards == null || shards.size() == 0)
        throw new IllegalStateException("Collection '"+collection+"' does not have any shards!");
    }
    return docCollection;
  }

  protected final synchronized CloudSolrClient getCloudSolrServer() {
    if (cloudSolrServer == null)
      cloudSolrServer = SolrSupport.getCachedCloudClient(zkHost);
    return cloudSolrServer;
  }
}
