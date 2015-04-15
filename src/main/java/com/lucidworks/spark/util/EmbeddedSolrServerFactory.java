package com.lucidworks.spark.util;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.lucidworks.spark.SolrSupport;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;

import org.apache.log4j.Logger;

import org.apache.commons.io.FileUtils;

/**
 * Supports one or more embedded Solr servers in the same JVM
 */
public class EmbeddedSolrServerFactory implements Serializable {

  private static final Logger log = Logger.getLogger(EmbeddedSolrServerFactory.class);

  public static final EmbeddedSolrServerFactory singleton = new EmbeddedSolrServerFactory();

  private transient Map<String, EmbeddedSolrServer> servers = new HashMap<String, EmbeddedSolrServer>();

  public synchronized EmbeddedSolrServer getEmbeddedSolrServer(String zkHost, String collection) {

    String key = zkHost+"/"+collection;

    EmbeddedSolrServer solr = servers.get(key);
    if (solr == null) {
      try {
        solr = bootstrapEmbeddedSolrServer(zkHost, collection);
      } catch (Exception exc) {
        if (exc instanceof RuntimeException) {
          throw (RuntimeException) exc;
        } else {
          throw new RuntimeException(exc);
        }
      }
      servers.put(key, solr);
    }
    return solr;
  }

  private EmbeddedSolrServer bootstrapEmbeddedSolrServer(String zkHost, String collection) throws Exception {

    CloudSolrClient cloudClient = (CloudSolrClient)SolrSupport.getSolrServer(zkHost);
    cloudClient.connect();

    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    if (!zkStateReader.getClusterState().hasCollection(collection))
      throw new IllegalStateException("Collection '"+collection+"' not found!");

    String configName = zkStateReader.readConfigName(collection);
    if (configName == null)
      throw new IllegalStateException("No configName found for Collection: "+collection);

    File tmpDir = FileUtils.getTempDirectory();
    File solrHomeDir = new File(tmpDir, "solr"+System.currentTimeMillis());

    log.info("Setting up embedded Solr server in local directory: "+solrHomeDir.getAbsolutePath());

    FileUtils.forceMkdir(solrHomeDir);
    
    writeSolrXml(solrHomeDir);

    String coreName = "embedded";

    File instanceDir = new File(solrHomeDir, coreName);
    FileUtils.forceMkdir(instanceDir);

    File confDir = new File(instanceDir, "conf");
    ZkConfigManager zkConfigManager =
      new ZkConfigManager(cloudClient.getZkStateReader().getZkClient());
    zkConfigManager.downloadConfigDir(configName, confDir.toPath());
    if (!confDir.isDirectory())
      throw new IOException("Failed to download /configs/"+configName+" from ZooKeeper!");

    writeSolrConfigXml(confDir);

    log.info(String.format("Attempting to bootstrap EmbeddedSolrServer instance in dir: %s",
      instanceDir.getAbsolutePath()));

    SolrResourceLoader solrResourceLoader =
      new SolrResourceLoader(solrHomeDir.getAbsolutePath());
    CoreContainer coreContainer = new CoreContainer(solrResourceLoader);
    coreContainer.load();

    CoreDescriptor descr =
      new CoreDescriptor(coreContainer, coreName, instanceDir.getAbsolutePath());
    SolrCore core = coreContainer.create(descr);
    return new EmbeddedSolrServer(coreContainer, coreName);
  }

  protected File writeSolrConfigXml(File confDir) throws IOException {
    return writeClasspathResourceToLocalFile("embedded/solrconfig.xml", new File(confDir, "solrconfig.xml"));
  }

  protected File writeSolrXml(File solrHomeDir) throws IOException {
    return writeClasspathResourceToLocalFile("embedded/solr.xml", new File(solrHomeDir, "solr.xml"));
  }
  
  protected File writeClasspathResourceToLocalFile(String resourceId, File destFile) throws IOException {
    InputStreamReader isr = null;
    OutputStreamWriter osw = null;
    int r = 0;
    char[] ach = new char[1024];
    try {
      InputStream in = getClass().getClassLoader().getResourceAsStream(resourceId);
      if (in == null)
        throw new IOException("Resource "+resourceId+" not found on classpath!");

      isr = new InputStreamReader(in, StandardCharsets.UTF_8);
      osw = new OutputStreamWriter(new FileOutputStream(destFile), StandardCharsets.UTF_8);
      while ((r = isr.read(ach)) != -1) osw.write(ach, 0, r);
      osw.flush();
    } finally {
      if (isr != null) {
        try {
          isr.close();
        } catch (Exception ignoreMe){}
      }
      if (osw != null) {
        try {
          osw.close();
        } catch (Exception ignoreMe){}
      }
    }
    return destFile;
  }
}
