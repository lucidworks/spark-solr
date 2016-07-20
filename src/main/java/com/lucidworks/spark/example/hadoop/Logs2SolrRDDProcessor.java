package com.lucidworks.spark.example.hadoop;

import com.lucidworks.spark.util.SolrSupport;
import com.lucidworks.spark.SparkApp;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.log4j.Logger;
import org.apache.parquet.Closeables;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.input.PortableDataStream;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipInputStream;

import scala.collection.JavaConversions;

public class Logs2SolrRDDProcessor implements SparkApp.RDDProcessor {
  
  public static Logger log = Logger.getLogger(Logs2SolrRDDProcessor.class);

  public String getName() { return "logs2solr"; }

  public Option[] getOptions() {
    return new Option[]{
      OptionBuilder
        .withArgName("PATH")
        .hasArg()
        .isRequired(false)
        .withDescription("HDFS path identifying the directories / files to index")
        .create("hdfsPath")
    };
  }

  public int run(SparkConf conf, CommandLine cli) throws Exception {
    JavaSparkContext jsc = new JavaSparkContext(conf);

    final String zkHost = cli.getOptionValue("zkHost", "localhost:9983");
    final String collection = cli.getOptionValue("collection", "collection1");
    final int batchSize = Integer.parseInt(cli.getOptionValue("batchSize", "1000"));
    jsc.binaryFiles(cli.getOptionValue("hdfsPath")).foreach(
      new VoidFunction<Tuple2<String, PortableDataStream>>() {
        public void call(Tuple2<String, PortableDataStream> t2) throws Exception {
          final SolrClient solrServer = SolrSupport.getCachedCloudClient(zkHost);
          List<SolrInputDocument> batch = new ArrayList<SolrInputDocument>(batchSize);
          String path = t2._1();
          BufferedReader br = null;
          String line = null;
          int lineNum = 0;
          try {
            br = new BufferedReader(new InputStreamReader(openPortableDataStream(t2._2()), "UTF-8"));
            while ((line = br.readLine()) != null) {
              ++lineNum;
              SolrInputDocument doc = new SolrInputDocument();
              doc.setField("id", path + ":" + lineNum);
              doc.setField("path_s", path);
              doc.setField("line_t", line);
              batch.add(doc);
              if (batch.size() >= batchSize)
                SolrSupport.sendBatchToSolr(solrServer, collection, JavaConversions.collectionAsScalaIterable(batch));

              if (lineNum % 10000 == 0)
                log.info("Sent "+lineNum+" docs to Solr from "+path);
            }
            if (!batch.isEmpty())
              SolrSupport.sendBatchToSolr(solrServer, collection, JavaConversions.collectionAsScalaIterable(batch));
          } catch (Exception exc) {
            log.error("Failed to read '" + path + "' due to: " + exc);
          } finally {
            if (br != null) {
              try {
                br.close();
              } catch (Exception ignore) {}
            }
          }
        }

        InputStream openPortableDataStream(PortableDataStream pds) throws Exception {
          InputStream in = null;
          String path = pds.getPath();
          log.info("Opening InputStream to " + path);
          if (path.endsWith(".zip")) {
            ZipInputStream zipIn = new ZipInputStream(pds.open());
            zipIn.getNextEntry();
            in = zipIn;
          } else if (path.endsWith(".bz2")) {
            in = new BZip2CompressorInputStream(pds.open());
          } else if (path.endsWith(".gz")) {
            in = new GzipCompressorInputStream(pds.open());
          }
          return in;
      }
    });
    return 0;
  }
}
