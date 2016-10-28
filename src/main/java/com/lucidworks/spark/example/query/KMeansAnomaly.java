package com.lucidworks.spark.example.query;

import com.lucidworks.spark.SparkApp;
import com.lucidworks.spark.rdd.SolrJavaRDD;
import com.lucidworks.spark.util.ConfigurationConstants;
import com.lucidworks.spark.util.PivotField;
import com.lucidworks.spark.util.SolrQuerySupport;
import com.lucidworks.spark.util.SolrSupport;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * Use K-means to do basic anomaly detection by examining user sessions
 */
public class KMeansAnomaly implements SparkApp.RDDProcessor {

  private static final String UID_FIELD = "clientip_s";
  private static final String TS_FIELD = "timestamp_tdt";

  public String getName() {
    return "kmeans-anomaly";
  }

  public Option[] getOptions() {
    return new Option[]{
      OptionBuilder
              .withArgName("QUERY")
              .hasArg()
              .isRequired(false)
              .withDescription("URL encoded Solr query to send to Solr")
              .create("query"),
        OptionBuilder
            .withArgName("SQL")
            .hasArg()
            .isRequired(false)
            .withDescription("File containing a SQL query to execute to generate the aggregated data.")
            .create("aggregationSQL")
    };
  }

  public int run(SparkConf conf, CommandLine cli) throws Exception {

    String getLogsQuery =
        "+clientip_s:[* TO *] +timestamp_tdt:[* TO *] +bytes_s:[* TO *] +verb_s:[* TO *] +response_s:[* TO *]";

    String zkHost = cli.getOptionValue("zkHost", "localhost:9983");
    String collection = cli.getOptionValue("collection", "apache_logs");
    String queryStr = cli.getOptionValue("query", getLogsQuery);

    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

    Map<String, String> options = new HashMap<String, String>();
    options.put("zkhost", zkHost);
    options.put("collection", collection);
    options.put("query", queryStr);
    options.put(ConfigurationConstants.SOLR_SPLIT_FIELD_PARAM(), "_version_");
    options.put(ConfigurationConstants.SOLR_SPLITS_PER_SHARD_PARAM(), "4");
    options.put(ConfigurationConstants.SOLR_FIELD_PARAM(), "id,_version_,"+UID_FIELD+","+TS_FIELD+",bytes_s,response_s,verb_s");

    // Use the Solr DataSource to load rows from a Solr collection using a query
    // highlights include:
    //   - parallelization of reads from each shard in Solr
    //   - more parallelization by splitting each shard into ranges
    //   - results are streamed back from Solr using deep-paging and streaming response
    Dataset logEvents = sparkSession.read().format("solr").options(options).load();

    // Convert rows loaded from Solr into rows with pivot fields expanded, i.e.
    //
    // verb_s=GET is expanded to http_method_get=1, http_method_post=0, ...
    //
    // TODO: we could push pivot transforms into the SolrRelation impl. and then pivoting can just be an option
    // TODO: supposedly you can do this with aggregateByKey using Spark, but this works for now ...
    PivotField[] pivotFields = new PivotField[] {
        new PivotField("verb_s", "http_method_"),
        new PivotField("response_s", "http_code_")
    };

    // this "view" has the verb_s and response_s fields expanded into aggregatable
    SolrJavaRDD solrRDD = SolrJavaRDD.get(zkHost, collection, jsc.sc());
    Dataset solrDataWithPivots = SolrQuerySupport.withPivotFields(logEvents, pivotFields, solrRDD.rdd(), false);
    // register this DataFrame so we can execute a SQL query against it for doing sessionization using lag window func
    solrDataWithPivots.registerTempTable("logs");

    // used in SQL below to convert a timestamp into millis since the epoch
    sparkSession.udf().register("ts2ms", new UDF1<Timestamp, Long>() {
      public Long call(final Timestamp ts) throws Exception {
        return (ts != null) ? ts.getTime() : 0L;
      }
    }, DataTypes.LongType);

    // sessionize using SQL and a lag window function
    long maxGapMs = 30 * 1000; // session gap of 30 seconds
    String lagWindowSpec = "(PARTITION BY "+UID_FIELD+" ORDER BY "+TS_FIELD+")";
    String lagSql = "SELECT *, sum(IF(diff_ms > "+maxGapMs+", 1, 0)) OVER "+lagWindowSpec+
        " session_id FROM (SELECT *, ts2ms("+TS_FIELD+") - lag(ts2ms("+TS_FIELD+")) OVER "+lagWindowSpec+" as diff_ms FROM logs) tmp";

    Dataset userSessions = sparkSession.sql(lagSql);
    //userSessions.printSchema();
    //userSessions.cache(); // much work done to get here ... cache it for better perf when executing queries
    userSessions.createOrReplaceTempView("sessions");

    // used to convert bytes_s into an int (or zero) if null
    sparkSession.udf().register("asInt", new UDF1<String, Integer>() {
      public Integer call(final String str) throws Exception {
        return (str != null) ? new Integer(str) : 0;
      }
    }, DataTypes.IntegerType);

    // execute some aggregation query
    // TODO: ugh - having to use dynamic fields here is crappy ... be better to use the schema api to define
    // the fields we need on-the-fly (see APOLLO-4127)
    Dataset sessionsAgg = sparkSession.sql(
        "SELECT   concat_ws('||', clientip_s,session_id) as id, " +
        "         first(clientip_s) as clientip_s, " +
        "         min(timestamp_tdt) as session_start_tdt, " +
        "         max(timestamp_tdt) as session_end_tdt, " +
        "         (ts2ms(max(timestamp_tdt)) - ts2ms(min(timestamp_tdt))) as session_len_ms_l, " +
        "         sum(asInt(bytes_s)) as total_bytes_l, " +
        "         count(*) as total_requests_l, " +
        "         sum(http_method_get) as num_get_l, " +
        "         sum(http_method_head) as num_head_l, " +
        "         sum(http_method_post) as num_post_l" +
        "    FROM sessions " +
        "GROUP BY clientip_s,session_id");

    sessionsAgg.cache();
    sessionsAgg.printSchema();

    // save the aggregated data back to Solr
    String aggCollection = collection + "_aggr";
    options = new HashMap<String, String>();
    options.put("zkhost", zkHost);
    options.put("collection", aggCollection);
    sessionsAgg.write().format("solr").options(options).mode(SaveMode.Overwrite).save();

    SolrSupport.getCachedCloudClient(zkHost).commit(aggCollection);

    // k-means clustering for finding anomalies
    
    JavaRDD<Vector> vectors = sessionsAgg.javaRDD().map(new Function<Row, Vector>() {
      @Override
      public Vector call(Row row) throws Exception {
        // todo: select whichever fields from the sessionsAgg that should be included in the vectors
        long sessionLenMs = row.getLong(row.fieldIndex("session_len_ms_l"));
        long totalBytes = row.getLong(row.fieldIndex("total_bytes_l"));
        long numGets = row.getLong(row.fieldIndex("num_get_l"));
        long numHeads = row.getLong(row.fieldIndex("num_head_l"));
        return Vectors.dense(new double[]{sessionLenMs, totalBytes, numGets, numHeads});
      }
    });
    vectors.cache();

    // Cluster the data using KMeans (make k and iters configurable)
    int k = 8;
    int iterations = 20;
    KMeansModel clusters = KMeans.train(vectors.rdd(), k, iterations);

    double WSSSE = clusters.computeCost(vectors.rdd());
    System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

    // TODO: interpret the KMeansModel to find anomalies

    jsc.stop();

    return 0;
  }
}
