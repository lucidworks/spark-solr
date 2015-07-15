Lucidworks Spark/Solr Integration
========

Tools for reading data from Solr as a Spark RDD and indexing objects from Spark into Solr using SolrJ.

Features
========

* Send objects from a Spark Streaming application into Solr
* Read the results from a Solr query as a Spark RDD
* Read a large results set from Solr using distributed deep paging as a Spark RDD

Example Applications
========

First, build the Jar for this project:

`mvn clean package`

This will build 2 jars in the `/target` directory: spark-solr-1.0-SNAPSHOT.jar and spark-solr-1.0-SNAPSHOT-shaded.jar. 
The first is what you'd want to use if you were using spark-solr in your own project. The second is what you'd use to 
submit one of the included example apps to Spark.

Now, let's populate a SolrCloud index with tweets (be sure to update the command shown below with your Twitter API credentials):

```
$SPARK_HOME/bin/spark-submit --master local[2] \
  --conf "spark.executor.extraJavaOptions=-Dtwitter4j.oauth.consumerKey=? -Dtwitter4j.oauth.consumerSecret=? -Dtwitter4j.oauth.accessToken=? -Dtwitter4j.oauth.accessTokenSecret=?" \
  --class com.lucidworks.spark.SparkApp \
  ./target/spark-solr-1.0-SNAPSHOT-shaded.jar \
  twitter-to-solr -zkHost localhost:9983 -collection gettingstarted
```

After populating your collection with tweets, you can see an example of how to transform the results from a Solr
query into a Spark RDD:

```
./spark-submit --master local[2] --class com.lucidworks.spark.SparkApp \
  <PROJECT_PATH>/target/spark-solr-1.0-SNAPSHOT-shaded.jar \
  query-solr -zkHost=localhost:9983 -collection=gettingstarted -query="*:*"
```

Reading data from Solr as a Spark RDD
========

The `com.lucidworks.spark.SolrRDD` class transforms the results of a Solr query into a Spark RDD.

```
SolrRDD solrRDD = new SolrRDD(zkHost, collection);
JavaRDD<SolrDocument> solrJavaRDD = solrRDD.queryShards(jsc, solrQuery);
```

Once you've converted the results in an RDD, you can use the Spark API to perform analytics against the data from Solr.
For instance, the following code extracts terms from the tweet_s field of each document in the results:

```
JavaRDD<String> words = solrJavaRDD.flatMap(new FlatMapFunction<SolrDocument, String>() {
  public Iterable<String> call(SolrDocument doc) {
    Object tweet_s = doc.get("tweet_s");
    String str = tweet_s != null ? tweet_s.toString() : "";
    str = str.toLowerCase().replaceAll("[.,!?\n]", " ");
    return Arrays.asList(str.split(" "));
  }
});
```

Writing data to Solr from Spark Streaming
========

The `com.lucidworks.spark.SolrSupport` class provides static helper functions for send data to Solr from a Spark
 streaming application. The TwitterToSolrStreamProcessor class provides a good example of how to use the SolrSupport
 API. For sending documents directly to Solr, you need to build-up a `SolrInputDocument` in your
 Spark streaming application code. 

```
    String zkHost = cli.getOptionValue("zkHost", "localhost:9983");
    String collection = cli.getOptionValue("collection", "collection1");
    int batchSize = Integer.parseInt(cli.getOptionValue("batchSize", "10"));
    SolrSupport.indexDStreamOfDocs(zkHost, collection, batchSize, docs);
```

Developing a Spark Application
========

The `com.lucidworks.spark.SparkApp` provides a simple framework for implementing Spark applications in Java. The
class saves you from having to duplicate boilerplate code needed to run a Spark application, giving you more time to
focus on the business logic of your application. To leverage this framework, you need to develop a concrete class
that either implements RDDProcessor or extends StreamProcessor depending on the type of application you're developing.

RDDProcessor
-------------

Implement the `com.lucidworks.spark.SparkApp$RDDProcessor` interface for building a Spark application that operates
 on a JavaRDD, such as one pulled from a Solr query (see: SolrQueryProcessor as an example)

StreamProcessor
-------------

Extend the `com.lucidworks.spark.SparkApp$StreamProcessor` abstract class to build a Spark streaming application.
See com.lucidworks.spark.example.streaming.oneusagov.OneUsaGovStreamProcessor or
com.lucidworks.spark.example.streaming.TwitterToSolrStreamProcessor for examples of how to write a StreamProcessor.

Working at the Spark Shell
========

When launching the Spark shell (Scala mode), you need to add this project's JAR file to the environment using ADD_JARS:

```
ADD_JARS=$PROJECT_HOME/target/spark-solr-1.0-SNAPSHOT-shaded.jar bin/spark-shell
```

You should see a message like this from Spark during shell initialization:

```
15/05/27 10:07:53 INFO SparkContext: Added JAR file:/spark-solr/target/spark-solr-1.0-SNAPSHOT-shaded.jar at http://192.168.1.3:57936/jars/spark-solr-1.0-SNAPSHOT-shaded.jar with timestamp 1432742873044
```

To use SolrRDD to access data in Solr, you need to import the class and create an instance of SolrRDD by passing in the ZooKeeper connection string and collection name:

```
import com.lucidworks.spark.SolrRDD;
var solrRDD = new SolrRDD("localhost:9983","gettingstarted");
```

To get query results as an RDD, use the query method:

```
var tweets = solrRDD.query(sc,"*:*");
var count = tweets.count();
```

Behind the scenes, Spark will query each shard of the gettingstarted collection and then count the results.

To get query results as a temp table (DataFrame), you can use the asTempTable method:

```
var tweets = solrRDD.asTempTable(sqlContext, "*:*", "tweets");
sqlContext.sql("SELECT COUNT(type_s) FROM tweets WHERE type_s='echo'").show();
```

Notice that SolrRDD figured out the schema for you by retrieving metadata from Solr using the Schema API. In other words, the query does not specify the `type_s` field but it is still available as part of the temp table definition because field metadata for your Solr query are retrieved dynamically if not supplied.

To see the schema created from Solr metadata, simply do: `tweets.printSchema();`

Authenticating with Kerberized Solr
========

For background on Solr security, see: https://cwiki.apache.org/confluence/display/solr/Security

The SparkApp framework allows you to pass the path to a JAAS authentication configuration file using the -solrJaasAuthConfig option.

For example, if you need to authenticate using the "solr" Kerberos principal, you need to create a JAAS config file that sets the location of your Kerberos keytab file, such as:

```
Client {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  keyTab="/opt/lucidworks-hdpsearch/job/solr.keytab"
  storeKey=true
  useTicketCache=true
  debug=true
  principal="solr";
};
```

To use this configuration to authenticate to Solr, you simply need to pass the path using the -solrJaasAuthConfig option, such as:

```
spark-submit --master yarn-server \
  --class com.lucidworks.spark.SparkApp \
  $SPARK_SOLR_PROJECT/target/lucidworks-spark-rdd-2.0.3.jar \
  hdfs-to-solr -zkHost $ZK -collection spark-hdfs \
  -hdfsPath /user/spark/testdata/syn_sample_50k \
  -solrJaasAuthConfig=/opt/lucidworks-hdpsearch/job/jaas-client.conf
```

