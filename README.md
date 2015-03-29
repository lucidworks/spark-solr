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

To run the Twitter examples, you'll need to configure your Twitter API credentials in:

```
src/main/resources/twitter4j.properties
```

and then re-build the package (mvn package).

Now, let's populate a SolrCloud index with tweets:

```
./spark-submit --master local[2] --class com.lucidworks.spark.SparkApp \
  <PROJECT_PATH>/target/spark-solr-5x-1.0-SNAPSHOT.jar \
  twitter-to-solr -zkHost localhost:9983 -collection collection1
```

After populating your collection with tweets, you can see an example of how to transform the results from a Solr
query into a Spark RDD:

```
./spark-submit --master local[2] --class com.lucidworks.spark.SparkApp \
  <PROJECT_PATH>/target/spark-solr-5x-1.0-SNAPSHOT.jar \
  query-solr -zkHost=localhost:9983 -collection=collection1 -query="*:*"
```

Reading data from Solr as a Spark RDD
========

The `com.lucidworks.spark.SolrRDD` class transforms the results of a Solr query into a Spark RDD.

```
SolrRDD solrRDD = new SolrRDD(zkHost, collection);
JavaRDD<SolrDocument> solrJavaRDD = solrRDD.query(jsc, solrQuery, useDeepPagingCursor);
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

Also, notice in the example above, the third argument to the solrRDD.query() function indicates whether to use
deep-paging cursors, which allows you to process very large result sets across a Spark cluster.

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
