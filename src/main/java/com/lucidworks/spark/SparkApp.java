package com.lucidworks.spark;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.lucidworks.spark.port.example.events.EventsimIndexer;
import com.lucidworks.spark.example.hadoop.HdfsToSolrRDDProcessor;
import com.lucidworks.spark.example.hadoop.Logs2SolrRDDProcessor;
import com.lucidworks.spark.example.query.KMeansAnomaly;
import com.lucidworks.spark.example.query.*;
import com.lucidworks.spark.example.streaming.DocumentFilteringStreamProcessor;
import com.lucidworks.spark.example.streaming.TwitterToSolrStreamProcessor;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.impl.Krb5HttpClientBuilder.LOGIN_CONFIG_PROP;

/**
 * Command-line utility for implementing Spark applications; reduces
 * boilerplate code for implementing multiple Spark applications.
 */
public class SparkApp implements Serializable {

  private static final String sparkExecutorExtraJavaOptionsParam = "spark.executor.extraJavaOptions";

  /**
   * Defines the interface to a Spark RDD processing implementation that can be run from this command-line app.
   */
  public interface RDDProcessor extends Serializable {
    String getName();
    Option[] getOptions();
    int run(SparkConf conf, CommandLine cli) throws Exception;
  }

  /**
   * Defines the interface to a stream processing implementation that can be run from this command-line app.
   */
  public static abstract class StreamProcessor implements RDDProcessor {
    
    protected String zkHost;
    protected String collection;
    protected int batchSize;
    
    public int run(SparkConf conf, CommandLine cli) throws Exception {

      this.zkHost = cli.getOptionValue("zkHost", "localhost:9983");
      this.collection = cli.getOptionValue("collection", "collection1");
      this.batchSize = Integer.parseInt(cli.getOptionValue("batchSize", "10"));

      // Create a local StreamingContext with two working thread and batch interval
      int batchIntervalSecs = Integer.parseInt(cli.getOptionValue("batchInterval", "1"));
      JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(batchIntervalSecs * 1000L));

      // distribute the pipeline definition file if provided
      if (cli.hasOption("pipeline")) {
        File pipelineFile = new File(cli.getOptionValue("pipeline"));
        if (!pipelineFile.isFile())
          throw new FileNotFoundException(pipelineFile.getAbsolutePath()+" not found!");
        jssc.sparkContext().addFile(cli.getOptionValue("pipeline"));
      }

      setup(jssc, cli);

      jssc.start();              // Start the computation
      jssc.awaitTermination();   // Wait for the computation to terminate

      return 0;
    }

    public String getCollection() {
      return collection;
    }

    public String getZkHost() {
      return zkHost;
    }

    public int getBatchSize() {
      return batchSize;
    }

    /**
     * Setup for stream processing; the actually processing will be started and managed by the base class.
     */
    public abstract void setup(JavaStreamingContext jssc, CommandLine cli) throws Exception;
  }

  public static Logger log = LoggerFactory.getLogger(SparkApp.class);

  /**
   * Runs a stream processor implementation.
   */
  public static void main(String[] args) throws Exception {
    if (args == null || args.length == 0 || args[0] == null || args[0].trim().length() == 0) {
      System.err.println("Invalid command-line args! Must pass the name of a processor to run.\n"
          + "Supported processors:\n");
      displayProcessorOptions(System.err);
      System.exit(1);
    }

    // Determine the processor to run
    RDDProcessor procImpl;
    ClassLoader myCL = SparkApp.class.getClassLoader();
    try {
      Class<? extends RDDProcessor> clazz = (Class<? extends RDDProcessor>) myCL.loadClass(args[0]);
      procImpl = clazz.newInstance();
    } catch (ClassNotFoundException cnfe) {
      procImpl = newProcessor(args[0].trim().toLowerCase(Locale.ROOT));
    }

    // ensure the processor is serializable
    assertSerializable(procImpl);

    String[] procImplArgs = new String[args.length - 1];
    System.arraycopy(args, 1, procImplArgs, 0, procImplArgs.length);

    // process command-line args to configure this application
    CommandLine cli =
        processCommandLineArgs(
          joinCommonAndProcessorOptions(procImpl.getOptions()), procImplArgs);

    SparkConf sparkConf = new SparkConf().setAppName(procImpl.getName());

    //sparkConf.set("spark.serializer", KryoSerializer.class.getName());
    //sparkConf.set("spark.kryo.registrator", LWKryoRegistrator.class.getName());
    sparkConf.set("spark.task.maxFailures", "10");

    setupSolrAuthenticationProps(cli, sparkConf);

    String masterUrl = cli.getOptionValue("master");
    if (masterUrl != null)
      sparkConf.setMaster(masterUrl);

    // Create a local StreamingContext with two working thread and batch interval
    log.info("Running processor "+procImpl.getName());
    int exitCode = procImpl.run(sparkConf, cli);

    System.exit(exitCode);
  }

  protected static void setupSolrAuthenticationProps(CommandLine cli, SparkConf sparkConf) {
    String solrJaasAuthConfig = cli.getOptionValue("solrJaasAuthConfig");
    if (solrJaasAuthConfig == null || solrJaasAuthConfig.isEmpty())
      return; // no jaas auth config provided

    String solrJaasAppName = cli.getOptionValue("solrJaasAppName", "Client");
    String solrJaasOpts = String.format(Locale.ROOT, "-D%s=%s -Dsolr.kerberos.jaas.appname=%s",
        LOGIN_CONFIG_PROP, solrJaasAuthConfig, solrJaasAppName);
    String sparkExecutorExtraJavaOptions =
      sparkConf.contains(sparkExecutorExtraJavaOptionsParam) ? sparkConf.get(sparkExecutorExtraJavaOptionsParam) : null;
    if (sparkExecutorExtraJavaOptions == null) {
      sparkExecutorExtraJavaOptions = solrJaasOpts;
    } else {
      if (!sparkExecutorExtraJavaOptions.contains(LOGIN_CONFIG_PROP)) {
        sparkExecutorExtraJavaOptions += " " + solrJaasOpts;
      }
    }
    sparkConf.set(sparkExecutorExtraJavaOptionsParam, sparkExecutorExtraJavaOptions);
    System.setProperty(LOGIN_CONFIG_PROP, solrJaasAuthConfig);
    System.setProperty("solr.kerberos.jaas.appname", solrJaasAppName);
    log.info("Added {} to {} for authenticating to Solr", solrJaasOpts, sparkExecutorExtraJavaOptionsParam);
  }

  /**
   * Support options common to all tools.
   */
  public static Option[] getCommonOptions() {
    return new Option[] {
      Option.builder()
              .hasArg()
              .required(false)
              .desc("Batch interval (seconds) for streaming applications; default is 1 second")
              .longOpt("batchInterval")
              .build(),
      Option.builder()
              .hasArg()
              .required(false)
              .desc("The master URL to connect to, such as \"local\" to run locally with one thread, \"local[4]\" to run locally with 4 cores, or \"spark://master:7077\" to run on a Spark standalone cluster.")
              .longOpt("master")
              .build(),
      Option.builder()
              .hasArg()
              .required(false)
              .desc("Address of the Zookeeper ensemble; defaults to: localhost:9983")
              .longOpt("zkHost")
              .build(),
      Option.builder()
              .hasArg()
              .required(false)
              .desc("Name of collection; no default")
              .longOpt("collection")
              .build(),
      Option.builder()
              .hasArg()
              .required(false)
              .desc("Number of docs to queue up on the client before sending to Solr; default is 10")
              .longOpt("batchSize")
              .build(),
      Option.builder()
              .hasArg()
              .required(false)
              .desc("For authenticating to Solr using JAAS, sets the '" + LOGIN_CONFIG_PROP + "' system property.")
              .longOpt("solrJaasAuthConfig")
              .build(),
      Option.builder()
              .hasArg()
              .required(false)
              .desc("For authenticating to Solr using JAAS, sets the 'solr.kerberos.jaas.appname' system property; default is Client")
              .longOpt("solrJaasAppName")
              .build()
    };
  }

  // Creates an instance of the requested tool, using classpath scanning if necessary
  private static RDDProcessor newProcessor(String streamProcType) throws Exception {

    streamProcType = streamProcType.trim();

    if ("twitter-to-solr".equals(streamProcType))
      return new TwitterToSolrStreamProcessor();
    else if ("word-count".equals(streamProcType))
      return new WordCount();
    else if ("term-vectors".equals(streamProcType))
      return new ReadTermVectors();
    else if ("docfilter".equals(streamProcType))
      return new DocumentFilteringStreamProcessor();
    else if ("hdfs-to-solr".equals(streamProcType))
      return new HdfsToSolrRDDProcessor();
    else if ("logs2solr".equals(streamProcType))
      return new Logs2SolrRDDProcessor();
    else if ("query-solr-benchmark".equals(streamProcType))
      return new QueryBenchmark();
    else if ("kmeans-anomaly".equals(streamProcType))
      return new KMeansAnomaly();
    else if ("eventsim".equals(streamProcType))
      return new EventsimIndexer();

    // If you add a built-in RDDProcessor to this class, add it here to avoid
    // classpath scanning

    for (Class<RDDProcessor> next : findProcessorClassesInPackage("com.lucidworks.spark")) {
      RDDProcessor streamProc = next.newInstance();
      if (streamProcType.equals(streamProc.getName()))
        return streamProc;
    }

    System.err.println("\n\n "+streamProcType+
            " not supported! Please check your command-line arguments and re-try. \n\n");
    System.exit(1);

    return null; // won't get here
  }

  private static void displayProcessorOptions(PrintStream out) throws Exception {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("twitter-to-solr", getProcessorOptions(new TwitterToSolrStreamProcessor()));
    formatter.printHelp("word-count", getProcessorOptions(new WordCount()));
    formatter.printHelp("term-vectors", getProcessorOptions(new ReadTermVectors()));
    formatter.printHelp("docfilter", getProcessorOptions(new DocumentFilteringStreamProcessor()));
    formatter.printHelp("hdfs-to-solr", getProcessorOptions(new HdfsToSolrRDDProcessor()));
    formatter.printHelp("logs2solr", getProcessorOptions(new Logs2SolrRDDProcessor()));
    formatter.printHelp("query-solr-benchmark", getProcessorOptions(new QueryBenchmark()));
    formatter.printHelp("kmeans-anomaly", getProcessorOptions(new KMeansAnomaly()));
    formatter.printHelp("eventsim", getProcessorOptions(new EventsimIndexer()));

    List<Class<RDDProcessor>> toolClasses = findProcessorClassesInPackage("com.lucidworks.spark");
    for (Class<RDDProcessor> next : toolClasses) {
      RDDProcessor tool = next.newInstance();
      formatter.printHelp(tool.getName(), getProcessorOptions(tool));
    }
  }

  private static Options getProcessorOptions(RDDProcessor tool) {
    Options options = new Options();
    options.addOption("h", "help", false, "Print this message");
    options.addOption("v", "verbose", false, "Generate verbose log messages");
    Option[] toolOpts = joinCommonAndProcessorOptions(tool.getOptions());
    for (int i = 0; i < toolOpts.length; i++)
      options.addOption(toolOpts[i]);
    return options;
  }

  public static Option[] joinCommonAndProcessorOptions(Option[] toolOpts) {
    return joinOptions(getCommonOptions(), toolOpts);
  }

  public static Option[] joinOptions(Option[] lhs, Option[] rhs) {
    List<Option> options = new ArrayList<Option>();
    if (lhs != null && lhs.length > 0) {
      for (Option opt : lhs)
        options.add(opt);
    }

    if (rhs != null) {
      for (Option opt : rhs)
        options.add(opt);
    }

    return options.toArray(new Option[0]);
  }


  /**
   * Parses the command-line arguments passed by the user.
   */
  public static CommandLine processCommandLineArgs(Option[] customOptions, String[] args) {
    Options options = new Options();

    options.addOption("h", "help", false, "Print this message");
    options.addOption("v", "verbose", false, "Generate verbose log messages");

    if (customOptions != null) {
      for (int i = 0; i < customOptions.length; i++)
        options.addOption(customOptions[i]);
    }

    CommandLine cli = null;
    try {
      cli = (new GnuParser()).parse(options, args);
    } catch (ParseException exp) {
      boolean hasHelpArg = false;
      if (args != null && args.length > 0) {
        for (int z = 0; z < args.length; z++) {
          if ("-h".equals(args[z]) || "-help".equals(args[z])) {
            hasHelpArg = true;
            break;
          }
        }
      }
      if (!hasHelpArg) {
        System.err.println("Failed to parse command-line arguments due to: " + exp.getMessage());
      }
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(SparkApp.class.getName(), options);
      System.exit(1);
    }

    if (cli.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(SparkApp.class.getName(), options);
      System.exit(0);
    }

    return cli;
  }

  /**
   * Scans Jar files on the classpath for RDDProcessor implementations to activate.
   */
  @SuppressWarnings("unchecked")
  private static List<Class<RDDProcessor>> findProcessorClassesInPackage(String packageName) {
    List<Class<RDDProcessor>> streamProcClasses = new ArrayList<Class<RDDProcessor>>();
    try {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      String path = packageName.replace('.', '/');
      Enumeration<URL> resources = classLoader.getResources(path);
      Set<String> classes = new TreeSet<String>();
      while (resources.hasMoreElements()) {
        URL resource = (URL) resources.nextElement();
        classes.addAll(findClasses(resource.getFile(), packageName));
      }
      for (String classInPackage : classes) {
        Class<?> theClass = classLoader.loadClass(classInPackage);
        if (RDDProcessor.class.isAssignableFrom(theClass))
          streamProcClasses.add((Class<RDDProcessor>) theClass);
      }
    } catch (Exception e) {
      // safe to squelch this as it's just looking for streamProcs to run
      e.printStackTrace();
    }
    return streamProcClasses;
  }

  private static Set<String> findClasses(String path, String packageName) throws Exception {
    Set<String> classes = new TreeSet<String>();
    if (path.startsWith("file:") && path.contains("!")) {
      String[] split = path.split("!");
      URL jar = new URL(split[0]);
      ZipInputStream zip = new ZipInputStream(jar.openStream());
      ZipEntry entry;
      while ((entry = zip.getNextEntry()) != null) {
        if (entry.getName().endsWith(".class")) {
          String className = entry.getName().replaceAll("[$].*", "")
              .replaceAll("[.]class", "").replace('/', '.');
          if (className.startsWith(packageName)) {
            classes.add(className);
          }
        }
      }
    } else {
      // when running unit tests
      File dir = new File(path);
      if (dir.isDirectory()) {
        String packagePath = packageName.replace('.',File.separatorChar);
        if (dir.getAbsolutePath().endsWith(packagePath)) {
          for (File file : dir.listFiles()) {
            if (file.getName().endsWith(".class")) {
              String className = file.getName().replaceAll("[$].*", "")
                      .replaceAll("[.]class", "").replace('/', '.');
              if (className.indexOf("$") == -1)
                classes.add(packageName + "." + className);
            }
          }
        }
      }
    }
    return classes;
  }

  public static Serializable assertSerializable(Object obj) {
    Serializable ser = (Serializable)obj;
    try {
      ser = bytes2ser(ser2bytes(ser));
    } catch (Exception exc) {
      throw new IllegalArgumentException("Object of type ["+obj.getClass().getName()+"] is not Serializable due to: "+exc);
    }
    return ser;
  }

  public static Serializable bytes2ser(byte[] objBytes) throws Exception {
    Serializable ser = null;
    ObjectInputStream ois = null;
    try {
      ois = new ObjectInputStream(new ByteArrayInputStream(objBytes));
      ser = (Serializable) ois.readObject();
    } finally {
      if (ois != null) {
        try {
          ois.close();
        } catch (Exception ignore) {}
      }
    }
    return ser;
  }

  private static byte[] ser2bytes(Serializable obj) throws Exception {
    byte[] objBytes = null;
    ObjectOutputStream oos = null;
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      oos = new ObjectOutputStream(baos);
      oos.writeObject(obj);
      oos.flush();
      objBytes = baos.toByteArray();
    } finally {
      if (oos != null) {
        try {
          oos.close();
        } catch (Exception ign) {}
      }
    }
    return objBytes;
  }
}
