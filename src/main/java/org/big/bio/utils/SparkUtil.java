package org.big.bio.utils;

import org.apache.commons.configuration.ConfigurationFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;

/**
 * This is a utility class to create JavaSparkContext 
 * and other objects required by Spark. There are many 
 * ways to create JavaSparkContext object. Here we offer 
 * 2 ways to create it:
 *
 *   1. by using YARN's resource manager host name
 *
 *   2. by using spark master URL, which is expressed as: 
 *
 *           spark://<spark-master-host-name>:7077
 *
 * @author ypriverol
 *
 */
public class SparkUtil {

    /**
     * The application name is provided and the node where this will run, it works
     * for local machines, foe example:
     * local[*]
     *
     * @param applicationName Application Name
     * @param master Machine where this application will run
     * @return JavaSparkContext
     */
    public static JavaSparkContext createJavaSparkContext(String applicationName, String master){
      SparkConf conf = new SparkConf().setAppName(applicationName).setMaster(master);

      // This is need to re-write the corresponding partition.
      conf.set("spark.hadoop.validateOutputSpecs", "false");
      return new JavaSparkContext(conf);
   }

    /**
     * The application name. The node or machine where the algorithm will run needs to be
     * provided by the config file.
     *
     * @param applicationName Application Name
     * @return JavaSparkContext
     */
   public static JavaSparkContext createJavaSparkContextWithFile(String applicationName, String confFile){
       SparkConf conf = new SparkConf().setAppName(applicationName);
        // This is need to re-write the corresponding partition.
       conf.set("spark.hadoop.validateOutputSpecs", "false");
       conf.set("spark.kryo.classesToRegister", "org.apache.hadoop.io.LongWritable, org.apache.hadoop.io.Text");
       JavaSparkContext sparkContext = new JavaSparkContext("local[*]", applicationName);
       sparkContext.addFile(confFile);

       return sparkContext;
    }


}
