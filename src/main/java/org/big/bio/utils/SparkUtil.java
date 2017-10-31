package org.big.bio.utils;

import org.apache.commons.configuration.ConfigurationFactory;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.big.bio.clustering.kmeans.SparkMLClusteringKMeans;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.Serializable;

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

    private static final Logger LOGGER = Logger.getLogger(SparkUtil.class);


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
       JavaSparkContext sparkContext = new JavaSparkContext("local[*]", applicationName);
       sparkContext.addFile(confFile);

       return sparkContext;
    }

    /**
     * This function allow to persist a JavaPAirRDD and print their count.
     *
     * @param message Message to Print in the LOGGER information.
     * @param inp     RDD Object
     * @return Same RDD persistent.
     */
    @Nonnull
    public static <K extends Serializable, V extends Serializable> JavaPairRDD<K, V> persistAndCount(@Nonnull final String message, @Nonnull final JavaPairRDD<K, V> inp) {
        JavaPairRDD<K, V> ret = persist(inp);
        long count = ret.count();
        LOGGER.debug(message + " = " + count);
        return ret;
    }

    /**
     * This made the RDD persitant in DISK ONLY.
     *
     * @param inp RDD
     * @return Same RDD but DISK Persistant
     */
    @Nonnull
    public static <K, V> JavaPairRDD<K, V> persist(@Nonnull final JavaPairRDD<K, V> inp) {
        return  persist(inp, StorageLevel.DISK_ONLY());
    }
    /**
     * persist in the best way - saves remembering which storage level
     *
     * @param inp
     * @return
     */
    @Nonnull
    public static <K, V> JavaPairRDD<K, V> persist(@Nonnull final JavaPairRDD<K, V> inp,StorageLevel storageLevel) {
        return inp.persist(storageLevel);
    }



}
