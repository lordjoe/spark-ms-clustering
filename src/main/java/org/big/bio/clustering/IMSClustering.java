package org.big.bio.clustering;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 * <p>
 * This interface provides the methods for all the clustering implementations. For example,
 * it provides the methods to retrieve the Spark Context.
 * <p>
 * Created by Yasset Perez-Riverol (ypriverol@gmail.com) on 27/10/2017.
 */
public interface IMSClustering {

    /**
     * This method returns the current spark context for the algorithm
     * @return JavaSparkContext
     */
    JavaSparkContext context();

    /**
     * Returns the configuration parameters for the clustering method.
     * @return Configuration.
     */
    IConfigurationParameters configuration();

    /**
     * This method returns a parameter value from the Configuration
     * @param key
     * @return
     */
    String getProperty(String key);
}
