package org.big.bio.clustering.pride;

import java.util.ArrayList;
import java.util.List;

/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 * <p>
 * This class contains helper methods for PRIDE Cluster algorithm.
 * <p>
 * Created by ypriverol (ypriverol@gmail.com) on 07/11/2017.
 */
public class PRIDEClusterUtils {

    /**
     * Helper function to create the List of clustering thresholds based on the initial starting threshold
     * (highest precision), the target threshold, and the number of clustering rounds to perform.
     * @param startThreshold Highest starting precision to use.
     * @param endThreshold Lowest target precision for the clustering run.
     * @param clusteringRounds Number of round of clustering to perform.
     * @return A List of thresholds used as parameter for the clustering runs.
     */
    public static List<Float> generateClusteringThresholds(Float startThreshold, Float endThreshold, int clusteringRounds) {
        List<Float> thresholds = new ArrayList<>(clusteringRounds);
        float stepSize = (startThreshold - endThreshold) / (clusteringRounds - 1);

        for (int i = 0; i < clusteringRounds; i++) {
            thresholds.add(startThreshold - (stepSize * i));
        }

        return thresholds;
    }
}
