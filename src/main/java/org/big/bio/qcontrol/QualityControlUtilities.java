package org.big.bio.qcontrol;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;
import uk.ac.ebi.pride.spectracluster.spectrum.KnownProperties;

/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 * <p>
 * This class contains functionalities for Quality control of Clusters. Including
 * <p>
 * Created by ypriverol (ypriverol@gmail.com) on 10/11/2017.
 */
public class QualityControlUtilities {


    /**
     * This function return true if the cluster average ratio is over a certain value.
     *
     * @param iCluster Cluster to filter
     * @return double average of identified spectra
     */
    public static double avgIdentifiedRatio(ICluster iCluster) {
        double identified = 0;
        double total = iCluster.getClusteredSpectraCount();
        for(ISpectrum spectrum: iCluster.getClusteredSpectra()){
            if(spectrum.getProperty(KnownProperties.IDENTIFIED_PEPTIDE_KEY) != null)
                identified++;
        }
        return identified/total;
    }

    /**
     * Number of identified spectra in the cluster
     * @param iCluster cluster
     * @return number of identified spectra
     */
    public static int numberOfIdentifiedSpectra(ICluster iCluster){
        int identified = 0;
        for(ISpectrum spectrum: iCluster.getClusteredSpectra()){
            if(spectrum.getProperty(KnownProperties.IDENTIFIED_PEPTIDE_KEY) != null)
                identified++;
        }
        return identified;
    }

    /**
     * Return the number of Spectra by Cluster
     * @param cluster Cluster
     * @return Number of spectra in the cluster.
     */
    public static int numberOfSpectra(ICluster cluster){
        return cluster.getClusteredSpectraCount();
    }


    /**
     * Clustering Global Quality is the ratio of spectra between Identified Clusters (clusters with identified spectra  >=3)
     * and all spectra.
     *
     * @param clusters result clusters
     * @param identifiedPeptideThershold number of identified spectra in a cluster to consider it identified
     * @return ratio.
     */
    public static double clusteringGlobalQuality(JavaRDD<ICluster> clusters, int identifiedPeptideThershold ){
        return (double)numberOfSpectra(clusters.filter(cluster -> numberOfIdentifiedSpectra(cluster) > identifiedPeptideThershold))/ (double)numberOfSpectra(clusters);
    }

    private static int numberOfSpectra(JavaRDD<ICluster> clusters) {
        return clusters.map(cluster -> cluster.getClusteredSpectraCount()).reduce((acum , n) -> acum + n);
    }


}
