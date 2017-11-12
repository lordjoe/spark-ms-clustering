package org.big.bio.qcontrol;

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
}
