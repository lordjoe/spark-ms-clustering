package org.big.bio.qcontrol;

import org.big.bio.core.IConfigurationParameters;

import java.util.Properties;

/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 * <p>
 * This class Controls the Default parameters for the Quality Control
 * <p>
 * Created by ypriverol (ypriverol@gmail.com) on 10/11/2017.
 */
public class QualityControlDefaultParameters implements IConfigurationParameters{

    /** Quality Control Peptides */

    //Number of identified spectra in the cluster.
    static final int DEFAULT_NUMBER_OF_SPECTRA_IDENTIFIED = 3;

    /** Properties for Quality Control */

    // Number of identified spectra in a cluster.
    static final String NUMBER_OF_SPECTRA_IDENTIFIED_PROPERTY = "pride.cluster.qc.identified.spectra";

    @Override
    public Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(NUMBER_OF_SPECTRA_IDENTIFIED_PROPERTY, String.valueOf(DEFAULT_NUMBER_OF_SPECTRA_IDENTIFIED));
        return properties;
    }
}