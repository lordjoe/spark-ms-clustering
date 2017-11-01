package org.big.bio.transformers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.big.bio.hadoop.MGFInputFormat;
import org.big.bio.hadoop.MGFInputFormatRunner;
import org.big.bio.keys.MZKey;
import org.big.bio.utils.SparkUtil;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 * <p>
 * This class
 * <p>
 * Created by ypriverol (ypriverol@gmail.com) on 01/11/2017.
 */
public class SpectrumToInitialClusterTest {

    private static final Logger LOGGER = Logger.getLogger(SpectrumToInitialClusterTest.class);

    public static void main(String[] args) throws IOException, InterruptedException {

        JavaSparkContext sparkConf = SparkUtil.createJavaSparkContext("Test Spectrum Transformation to Cluster", "local[*]");
        Configuration hadoopConf = sparkConf.hadoopConfiguration();

        String hdfsFileName = "./data/spectra/";

        Class inputFormatClass = MGFInputFormat.class;
        Class keyClass = String.class;
        Class valueClass = String.class;

        JavaPairRDD<Text, Text> spectraAsStrings = sparkConf.newAPIHadoopFile(hdfsFileName, inputFormatClass, keyClass, valueClass, hadoopConf);

        JavaPairRDD<String, ISpectrum> spectra = spectraAsStrings.flatMapToPair(new MGFStringTupleToSpectrumTuple());
        LOGGER.info("Number of Spectra = " + spectra.count());

        JavaPairRDD<MZKey, ICluster> initialClusters =  spectra.flatMapToPair(new SpectrumToInitialCluster(sparkConf));
        LOGGER.info("Number of Spectra = " + initialClusters.count());


    }

}