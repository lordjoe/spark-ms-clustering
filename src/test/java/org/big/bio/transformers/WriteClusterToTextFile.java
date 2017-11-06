package org.big.bio.transformers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.big.bio.hadoop.ClusteringFileOutputFormat;
import org.big.bio.hadoop.MGFInputFormat;
import org.big.bio.keys.BinMZKey;
import org.big.bio.keys.MZKey;
import org.big.bio.utils.SparkUtil;
import org.junit.Before;
import org.junit.Test;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;

import java.io.IOException;
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
 * This class
 * <p>
 * Created by ypriverol (ypriverol@gmail.com) on 06/11/2017.
 */
public class WriteClusterToTextFile {

    private static final Logger LOGGER = Logger.getLogger(WriteClusterToTextFile.class);
    private String hdfsFileName;
    private String hdfsOutputFile;
    private JavaSparkContext sparkConf;

    @Before
    public void setup(){
        sparkConf = SparkUtil.createJavaSparkContext("Test Spectrum Transformation to Cluster", "local[*]");
        hdfsFileName = "./data/spectra/";
        hdfsOutputFile = "./hdfs/output/";
    }

    @Test
    public  void writingClusters() throws IOException, InterruptedException {

        Configuration hadoopConf = sparkConf.hadoopConfiguration();

        Class inputFormatClass = MGFInputFormat.class;
        Class keyClass = String.class;
        Class valueClass = String.class;

        JavaPairRDD<Text, Text> spectraAsStrings = sparkConf.newAPIHadoopFile(hdfsFileName, inputFormatClass, keyClass, valueClass, hadoopConf);

        JavaPairRDD<String, ISpectrum> spectra = spectraAsStrings.flatMapToPair(new MGFStringToSpectrumTransformer());
        LOGGER.info("Number of Spectra = " + spectra.count());

        JavaPairRDD<MZKey, ICluster> initialClusters =  spectra.flatMapToPair(new SpectrumToInitialClusterTransformer(sparkConf));

        JavaPairRDD<BinMZKey, ICluster> precursorClusters =  initialClusters.flatMapToPair(new PrecursorBinnerTransformer(sparkConf));

        JavaPairRDD<BinMZKey, Iterable<ICluster>> clusters = precursorClusters.groupByKey();

        JavaRDD<String> clusterString = clusters.map(new IterableClustersToStringTransformer());

        clusterString.saveAsTextFile(hdfsOutputFile);

        LOGGER.info("Number of Binned Clusters = " + precursorClusters.count());

    }

}