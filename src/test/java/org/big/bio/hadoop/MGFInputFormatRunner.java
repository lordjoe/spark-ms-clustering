package org.big.bio.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.big.bio.transformers.MGFStringToSpectrumTransformer;
import org.big.bio.utils.SparkUtil;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;

import java.io.IOException;

/**
 * This runner is Helper class for running MGFInputFormat on a given MGF file and
 * check the quality of the MGF and data. This can used for testing the correctness of MGF file.
 *
 * @author Rui Wang
 * @author Yasset Perez-Riverol
 *
 */
public class MGFInputFormatRunner {

    private static final Logger LOGGER = Logger.getLogger(MGFInputFormatRunner.class);

    public static void main(String[] args) throws IOException, InterruptedException {

        JavaSparkContext sparkConf = SparkUtil.createJavaSparkContext("Test MGF Read", "local[*]");
        Configuration hadoopConf = sparkConf.hadoopConfiguration();

        String hdfsFileName = "./data/spectra/";

        Class inputFormatClass = MGFInputFormat.class;
        Class keyClass = String.class;
        Class valueClass = String.class;

        JavaPairRDD<Text, Text> spectraAsStrings = sparkConf.newAPIHadoopFile(hdfsFileName, inputFormatClass, keyClass, valueClass, hadoopConf);

        JavaPairRDD<String, ISpectrum> spectra = spectraAsStrings.flatMapToPair(new MGFStringToSpectrumTransformer());

        boolean forceShuffle = true;
        JavaRDD<ISpectrum> spectraToScore = spectra.values();
        spectraToScore.coalesce(120, forceShuffle);

        spectraToScore = spectraToScore.persist(StorageLevel.DISK_ONLY());

        long pairs = spectraToScore.count();
        LOGGER.info("Read  " + pairs + " records");
    }

}
