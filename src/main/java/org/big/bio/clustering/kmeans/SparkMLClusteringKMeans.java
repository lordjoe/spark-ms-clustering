package org.big.bio.clustering.kmeans;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.big.bio.keys.BinMZKey;
import org.big.bio.utils.IOUtilities;
import org.big.bio.utils.SparkUtil;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;

import java.util.List;


/**
 * The SparkML Clustering KMeans used a Bisecting Kmeans approach as hierarchical clustering approached.
 * The input of the algorithm is the folder that contains all the Projects and corresponding spectra.
 *
 * @author ypriverol
 *
 */
public class SparkMLClusteringKMeans extends MSClustering{

    private static final Logger LOGGER = Logger.getLogger(SparkMLClusteringKMeans.class);

    public static void main(String[] args) {

        IMSClustering clusteringMethod;
        try {

            CommandLine cmd = MSClustering.parseCommandLine(args, MSClustering.getCLIParameters());
            if(!cmd.hasOption("i")){
                MSClustering.printHelpCommands();
                System.exit( -1 );
            }
            if(!cmd.hasOption("c")){
                LOGGER.info("The algorithm will run in local mode");
                clusteringMethod = new SparkMLClusteringKMeans();
            }else{
                clusteringMethod = new MSClustering(cmd.getOptionValue("c"));
            }

            String inputPath = cmd.getOptionValue("i");

            JavaPairRDD<String, String> spectra = IOUtilities.parseMGFRDD(clusteringMethod.context(), inputPath);

            LOGGER.info("Number of Spectra = " + spectra.count());


        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}