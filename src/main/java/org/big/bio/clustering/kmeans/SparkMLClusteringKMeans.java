package org.big.bio.clustering.kmeans;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.big.bio.utils.SparkUtil;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;


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

        SparkMLClusteringKMeans kmeansClustering = new SparkMLClusteringKMeans();

        try {

            CommandLine cmd = kmeansClustering.parseCommandLine(args, kmeansClustering.getCLIParameters());
            if(!cmd.hasOption("i")){
                kmeansClustering.printHelpCommands();
                System.exit( -1 );
            }
            String inputPath = cmd.getOptionValue("i");

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}