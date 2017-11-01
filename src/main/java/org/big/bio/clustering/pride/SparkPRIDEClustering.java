package org.big.bio.clustering.pride;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.big.bio.clustering.IMSClustering;
import org.big.bio.clustering.MSClustering;
import org.big.bio.utils.IOUtilities;
import org.big.bio.utils.SparkUtil;


/**
 * The SparkML Clustering KMeans used a Bisecting Kmeans approach as hierarchical clustering approached.
 * The input of the algorithm is the folder that contains all the Projects and corresponding spectra.
 *
 * @author Yasset Perez-Riverol
 *
 */
public class SparkPRIDEClustering extends MSClustering {

    private static final Logger LOGGER = Logger.getLogger(SparkPRIDEClustering.class);

    public static final String WINDOW_SIZE_PROPERTY = "mapper.window_size";

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
                clusteringMethod = new SparkPRIDEClustering();
            }else{
                clusteringMethod = new MSClustering(cmd.getOptionValue("c"));
            }

            String inputPath = cmd.getOptionValue("i");

            JavaPairRDD<String, String> spectra = IOUtilities.parseMGFRDD(clusteringMethod.context(), inputPath);
            SparkUtil.persistAndCount("Number of Spectra", spectra);


        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}