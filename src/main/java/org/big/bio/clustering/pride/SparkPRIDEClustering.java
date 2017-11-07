package org.big.bio.clustering.pride;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.big.bio.clustering.IMSClustering;
import org.big.bio.clustering.MSClustering;
import org.big.bio.hadoop.MGFileFInputFormat;
import org.big.bio.keys.BinMZKey;
import org.big.bio.transformers.MGFStringToSpectrumTransformer;
import org.big.bio.transformers.PrecursorBinnerTransformer;
import org.big.bio.transformers.SpectrumToInitialClusterTransformer;
import org.big.bio.utils.SparkUtil;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.similarity.ISimilarityChecker;
import uk.ac.ebi.pride.spectracluster.util.predicate.cluster_comparison.ClusterShareMajorPeakPredicate;

import java.io.IOException;
import java.util.Properties;


/**
 * The SparkML Clustering KMeans used a Bisecting Kmeans approach as hierarchical clustering approached.
 * The input of the algorithm is the folder that contains all the Projects and corresponding spectra.
 *
 * @author Yasset Perez-Riverol
 *
 */
public class SparkPRIDEClustering extends MSClustering {

    // Logger
    private static final Logger LOGGER = Logger.getLogger(SparkPRIDEClustering.class);

    // Default parameters for the algorithm
    private static PRIDEClusterDefaultParameters defaultParameters = new PRIDEClusterDefaultParameters();

    //Application Spark PRIDE Clustering
    private static String APPLICATION_NAME = "SparkPRIDEClustering";

    /**
     * SparkClustering algorithm created from conf file and default properties files.
     * @param confFile Configuration file
     * @param properties default properties.
     * @throws IOException
     */
    public SparkPRIDEClustering(String confFile, Properties properties) throws IOException {
        super(APPLICATION_NAME, confFile, properties);
    }

    public SparkPRIDEClustering() {
        super(APPLICATION_NAME);
    }

    /**
     * Main method for running cluster.
     *
     * parameters:
     *  --conf: Configuration File with all paramters for the algorithm.
     *  --input-path: Input path
     *
     * @param args arguments parameters for PRIDE Cluster algorithm
     *
     */
    public static void main(String[] args) {

        // Clustering method.
        IMSClustering clusteringMethod;

        try {
            // Parse the corresponding parameters of the algorithm
            CommandLine cmd = MSClustering.parseCommandLine(args, MSClustering.getCLIParameters());

            if(!cmd.hasOption("i")){
                MSClustering.printHelpCommands();
                System.exit( -1 );
            }

            if(!cmd.hasOption("c")){
                LOGGER.info("The algorithm will run in local mode");
                clusteringMethod = new SparkPRIDEClustering();
            }else{
                clusteringMethod = new SparkPRIDEClustering(cmd.getOptionValue("c"), defaultParameters.getProperties());
            }

            String inputPath = cmd.getOptionValue("i");

            Class inputFormatClass = MGFileFInputFormat.class;
            Class keyClass = String.class;
            Class valueClass = String.class;

            // Read the corresponding Spectra from the File System.
            JavaPairRDD<Text, Text> spectraAsStrings = clusteringMethod.context().newAPIHadoopFile(inputPath, inputFormatClass, keyClass, valueClass, clusteringMethod.context().hadoopConfiguration());
            SparkUtil.collectLogCount("Number of Spectra", spectraAsStrings);

            // Process the Spectra Files and convert them into BinMZKey Hash map. Each entry correspond to a "unique" precursor mass.
            JavaPairRDD<BinMZKey, ICluster> spectra = spectraAsStrings
                    .flatMapToPair(new MGFStringToSpectrumTransformer())
                    .flatMapToPair(new SpectrumToInitialClusterTransformer(clusteringMethod.context()))
                    .flatMapToPair(new PrecursorBinnerTransformer(clusteringMethod.context()));
            SparkUtil.collectLogCount("Number of Binned Precursors" , spectra);

            // Group the ICluster by BinMzKey.
            JavaPairRDD<BinMZKey, Iterable<ICluster>> binnedPrecursors = spectra.groupByKey();
            SparkUtil.collectLogCount("Number Clusters by BinMz", binnedPrecursors);

            // The first step is to create the Major comparison predicate.
            ClusterShareMajorPeakPredicate comparisonPredicate = new ClusterShareMajorPeakPredicate(Integer.parseInt(clusteringMethod.getProperty(PRIDEClusterDefaultParameters.MAJOR_PEAK_COUNT_PROPERTY)));

            // Create the similarity Checker.
            ISimilarityChecker similarityChecker = PRIDEClusterDefaultParameters.getSimilarityCheckerFromConfiguration(clusteringMethod.context().hadoopConfiguration());
            double originalPrecision = Float.parseFloat(clusteringMethod.getProperty(PRIDEClusterDefaultParameters.CLUSTER_START_THRESHOLD_PROPERTY));

            binnedPrecursors = binnedPrecursors.flatMapToPair(new IncrementalClustering(similarityChecker, originalPrecision, null, comparisonPredicate));
            SparkUtil.collectLogCount("Number Clusters by BinMz", binnedPrecursors);



        } catch (ParseException | IOException e) {
            MSClustering.printHelpCommands();
            e.printStackTrace();
        }
    }



}