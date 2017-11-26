package org.big.bio.clustering.pride;

import com.lordjoe.distributed.spark.accumulators.AbstractLoggingPairFlatMapFunction;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.big.bio.clustering.IMSClustering;
import org.big.bio.clustering.MSClustering;
import org.big.bio.clustering.output.*;
import org.big.bio.hadoop.ClusteringFileOutputFormat;
import org.big.bio.hadoop.MGFileFInputFormat;
import org.big.bio.keys.BinMZKey;
import org.big.bio.transformers.*;
import org.big.bio.utils.IdentityFlatMapFunction;
import org.big.bio.utils.SparkUtil;
import scala.Tuple2;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.similarity.ISimilarityChecker;
import uk.ac.ebi.pride.spectracluster.util.predicate.cluster_comparison.ClusterShareMajorPeakPredicate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * The SparkML Clustering KMeans used a Bisecting Kmeans approach as hierarchical clustering approached.
 * The input of the algorithm is the folder that contains all the Projects and corresponding spectra.
 *
 * @author Yasset Perez-Riverol
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
     *
     * @param confFile   Configuration file
     * @param properties default properties.
     * @throws IOException Error creating the Context or reading the properties.
     */
    public SparkPRIDEClustering(String confFile, Properties properties) throws IOException {
        super(APPLICATION_NAME, confFile, properties);
    }

    public SparkPRIDEClustering() {
        super(APPLICATION_NAME);
    }

    public static IMSClustering getClusteringMethod(CommandLine cmd) {
        try {
            if (!cmd.hasOption("c")) {
                LOGGER.info("The algorithm will run in local mode");
                return new SparkPRIDEClustering();
            } else {
                return new SparkPRIDEClustering(cmd.getOptionValue("c"), defaultParameters.getProperties());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);

        }

    }

    public static boolean useOutputFile(CommandLine cmd) {
        boolean ret = false;
        if (cmd.hasOption("f")) {
            ret = "true".equals(cmd.getOptionValue("f"));
        }
        return ret;
    }

    /**
     * output may be saved as a single file or a hadoop directory
     * @param hdfsOutputFile
     * @param binnedPrecursors
     * @param clusteringMethod
     * @param cmd
     */
    public static void saveOutput(String hdfsOutputFile, JavaPairRDD<BinMZKey, Iterable<ICluster>> binnedPrecursors, IMSClustering clusteringMethod,
                                  CommandLine cmd ) {

        SparkUtil.collectLogCount("Number Clusters by BinMz", binnedPrecursors);
        if(useOutputFile(cmd))
            saveOutputAsFile(hdfsOutputFile,  binnedPrecursors, clusteringMethod);
         else
            saveOutputAsHadoopDirectory(hdfsOutputFile,  binnedPrecursors, clusteringMethod);


    }

    /**
     *  output may be saved as a   hadoop directory
     * @param hdfsOutputFile
     * @param binnedPrecursors
     * @param clusteringMethod
     */
    public static void saveOutputAsHadoopDirectory(String hdfsOutputFile, JavaPairRDD<BinMZKey, Iterable<ICluster>> binnedPrecursors, IMSClustering clusteringMethod) {

        // The export can be done in two different formats CGF or Clustering (JSON)
        JavaPairRDD<String, String> finalStringClusters;
        if (Boolean.parseBoolean(clusteringMethod.getProperty(PRIDEClusterDefaultParameters.CLUSTER_EXPORT_FORMAT_CDF_PROPERTY)))
            finalStringClusters = binnedPrecursors.flatMapToPair(new IterableClustersToCGFStringTransformer());
        else
            finalStringClusters = binnedPrecursors.flatMapToPair(new IterableClustersToJSONStringTransformer());

        // Final export of the results
        finalStringClusters.saveAsNewAPIHadoopFile(hdfsOutputFile, String.class, String.class, ClusteringFileOutputFormat.class);

    }

    /**
     *
     * output may be saved as a single file
     * @param hdfsOutputFile
     * @param binnedPrecursors
     * @param clusteringMethod
     */
    public static void saveOutputAsFile(String hdfsOutputFile, JavaPairRDD<BinMZKey, Iterable<ICluster>> binnedPrecursors, IMSClustering clusteringMethod) {
        IClusterConverter converrt = new CGFClusterConverter();


        binnedPrecursors.flatMapToPair()
        JavaRDD<Iterable<ICluster>> binMZKeyUJavaPairRDD = binnedPrecursors.values( );
        JavaRDD<ICluster> clusters = binMZKeyUJavaPairRDD.flatMap(new IdentityFlatMapFunction());
        clusters.sortBy(  )
        String property = clusteringMethod.getProperty(PRIDEClusterDefaultParameters.CLUSTER_EXPORT_FORMAT_CDF_PROPERTY);
        if (Boolean.parseBoolean(property))
            converrt = new CGFClusterConverter();
        else
            converrt = new JSONClusterConverter();

        TestClusterWriter writer = new TestClusterWriter(converrt);
        ClusterConsolidator consolidator = new ClusterConsolidator(writer,clusteringMethod,hdfsOutputFile);
        JavaRDD<Iterable<ICluster>> values = binnedPrecursors.values();
        consolidator.writeScores(values);
    }

    public static class Foo extends AbstractLoggingPairFlatMapFunction< Iterable<ICluster>,BinMZKey, ICluster >    {

        @Override
        public Iterable<Tuple2<BinMZKey, ICluster>> doCall(Iterable<ICluster> iClusters) throws Exception {
            List<Tuple2<BinMZKey, ICluster>> ret = new ArrayList<Tuple2<BinMZKey, ICluster>>();
            for (ICluster iCluster : iClusters) {

            }
            return ret;
        }
    }

    /**
     * Main method for running cluster.
     * <p>
     * parameters:
     * --conf: Configuration File with all paramters for the algorithm.
     * --input-path: Input path
     *
     * @param args arguments parameters for PRIDE Cluster algorithm
     */
    public static void main(String[] args) {


        try {
            // Parse the corresponding parameters of the algorithm
            CommandLine cmd = MSClustering.parseCommandLine(args, MSClustering.getCLIParameters());

            if (!cmd.hasOption("i") || !cmd.hasOption("o")) {
                MSClustering.printHelpCommands();
                System.exit(-1);
            }

            IMSClustering clusteringMethod = getClusteringMethod(cmd);

            // Input Path
            String inputPath = cmd.getOptionValue("i");

            //Output Path
            String hdfsOutputFile = cmd.getOptionValue("o");

            Class inputFormatClass = MGFileFInputFormat.class;
            Class keyClass = String.class;
            Class valueClass = String.class;

            // Read the corresponding Spectra from the File System.
            JavaPairRDD<Text, Text> spectraAsStrings = clusteringMethod.context().newAPIHadoopFile(inputPath, inputFormatClass, keyClass, valueClass, clusteringMethod.context().hadoopConfiguration());
            SparkUtil.collectLogCount("Number of Spectra", spectraAsStrings);

            String binWidthName = "bin_width"; // added slewis for   SpectrumToInitialClusterTransformer

            // Process the Spectra Files and convert them into BinMZKey Hash map. Each entry correspond to a "unique" precursor mass.
            JavaPairRDD<BinMZKey, ICluster> spectra = spectraAsStrings
                    .flatMapToPair(new MGFStringToSpectrumTransformer())
                    .flatMapToPair(new SpectrumToInitialClusterTransformer(clusteringMethod.context()))
                    .flatMapToPair(new PrecursorBinnerTransformer(clusteringMethod.context(),binWidthName));
            SparkUtil.collectLogCount("Number of Binned Precursors", spectra);

            // Group the ICluster by BinMzKey.
            JavaPairRDD<BinMZKey, Iterable<ICluster>> binnedPrecursors = spectra.groupByKey();
            SparkUtil.collectLogCount("Number Clusters by BinMz", binnedPrecursors);

            // The first step is to create the Major comparison predicate.
            ClusterShareMajorPeakPredicate comparisonPredicate = new ClusterShareMajorPeakPredicate(Integer.parseInt(clusteringMethod.getProperty(PRIDEClusterDefaultParameters.MAJOR_PEAK_COUNT_PROPERTY)));

            // Create the similarity Checker.
            ISimilarityChecker similarityChecker = PRIDEClusterDefaultParameters.getSimilarityCheckerFromConfiguration(clusteringMethod.context().hadoopConfiguration());
            double originalPrecision = Float.parseFloat(clusteringMethod.getProperty(PRIDEClusterDefaultParameters.CLUSTER_START_THRESHOLD_PROPERTY));

            binnedPrecursors = binnedPrecursors.flatMapToPair(new IncrementalClustering(similarityChecker, originalPrecision, null, comparisonPredicate));

            saveOutput(hdfsOutputFile, binnedPrecursors, clusteringMethod,cmd,clusteringMethod);


        } catch (Exception e) {
            MSClustering.printHelpCommands();
            e.printStackTrace();
        }
    }


}