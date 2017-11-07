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


/**
 * The SparkML Clustering KMeans used a Bisecting Kmeans approach as hierarchical clustering approached.
 * The input of the algorithm is the folder that contains all the Projects and corresponding spectra.
 *
 * @author Yasset Perez-Riverol
 *
 */
public class SparkPRIDEClustering extends MSClustering {

    private static final Logger LOGGER = Logger.getLogger(SparkPRIDEClustering.class);

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

            // The first iteration of the algorithm cluster spectra only using the more relevant peaks.






        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}