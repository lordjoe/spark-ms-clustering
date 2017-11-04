package org.big.bio.clustering.pride;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.big.bio.clustering.IMSClustering;
import org.big.bio.clustering.MSClustering;
import org.big.bio.hadoop.MGFInputFormat;
import org.big.bio.keys.BinMZKey;
import org.big.bio.keys.MZKey;
import org.big.bio.transformers.MGFStringToSpectrumTransformer;
import org.big.bio.transformers.PrecursorBinnerTransformer;
import org.big.bio.transformers.SpectrumToInitialClusterTransformer;
import org.big.bio.utils.IOUtilities;
import org.big.bio.utils.SparkUtil;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;

import java.util.List;


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

            Class inputFormatClass = MGFInputFormat.class;
            Class keyClass = String.class;
            Class valueClass = String.class;

            JavaPairRDD<Text, Text> spectraAsStrings = clusteringMethod.context().newAPIHadoopFile(inputPath, inputFormatClass, keyClass, valueClass, clusteringMethod.context().hadoopConfiguration());
            LOGGER.info("Number of Spectra = " + spectraAsStrings.count());

            JavaPairRDD<BinMZKey, ICluster> spectra = spectraAsStrings
                    .flatMapToPair(new MGFStringToSpectrumTransformer())
                    .flatMapToPair(new SpectrumToInitialClusterTransformer(clusteringMethod.context()))
                    .flatMapToPair(new PrecursorBinnerTransformer(clusteringMethod.context()));

            LOGGER.info("Number of Spectra = " + spectra.count());

            JavaPairRDD<BinMZKey, Iterable<ICluster>> clusters = spectra.groupByKey();

            LOGGER.info("Number Clusters by BinMz = " + clusters.count());


        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}