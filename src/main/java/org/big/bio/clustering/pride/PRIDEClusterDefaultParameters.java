package org.big.bio.clustering.pride;


import org.apache.hadoop.conf.Configuration;
import org.big.bio.clustering.IConfigurationParameters;
import uk.ac.ebi.pride.spectracluster.similarity.ISimilarityChecker;
import uk.ac.ebi.pride.spectracluster.util.Defaults;
import uk.ac.ebi.pride.spectracluster.util.MZIntensityUtilities;
import uk.ac.ebi.pride.spectracluster.util.NumberUtilities;
import uk.ac.ebi.pride.spectracluster.util.binner.IWideBinner;
import uk.ac.ebi.pride.spectracluster.util.binner.SizedWideBinner;

import java.util.Properties;

/**
 * Default configurations for running pride cluster algorithm, when the algorithm is run, this parameters are used if they are not provided
 * by the user in the configuration file.
 *
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @author Yasset Perez-Riverol
 *
 */
public class PRIDEClusterDefaultParameters implements IConfigurationParameters {

    /** Default PRIDE Cluster Algorithm values  **/

    // Major clustering sliding window is this
    public static final double DEFAULT_MAJOR_PEAK_MZ_WINDOW  = 4.0;

    // window to merge to spectrum in one cluster.
    public static final double DEFAULT_SPECTRUM_MERGE_WINDOW = 0.5;

    // the number of major peaks that would be consider in the first iteration of clustering
    public static final int DEFAULT_MAJOR_PEAK_COUNT = 5;

    // Enable the peak filter
    public static final boolean DEFAULT_ENABLE_COMPARISON_PEAK_FILTER = true;

    // Number of highest intensity peaks that would be consider for the clustering.
    public static final int DEFAULT_INITIAL_HIGHEST_PEAK_FILTER = 150;

    //Number of comparisons to be performed in the algorithm
    public static final int DEFAULT_MIN_NUMBER_COMPARISONS  = 10000;

    /**
     * If this option is > 0, a new clustering engine is being
     * created if the total number of clusters is reached
     */
    public static final int DEFAULT_MAXIMUM_NUMBER_OF_CLUSTERS = 0;

    // Binning sizes
    private static final double NARRROW_BIN_WIDTH = 1; // 0.15; //0.005; // 0.3;
    private static final double NARRROW_BIN_OVERLAP = 0; // 0.03; //0.002; // 0.1;

    // Default WIDE MZ Binner
    public static final IWideBinner DEFAULT_WIDE_MZ_BINNER = new SizedWideBinner(MZIntensityUtilities.HIGHEST_USABLE_MZ,
            NARRROW_BIN_WIDTH, MZIntensityUtilities.LOWEST_USABLE_MZ,
            NARRROW_BIN_OVERLAP);

    //Todo: We need to define How this is used and if is not removed it.
    public static final String DEFAULT_BINNING_RESOURCE = "/pride-binning.tsv";

    // This is the default checker for the similarity CombinedFisherIntesityTest
    public static final String DEFAULT_SIMILARITY_CHECKER_CLASS = "uk.ac.ebi.pride.spectracluster.similarity.CombinedFisherIntensityTest";

    // Start threshold of the PRIDE Cluster algorithm.
    public static final Float DEFAULT_START_THRESHOLD = 0.999F;

    // End threshold of the PRIDE Cluster algorithm.
    public static final Float DEFAULT_END_THRESHOLD  = 0.99F;


    /** Label of each Default property of PRIDE cluster algorithm  **/


    // Number of Peaks to be compare on each Spectra comparison.
    public static final String NUMBER_COMPARED_PEAKS_PROPERTY  = "pride.cluster.number.compared.peaks";


    // Fragment ION Tolerance that would be use to define if two MZ values are equal.
    public static final String FRAGMENT_ION_TOLERANCE_PROPERTY = "pride.cluster.similarity.fragment.tolerance";

    // Retain similarity threshold to keep the spectra withing the same cluster.
    public static final String RETAIN_THRESHOLD_PROPERTY       = "pride.cluster.retain.threshold";

    // Cluster similarity Threshold.
    public static final String SIMILARITY_THRESHOLD_PROPERTY   = "pride.cluster.similarity.threshold";

    // Todo: We need to define what is this.
    public static final String SPECTRUM_MERGE_WINDOW_PROPERTY  = "pride.cluster.spectrum.merge.window";

    // Major Peaks windows define the threshold between the major peak and all the rest peaks in the spectrum.
    public static final String MAJOR_PEAK_WINDOW_PROPERTY      = "pride.cluster.major.clustering.window";

    //Clustering minimum num,ber of comparisons by cluster.
    public static final String CDF_MIN_NUMBER_COMPARISONS_PROPERTY    = "pride.cluster.cdf.min_number_comparisons";

    // Todo: We need to define what is this.
    public static final String ENABLE_COMPARISON_PEAK_FILTER_PROPERTY = "enable.comparison.clustering.filter";

    // Todo: We need to define what is this.
    public static final String INITIAL_HIGHEST_PEAK_FILTER_PROPERTY   = "pride.cluster.initial.highest.clustering.filter";

    public static final String MAXIMUM_NUMBER_CLUSTERS_PROPERTY       = "pride.cluster.maximum_number_clusters";

    // Check the similarity checker method.
    public static final String SIMILARITY_CHECKER_PROPERTY            = "pride.cluster.similarity.checker";

    // PRIDE Cluster consensus min of peaks.
    public static final String CONSENSUS_SPEC_MIN_PEAKS_PROPERTY      = "pride.cluster.consensus_min_peaks";

    // PRIDE Cluster Major Peak Count first iteration.
    public static final String MAJOR_PEAK_COUNT_PROPERTY              = "pride.cluster.major.peak.count";

    //PRIDE Cluster start threshold accuracy.
    public static final String CLUSTER_START_THRESHOLD_PROPERTY       = "pride.cluster.start.threshold";

    //PRIDE Cluster end threshold accuracy
    public static final String CLUSTER_END_THRESHOLD_PROPERTY         = "pride.cluster.end.thershold";



    /**
     * Get Default properties for PRIDE Cluster algorithm. Some of this properties are defined in
     * Spectra Cluster sequential algorithm https://github.com/spectra-cluster/spectra-cluster
     *
     * @return Default Properties of PRIDE Cluster algorithm
     */

    @Override
    public Properties getProperties() {
        Properties properties = new Properties();

        // Set the default properties, most of these properties are defined in the sequential algorithm.
        properties.setProperty(NUMBER_COMPARED_PEAKS_PROPERTY, String.valueOf(Defaults.getNumberComparedPeaks()));
        properties.setProperty(FRAGMENT_ION_TOLERANCE_PROPERTY, NumberUtilities.formatDouble(Defaults.getFragmentIonTolerance(), 3));
        properties.setProperty(SIMILARITY_THRESHOLD_PROPERTY, NumberUtilities.formatDouble(Defaults.getSimilarityThreshold(), 3));
        properties.setProperty(RETAIN_THRESHOLD_PROPERTY, NumberUtilities.formatDouble(Defaults.getRetainThreshold(), 3));
        properties.setProperty(MAJOR_PEAK_WINDOW_PROPERTY, NumberUtilities.formatDouble(PRIDEClusterDefaultParameters.DEFAULT_MAJOR_PEAK_MZ_WINDOW, 3));
        properties.setProperty(SPECTRUM_MERGE_WINDOW_PROPERTY, NumberUtilities.formatDouble(PRIDEClusterDefaultParameters.DEFAULT_SPECTRUM_MERGE_WINDOW, 3));
        properties.setProperty(CONSENSUS_SPEC_MIN_PEAKS_PROPERTY, String.valueOf(Defaults.getDefaultConsensusMinPeaks()));
        properties.setProperty(CDF_MIN_NUMBER_COMPARISONS_PROPERTY, String.valueOf(PRIDEClusterDefaultParameters.DEFAULT_MIN_NUMBER_COMPARISONS));
        properties.setProperty(CONSENSUS_SPEC_MIN_PEAKS_PROPERTY, String.valueOf(Defaults.DEFAULT_CONSENSUS_MIN_PEAKS));
        properties.setProperty(SIMILARITY_CHECKER_PROPERTY, PRIDEClusterDefaultParameters.DEFAULT_SIMILARITY_CHECKER_CLASS);
        properties.setProperty(ENABLE_COMPARISON_PEAK_FILTER_PROPERTY, Boolean.toString(DEFAULT_ENABLE_COMPARISON_PEAK_FILTER));
        properties.setProperty(MAXIMUM_NUMBER_CLUSTERS_PROPERTY, String.valueOf(DEFAULT_MAXIMUM_NUMBER_OF_CLUSTERS));
        properties.setProperty(INITIAL_HIGHEST_PEAK_FILTER_PROPERTY, String.valueOf(DEFAULT_INITIAL_HIGHEST_PEAK_FILTER));
        properties.setProperty(MAJOR_PEAK_COUNT_PROPERTY, String.valueOf(DEFAULT_MAJOR_PEAK_COUNT));
        properties.setProperty(CLUSTER_START_THRESHOLD_PROPERTY, String.valueOf(DEFAULT_START_THRESHOLD));
        properties.setProperty(CLUSTER_END_THRESHOLD_PROPERTY, String.valueOf(DEFAULT_END_THRESHOLD));

        return properties;
    }


    public static ISimilarityChecker getSimilarityCheckerFromConfiguration(Configuration configuration) {
        Class similarityCheckerClass = configuration.getClass(PRIDEClusterDefaultParameters.SIMILARITY_CHECKER_PROPERTY, Defaults.getDefaultSimilarityChecker().getClass(), ISimilarityChecker.class);
        ISimilarityChecker similarityChecker;
        try {
            similarityChecker = (ISimilarityChecker) similarityCheckerClass.newInstance();
        }
        catch (Exception e) {
            // throw an IllegalStateException for now
            throw new IllegalStateException(e);
        }

        return similarityChecker;
    }
}
