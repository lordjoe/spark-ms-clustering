package org.big.bio.clustering.pride;

import org.apache.hadoop.conf.Configuration;
import uk.ac.ebi.pride.spectracluster.similarity.ISimilarityChecker;
import uk.ac.ebi.pride.spectracluster.util.Defaults;
import uk.ac.ebi.pride.spectracluster.util.NumberUtilities;

import java.io.IOException;

/**
 *
 * Some configurations for the PRIDE Cluster Algorithm as originally published by Griss et. al.
 * This configuration file allows to define a set of parameters for any of the algorithms implemented.
 *
 * To enable these configurations, change the related job xml configuration file.
 *
 * For example, if you want to change LARGE_BINNING_REGION_PROPERTY to 3, you can add the following xml elements
 *
 * <property>
 *  <name>pride.cluster.large.binning.region</name>
 *  <value>3</value>
 * </property>
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @author Johannes Griss
 * @author Yasset Perez-Riverol
 */
public class ConfigurableProperties {

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
    public static final String CDF_MIN_NUMBER_COMPARISONS      = "cdf.min_number_comparisons";

    // Todo: We need to define what is this.
    public static final String ENABLE_COMPARISON_PEAK_FILTER   = "enable.comparison.clustering.filter";

    // Todo: We need to define what is this.
    public static final String INITIAL_HIGHEST_PEAK_FILTER     = "initial.highest.clustering.filter";

    public static final String MAXIMUM_NUMBER_CLUSTERS         = "pride.cluster.maximum_number_clusters";

    // Check the similarity checker method.
    public static final String SIMILARITY_CHECKER_PROPERTY     =  "pride.cluster.similarity.checker";

    // PRIDE Cluster consensus min of peaks.
    public static final String CONSENSUS_SPEC_MIN_PEAKS        = "pride.cluster.consensus_min_peaks";

    /**
     * This method create the algorithm with the Default Properties like defined by Spectra Cluster sequential algorithm
     *  https://github.com/spectra-cluster/spectra-cluster
     *
     * @param configuration source of parameters
     */
    public static void configureAnalysisParameters(Configuration configuration) {

        // Spectra Related properties.
        Defaults.setNumberComparedPeaks(configuration.getInt(NUMBER_COMPARED_PEAKS_PROPERTY, Defaults.DEFAULT_NUMBER_COMPARED_PEAKS));
        Defaults.setFragmentIonTolerance(configuration.getFloat(FRAGMENT_ION_TOLERANCE_PROPERTY, Defaults.DEFAULT_FRAGMENT_ION_TOLERANCE));
        Defaults.setRetainThreshold(configuration.getFloat(RETAIN_THRESHOLD_PROPERTY, (float) Defaults.DEFAULT_RETAIN_THRESHOLD));
        Defaults.setSimilarityThreshold(configuration.getFloat(SIMILARITY_THRESHOLD_PROPERTY, (float) Defaults.DEFAULT_SIMILARITY_THRESHOLD));
        Defaults.setMinNumberComparisons(configuration.getInt(CDF_MIN_NUMBER_COMPARISONS, Defaults.DEFAULT_MIN_NUMBER_COMPARISONS));
        Defaults.setDefaultConsensusMinPeaks(configuration.getInt(CONSENSUS_SPEC_MIN_PEAKS, Defaults.DEFAULT_CONSENSUS_MIN_PEAKS));

        // similarity checker - this must be created AFTER the fragmentIonTolerance property is being read
        Defaults.setDefaultSimilarityChecker(getSimilarityCheckerFromConfiguration(configuration));

        // Properties realated with the  the hadoop algorithm.
        ClusterHadoopDefaults.setMajorPeakMZWindowSize(configuration.getFloat(MAJOR_PEAK_WINDOW_PROPERTY, (float) ClusterHadoopDefaults.DEFAULT_MAJOR_PEAK_MZ_WINDOW));
        ClusterHadoopDefaults.setSpectrumMergeMZWindowSize(configuration.getFloat(SPECTRUM_MERGE_WINDOW_PROPERTY, (float) ClusterHadoopDefaults.DEFAULT_SPECTRUM_MERGE_WINDOW));
        ClusterHadoopDefaults.setEnableComparisonPeakFilter(configuration.getBoolean(ENABLE_COMPARISON_PEAK_FILTER, ClusterHadoopDefaults.DEFAULT_ENABLE_COMPARISON_PEAK_FILTER));
        ClusterHadoopDefaults.setInitialHighestPeakFilter(configuration.getInt(INITIAL_HIGHEST_PEAK_FILTER, ClusterHadoopDefaults.DEFAULT_INITIAL_HIGHEST_PEAK_FILTER));
        ClusterHadoopDefaults.setMaximumNumberOfClusters(configuration.getInt(MAXIMUM_NUMBER_CLUSTERS, ClusterHadoopDefaults.DEFAULT_MAXIMUM_NUMBER_OF_CLUSTERS));
    }

    private static ISimilarityChecker getSimilarityCheckerFromConfiguration(Configuration configuration) {
        Class similarityCheckerClass = configuration.getClass(SIMILARITY_CHECKER_PROPERTY, Defaults.getDefaultSimilarityChecker().getClass(), ISimilarityChecker.class);
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


    /**
     * used to write parameters in to a data sink like a clustering file
     *
     * @param out output
     */
    public static void appendAnalysisParameters(Appendable out) {
        try {
            out.append(NUMBER_COMPARED_PEAKS_PROPERTY).append("=").append(String.valueOf(Defaults.getNumberComparedPeaks())).append("\n");
            out.append(FRAGMENT_ION_TOLERANCE_PROPERTY).append("=").append(NumberUtilities.formatDouble(Defaults.getFragmentIonTolerance(), 3)).append("\n");
            out.append(SIMILARITY_THRESHOLD_PROPERTY).append("=").append(NumberUtilities.formatDouble(Defaults.getSimilarityThreshold(), 3)).append("\n");
            out.append(RETAIN_THRESHOLD_PROPERTY).append("=").append(NumberUtilities.formatDouble(Defaults.getRetainThreshold(), 3)).append("\n");
            out.append(MAJOR_PEAK_WINDOW_PROPERTY).append("=").append(NumberUtilities.formatDouble(ClusterHadoopDefaults.getMajorPeakMZWindowSize(), 3)).append("\n");
            out.append(SPECTRUM_MERGE_WINDOW_PROPERTY).append("=").append(NumberUtilities.formatDouble(ClusterHadoopDefaults.getSpectrumMergeMZWindowSize(), 3)).append("\n");
            out.append(CONSENSUS_SPEC_MIN_PEAKS).append("=").append(String.valueOf(Defaults.getDefaultConsensusMinPeaks())).append("\n");
            // TODO: discuss how to add the used ISimilarityChecker here
        } catch (IOException e) {
            throw new RuntimeException(e);

        }
    }
}
