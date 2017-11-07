package org.big.bio.transformers;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.big.bio.clustering.pride.PRIDEClusterDefaultParameters;
import org.big.bio.keys.MZKey;
import scala.Tuple2;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.normalizer.IIntensityNormalizer;
import uk.ac.ebi.pride.spectracluster.spectrum.IPeak;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;
import uk.ac.ebi.pride.spectracluster.spectrum.Spectrum;
import uk.ac.ebi.pride.spectracluster.util.ClusterUtilities;
import uk.ac.ebi.pride.spectracluster.util.Defaults;
import uk.ac.ebi.pride.spectracluster.util.MZIntensityUtilities;
import uk.ac.ebi.pride.spectracluster.util.binner.IWideBinner;
import uk.ac.ebi.pride.spectracluster.util.binner.SizedWideBinner;
import uk.ac.ebi.pride.spectracluster.util.function.Functions;
import uk.ac.ebi.pride.spectracluster.util.function.IFunction;
import uk.ac.ebi.pride.spectracluster.util.function.peak.HighestNPeakFunction;
import uk.ac.ebi.pride.spectracluster.util.function.spectrum.RemoveImpossiblyHighPeaksFunction;
import uk.ac.ebi.pride.spectracluster.util.function.spectrum.RemoveIonContaminantsPeaksFunction;
import uk.ac.ebi.pride.spectracluster.util.function.spectrum.RemovePrecursorPeaksFunction;
import uk.ac.ebi.pride.spectracluster.util.function.spectrum.RemoveWindowPeaksFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 * <p>
 * This class enable the mapping from Spectrum to Cluster it includes some pre-processing
 * steps to remove un-wanted
 * <p>
 * Created by Yasset Perez-Riverol  (ypriverol@gmail.com) on 01/11/2017.
 */
public class SpectrumToInitialClusterTransformer implements PairFlatMapFunction<Tuple2<String, ISpectrum>, MZKey, ICluster> {

    private static final double BIN_OVERLAP = 0;

    private static final float DEFAULT_BIN_WIDTH = 4F;

    private static final boolean OVERFLOW_BINS = true;

    private static final double LOWEST_MZ = 0;

    private IWideBinner binner;

    public static final String WINDOW_SIZE_PROPERTY = "mapper.window_size";

    private IFunction<List<IPeak>, List<IPeak>> peakFilter;

    /**
     * Reuse normalizer
     */
    private IIntensityNormalizer intensityNormalizer = Defaults.getDefaultIntensityNormalizer();
    private IFunction<ISpectrum, ISpectrum> initialSpectrumFilter =  Functions.join(
            new RemoveImpossiblyHighPeaksFunction(),
            new RemovePrecursorPeaksFunction(Defaults.getFragmentIonTolerance()),
            new RemoveIonContaminantsPeaksFunction(Defaults.getFragmentIonTolerance()),
            new RemoveWindowPeaksFunction(150F, Float.MAX_VALUE));


    /**
     * This Constructor define the parameters to do the transformation from ISpectrum to Cluster.
     * It defines the bin With for all the spectra.
     *
     * @param context Java Spark Context
     */
    public SpectrumToInitialClusterTransformer(JavaSparkContext context){

        // Read the Bin from the configuration file.
        double binWidth = context.hadoopConfiguration().getFloat(WINDOW_SIZE_PROPERTY, DEFAULT_BIN_WIDTH);

        binner = new SizedWideBinner(MZIntensityUtilities.HIGHEST_USABLE_MZ, binWidth, LOWEST_MZ, BIN_OVERLAP, OVERFLOW_BINS);

        //context.getCounter("bin-width", String.valueOf(binWidth)).increment(1);

        // only keep the N (default = 150) highest peaks per spectrum
        peakFilter = new HighestNPeakFunction(PRIDEClusterDefaultParameters.DEFAULT_INITIAL_HIGHEST_PEAK_FILTER);


    }

    /**
     * Transform the Spectrum into ICluster with the corresponding
     * @param spectrumTuple Spectrum Tuple.
     * @return MZKey - ICluster Maps.
     * @throws Exception
     */
    @Override
    public Iterator<Tuple2<MZKey, ICluster>> call(Tuple2<String, ISpectrum> spectrumTuple) throws Exception {

        ISpectrum spectrum = spectrumTuple._2();

        float precursorMz = spectrum.getPrecursorMz();

        List<Tuple2<MZKey, ICluster>> ret = new ArrayList<>();

        if (precursorMz < MZIntensityUtilities.HIGHEST_USABLE_MZ) {
            // increment dalton bin counter
            // context.getCounter("normal precursor", "spectra < " + String.valueOf(MZIntensityUtilities.HIGHEST_USABLE_MZ)).increment(1);

            // do initial filtering (ie. precursor removal, impossible high peaks, etc.)
            ISpectrum filteredSpectrum = initialSpectrumFilter.apply(spectrum);

            ISpectrum normalisedSpectrum = normaliseSpectrum(filteredSpectrum);

            // only retain the signal peaks (default = 150 highest peaks)
            ISpectrum reducedSpectrum = new Spectrum(filteredSpectrum, peakFilter.apply(normalisedSpectrum.getPeaks()));

            // generate a new cluster
            ICluster cluster = ClusterUtilities.asCluster(reducedSpectrum);

            // get the bin(s)
            int[] bins = binner.asBins(cluster.getPrecursorMz());

            // make sure the spectrum is only placed in a single bin since overlaps cannot happen in this config
            if (bins.length != 1) {
                throw new InterruptedException("This implementation only works if now overlap is set during binning.");
            }

//            // save the bin as a property for further use
//            cluster.setProperty(HadoopClusterProperties.SPECTRUM_TO_CLUSTER_BIN, String.valueOf(bins[0]));

            // This is really important because, here is when the cluster ID Is generated. We can explored the
            // idea in the future to generate the ID based on the Spectrum Cluster Peak List.
            cluster.setId(UUID.randomUUID().toString());

            // output cluster
            MZKey mzKey = new MZKey(precursorMz);

//            keyOutputText.set(mzKey.toString());
//            //Spectrum to Cluster Annotation.
//            valueOutputText.set(IOUtilities.convertClusterToCGFString(cluster));
//            context.write(keyOutputText, valueOutputText);
//        }
//        else {
//            // count the number of spectra with an impossibly high precursor
//            context.getCounter("high precursor", "spectra > " +
//                    String.valueOf(MZIntensityUtilities.HIGHEST_USABLE_MZ)).increment(1);
//        }

            ret.add(new Tuple2<>(mzKey, cluster));
        }
        return ret.iterator();

    }

    /**
     * This method normalize an Spectrum
     * @param filteredSpectrum spectrum to be Normalized
     * @return Normalized Spectrum
     */
    private ISpectrum normaliseSpectrum(ISpectrum filteredSpectrum) {
        List<IPeak> normalizedPeaks = intensityNormalizer.normalizePeaks(filteredSpectrum.getPeaks());
        return new Spectrum(filteredSpectrum, normalizedPeaks);
    }
}
