package org.big.bio.io;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import uk.ac.ebi.pride.spectracluster.io.ParserUtilities;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;

import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Convert Text MGF Structure to ISpectrum objects.
 *
 * @author ypriverol
 */
public class MGFStringTupleToSpectrumTuple implements PairFlatMapFunction<Tuple2<Text, Text>, String, ISpectrum> {

    public MGFStringTupleToSpectrumTuple() {

    }

    @Override
    public Iterator<Tuple2<String, ISpectrum>> call(final Tuple2<Text, Text> kv) throws Exception {
        List<Tuple2<String, ISpectrum>> ret = new ArrayList<>();
        LineNumberReader inp = new LineNumberReader(new StringReader(kv._2.toString()));
        ISpectrum spectrum = ParserUtilities.readMGFScan(inp);
        ret.add(new Tuple2<>(spectrum.getId(), spectrum));
        return ret.iterator();
    }
}
