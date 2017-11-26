package org.big.bio.clustering.output;

import org.big.bio.clustering.IMSClustering;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;

import java.io.IOException;

/**
 * org.big.bio.clustering.output.TestClusterWriter
 * User: Steve
 * Date: 11/14/2017
 */
public class TestClusterWriter implements IClusterWriter {

    public final IClusterConverter converter;

    public TestClusterWriter(IClusterConverter converter) {
        this.converter = converter;
    }

    @Override
    public IClusterConverter getConverter() {
        return converter;
    }

    @Override
    public void appendHeader(Appendable out, IMSClustering app) {

    }

    @Override
    public void appendFooter(Appendable out, IMSClustering app) {

    }

    @Override
    public void appendScan(Appendable out, IMSClustering app, ICluster scan) {
         String representation = getConverter().convert(scan);
        try {
            out.append(representation);
            out.append("\n");
        } catch (IOException e) {
            throw new RuntimeException(e);

        }
    }
}
