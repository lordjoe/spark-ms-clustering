package org.big.bio.clustering.output;

import org.big.bio.utils.IOUtilities;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;

import java.io.Serializable;

/**
 * org.big.bio.clustering.pride.IClusterConverter
 * User: Steve
 * Date: 11/13/2017
 */
public interface IClusterConverter extends Serializable {
    /**
     * make a cluster into a string to append
     * @param cluster
     * @return
     */
    public String convert(ICluster cluster);

}
