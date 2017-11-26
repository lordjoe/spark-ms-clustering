package org.big.bio.clustering.output;

import org.big.bio.utils.IOUtilities;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;

/**
 * org.big.bio.clustering.pride.CGFClusterConverter
 * User: Steve
 * Date: 11/13/2017
 */
public class CGFClusterConverter implements IClusterConverter{
    public String convert(ICluster cluster)  {
        return IOUtilities.convertClusterToCGFString(cluster);
    }
}
