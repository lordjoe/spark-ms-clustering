package org.big.bio.clustering.output;

import org.big.bio.clustering.IMSClustering;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;

import java.io.Serializable;

/**
 * org.big.bio.clustering.pride.IClusterWriter
 * User: Steve
 * Date: 11/13/2017
 */                         
public interface IClusterWriter extends Serializable {

    /**
     * how to convert to a string
     * @return
     */
    public IClusterConverter getConverter();
      /**
     * write the start of a file
     * @param out where to append
     * @param app appliaction data
     */
    public void appendHeader( Appendable out,IMSClustering app) ;

    /**
     * write the end of a file
     * @param out where to append
     * @param app appliaction data
     */
    public void appendFooter( Appendable out,IMSClustering app) ;


    /**
     * write one scan
     * @param out where to append
     * @param app appliaction data
     * @param scan one scan
     */
    public void appendScan( Appendable out,IMSClustering app,ICluster scan) ;

}
