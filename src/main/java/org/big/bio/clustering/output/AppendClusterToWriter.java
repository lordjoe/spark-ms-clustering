package org.big.bio.clustering.output;

import com.lordjoe.distributed.spark.accumulators.AbstractLoggingFunction;
import org.big.bio.clustering.MSClustering;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;

/**
 * org.big.bio.clustering.pride.AppendClusterToWriter
 * User: Steve
 * Date: 11/13/2017
 */
public  class AppendClusterToWriter<T extends ICluster> extends AbstractLoggingFunction<T, String> {
    private final IClusterWriter writer;
    private final MSClustering application;

    public AppendClusterToWriter(final IClusterWriter pWriter, MSClustering app) {
        writer = pWriter;
        application = app;
    }



    @Override
    public String doCall(final  T  v1) throws Exception {
        StringBuilder sb = new StringBuilder();
        writer.appendScan(sb, application, v1);
        return sb.toString();
    }

}
