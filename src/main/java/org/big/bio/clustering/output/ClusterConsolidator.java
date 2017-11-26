package org.big.bio.clustering.output;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.big.bio.clustering.MSClustering;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * org.big.bio.clustering.pride.CDFClusterConsolidator
 * User: Steve
 * Date: 11/13/2017
 */
public class  ClusterConsolidator   {

    private final IClusterWriter writer;
    private final MSClustering application;
    private final String outFile;

    public ClusterConsolidator(final IClusterWriter pWriter, final MSClustering pApplication,String pOutFile) {
        writer = pWriter;
        application = pApplication;
        outFile = pOutFile;

    }

    public IClusterWriter getWriter() {
        return writer;
    }


    public MSClustering getApplication() {
        return application;
    }

    /**
     * write scores into a file
     *
     * @param scans
     */
    public   int writeScores(JavaRDD<ICluster> scans) {

        scans = sortByIndex(scans);

        List<String> header = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();
        writer.appendHeader(sb, null);
        header.add(sb.toString());
        sb.setLength(0);
        //header.add(out.toString());
         JavaRDD<String> headerRDD = SparkUtilities.getCurrentContext().parallelize(header);

        List<String> footer = new ArrayList<String>();
        writer.appendFooter(sb, null);
        footer.add(sb.toString());
        sb.setLength(0);
        long[] scoreCounts = new long[1];

         JavaRDD<String> footerRDD = SparkUtilities.getCurrentContext().parallelize(footer);

        // make an RDD of the text for every Spectrum
        JavaRDD<String> textOut = scans.map(new AppendClusterToWriter(writer,getApplication()));

         JavaRDD<String> data = headerRDD.union(textOut).union(footerRDD).coalesce(1);



          Path path = new Path(outFile);

        SparkFileSaver.saveAsFile(path, textOut, header.toString(), footer.toString());

        return (int)scoreCounts[0];
    }


    public static  <T extends ICluster> JavaRDD<T> sortByIndex(JavaRDD<T> bestScores)
    {
        throw new UnsupportedOperationException("Fix This"); // ToDo
//        JavaPairRDD<String, T> byIndex = bestScores.mapToPair(new ToIndexTuple());
//        JavaPairRDD<String, T> sortedByIndex = byIndex.sortByKey();
//
//        return sortedByIndex.values();
//        //     return byIndex.values();

    }

}
