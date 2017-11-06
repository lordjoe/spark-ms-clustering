package org.big.bio.transformers;

import org.apache.spark.api.java.function.Function;
import org.big.bio.keys.BinMZKey;
import org.big.bio.utils.IOUtilities;
import scala.Tuple2;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;

/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 * <p>
 * This class
 * <p>
 * Created by ypriverol (ypriverol@gmail.com) on 06/11/2017.
 */
public class IterableClustersToStringTransformer implements Function<Tuple2<BinMZKey, Iterable<ICluster>>, String> {

    @Override
    public String call(Tuple2<BinMZKey, Iterable<ICluster>> binMZKeyIterableTuple2) throws Exception {
        StringBuilder builder = new StringBuilder();
        binMZKeyIterableTuple2._2().forEach( cluster ->{
            builder.append(IOUtilities.convertClusterToCGFString(cluster));
        });
        return builder.toString();
    }
}