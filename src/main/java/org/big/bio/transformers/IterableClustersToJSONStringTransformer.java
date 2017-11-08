package org.big.bio.transformers;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.big.bio.keys.BinMZKey;
import org.big.bio.utils.IOUtilities;
import scala.Tuple2;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
 * Created by ypriverol (ypriverol@gmail.com) on 08/11/2017.
 */
public class IterableClustersToJSONStringTransformer implements PairFlatMapFunction<Tuple2<BinMZKey, Iterable<ICluster>>, String, String> {

    @Override
    public Iterator<Tuple2<String, String>> call(Tuple2<BinMZKey, Iterable<ICluster>> binMZKeyIterableTuple2) throws Exception {
        List<Tuple2<String, String>> re = new ArrayList<>();
        binMZKeyIterableTuple2._2().forEach( cluster -> re.add(new Tuple2<>(cluster.getId(), IOUtilities.convertClusterToJSONClusteringString(cluster))));
        return re.iterator();
    }
}
