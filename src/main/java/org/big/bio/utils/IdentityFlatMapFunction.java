package org.big.bio.utils;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.Serializable;
import java.util.Iterator;

/**
 * org.big.bio.utils.IdentityFlatMapFunction
 * User: Steve
 * Date: 11/17/2017
 */
public class  IdentityFlatMapFunction<T> implements FlatMapFunction<Iterator<T>,T> ,Serializable {


    @Override
    public Iterator<T> call(Iterator<T> t) throws Exception {
        return t;
    }
}
