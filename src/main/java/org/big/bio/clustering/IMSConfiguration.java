package org.big.bio.clustering;

import java.util.Properties;

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
 * Created by ypriverol (ypriverol@gmail.com) on 02/11/2017.
 */
public interface IMSConfiguration {
    /**
     * Retrieve key-value pairs for all the Algorithm parameters.
     * @return Properties.
     */
    Properties getProperties();
}
