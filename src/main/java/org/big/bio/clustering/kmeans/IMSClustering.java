package org.big.bio.clustering.kmeans;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

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
 * Created by ypriverol (ypriverol@gmail.com) on 27/10/2017.
 */
public interface IMSClustering {

    /**
     * Every clustering method should implement they are own parameters system.
     * For example for kmeans in the number of clusters, for agglomerate the accuracy.
     *
     * @return
     */
    public Options getCLIParameters();

    /**
     * Method to parseCommandLine the commandline options from the command
     * @param args Tools commands
     * @param options options
     * @return Commandline parser
     */
    public CommandLine parseCommandLine(String[] args, Options options) throws ParseException;
}
