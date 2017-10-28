package org.big.bio.clustering.kmeans;

import org.apache.commons.cli.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.big.bio.utils.SparkUtil;

/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 * <p>
 * General class that contains the basic information for any implementation or Clustering method.
 * This class create for example the IMSClustering instance that will contain the spark context amount
 * other variables.
 *
 * <p>
 * Created by ypriverol (ypriverol@gmail.com) on 27/10/2017.
 */
public class MSClustering implements IMSClustering{

    public JavaSparkContext context;

    public static String APPLICATION_NAME  = "SparkMLClusteringKMeans";

    public MSClustering(){
        context = SparkUtil.createJavaSparkContext(APPLICATION_NAME, "local[*]");
    }

    public MSClustering(String confFile){
        context = SparkUtil.createJavaSparkContextWithFile(APPLICATION_NAME, confFile);
    }

    /**
     * Default parameters should be an input file and evey implementation should provide
     * a set of default parameters for the running of the algorithm.
     * @return CommandLine Options
     */

    public static Options getCLIParameters() {
        Options defaultOptions = new Options();
        Option optionFile = new Option("i", "input-path", true, "Input Path containing all the mass spectra");
        Option optionConf = new Option("c", "conf", true, "Configuration file for the spark application");
        defaultOptions.addOption(optionFile);
        defaultOptions.addOption(optionConf);
        return defaultOptions;
    }

    /**
     * This function provides the mechanism to parse the commandline options .
     * @param args Tools commands
     * @param options options
     * @return
     * @throws ParseException
     */
    public static CommandLine parseCommandLine(String[] args, Options options) throws ParseException {
        CommandLineParser parser = new PosixParser();
        return parser.parse(options, args);
    }

    /**
     * Print the commandline options
     */
    public static void printHelpCommands(){
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(APPLICATION_NAME, getCLIParameters());
    }


}
