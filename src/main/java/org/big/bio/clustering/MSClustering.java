package org.big.bio.clustering;

import org.apache.commons.cli.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.big.bio.utils.SparkUtil;

import java.io.IOException;
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
 * General class that contains the basic information for any implementation or Clustering method.
 * This class create for example the IMSClustering instance that will contain the spark context amount
 * other variables.
 *
 * <p>
 * Created by Yasset Perez-Riverol (ypriverol@gmail.com) on 27/10/2017.
 */
public class MSClustering implements IMSClustering {

    // The JavaSpark Context for the corresponding algorithm implementation
    private final JavaSparkContext context;

    // Java Application Name for the Spark Application
    public static String APPLICATION_NAME  = "SparkClustering";

    /**
     * Default Spark Constructor, it runs in local[*]
     */
    public MSClustering(){
        context = SparkUtil.createJavaSparkContext(APPLICATION_NAME, "local[*]");
    }

    /**
     * Default Constructor using the confFile and default parameters.
     * @param confFile configure File
     * @param defaultProperties default Properties
     */
    public MSClustering(String applicationName, String confFile, Properties defaultProperties) throws IOException {
        context = SparkUtil.createJavaSparkContextWithFile(applicationName, confFile, defaultProperties);
    }

    /**
     * The Constructor using the confile for the Spark Application
     * @param confFile Configuration File
     */
    public MSClustering(String confFile){
        context = SparkUtil.createJavaSparkContextWithFile(APPLICATION_NAME, confFile);
    }

    /**
     * The method to retrieve the JavaSpark Context for the algorithm.
     * @return Return the Spark Context
     */
    @Override
    public JavaSparkContext context(){
        return context;
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
        Option optionOutput = new Option("o", "output-path", true, "Output Path to write all the clusters");
        Option fileoutput = new Option("f", "output-method", true, "Output to file as opposed to directory");
        defaultOptions.addOption(optionFile);
        defaultOptions.addOption(optionConf);
        defaultOptions.addOption(optionOutput);
        defaultOptions.addOption(fileoutput);
        return defaultOptions;
    }

    /**
     * This function provides the mechanism to parse the commandline options .
     * @param args Tools commands
     * @param options options
     * @return The CommandLine with the options parsed
     * @throws ParseException Error parsing the arguments
     */
    public static CommandLine parseCommandLine(String[] args, Options options) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        return parser.parse(options, args);
    }

    /**
     * Print the commandline options
     */
    public static void printHelpCommands(){
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(APPLICATION_NAME, getCLIParameters());
    }

    /**
     * This method return the current value for an specific parameter.
     * for example
     * @param key property key (name)
     * @return value of the property.
     */
    @Override
    public String getProperty(String key) {
        return context.hadoopConfiguration().get(key);
    }
}
