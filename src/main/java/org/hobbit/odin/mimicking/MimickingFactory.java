package org.hobbit.odin.mimicking;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * Retrieves the correct mimicking algorithm type and creates a byte array of
 * the mimicking algorithm's parameters.
 * 
 * @author Kleanthi Georgala (georgala@informatik.uni-leipzig.de)
 * @version 1.0
 *
 */
public class MimickingFactory {
    protected static final Logger logger = Logger.getLogger(MimickingFactory.class.getName());

    /**
     * Factory function for retrieving a mimicking algorithm name from the set
     * of allowed types.
     *
     * @param expression,
     *            The name/type of the algorithm.
     * @return a specific mimicking algorithm type
     * @throws IOException
     *             if the type of the mimicking algorithm is invalid
     */
    public static MimickingType getMimickingType(String algorithm) throws IOException {
        return MimickingType.getMimickingType(algorithm);
    }

    /**
     * Function responsible for creating a byte array that includes the
     * appropriate input parameters for each mimicking algorithm.
     * 
     * @param type,
     *            Type of the mimicking algorithm
     * @param population,
     *            size of output events
     * @param outputType,
     *            type of output data
     * @param outputFolder,
     *            name of the output folder
     * @param seed,
     *            seed for the mimicking algorithm
     *
     * @exception IOException
     *                if the measure type is invalid
     *
     */
    public static String[] getMimickingArguments(MimickingType type, String population, String outputType,
            String outputFolder, String seed) throws IOException {

        // check if folder exists and create it if otherwise
        boolean success = false;
        File directory = new File(outputFolder);
        if (!directory.exists()) {
            success = directory.mkdir();
            if (!success) {
                logger.error("Failed to create new directory: " + outputFolder);
                throw new IOException();
            }
        }
        String[] mimickingTask = null;
        switch (type) {
        case TWIG:
            String[] twigArguments = new String[5];
            twigArguments[0] = String.valueOf(population);
            twigArguments[1] = "1";
            twigArguments[2] = "2009-09-29";
            twigArguments[3] = String.valueOf(seed);
            twigArguments[4] = outputFolder;
            mimickingTask = twigArguments;
            break;
        case TRANSPORT_DATA:
            String[] transportDataArguments = new String[2];
            transportDataArguments[0] = "GTFS_GEN_SEED=" + seed;
            transportDataArguments[1] = "GTFS_GEN_CONNECTIONS__CONNECTIONS=" + population;
            mimickingTask = transportDataArguments;
            break;
        default:
            logger.error("Wrong mimicking type: " + type.toString());
            throw new RuntimeException();
        }

        return mimickingTask;
    }
}
