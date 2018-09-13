package org.hobbit.odin.mimicking;

import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * Mimicking type class. Returns the name, the docker image (if applicable) and
 * the execute command for each mimicking algorithm.
 * 
 * @author Kleanthi Georgala (georgala@informatik.uni-leipzig.de)
 * @version 1.0
 *
 */
public enum MimickingType {

    TWIG, TRANSPORT_DATA, TT, OBS;

    private String executeCommand;
    private String mimickingName;
    private String outputType;
    protected static final Logger logger = Logger.getLogger(MimickingType.class.getName());

    static {

        TWIG.mimickingName = "TWIG";
        TWIG.executeCommand = "/odin/download.sh";
        TWIG.outputType = "rdf";

        TRANSPORT_DATA.mimickingName = "TRANSPORT_DATA";
        TRANSPORT_DATA.executeCommand = "podigg/podigg-lc-hobbit";
        TRANSPORT_DATA.outputType = "rdf";
        
        TT.mimickingName = "TT";
        TT.executeCommand = "git.project-hobbit.eu:4567/filipe.teixeira/synthetic-trace-generator:latest";
        TT.outputType = "rdf";
        
        OBS.mimickingName = "OBS";
        OBS.executeCommand = "git.project-hobbit.eu:4567/smirnp/sml-v1-mimicking-datagen:latest";
        OBS.outputType = "rdf";
    }

    /* Getters */
    public String getMimickingName() {
        return mimickingName;
    }

    public String getExecuteCommand() {
        return executeCommand;

    }

    public String getOutputType() {
        return outputType;
    }

    /**
     * Factory function that returns the correct mimicking type given a name.
     * 
     * @param name,
     *            the name of the mimicking algorithm
     * @return the corresponding mimicking algorithm type
     * @throws IOException
     *             if the input name does not correspond to any valid mimicking
     *             algorithm
     */
    public static MimickingType getMimickingType(String name) throws IOException {
        switch (name) {
        case ("TWIG"):
            return TWIG;
        case ("TRANSPORT_DATA"):
            return TRANSPORT_DATA;
        case ("TT"):
            return TT;
        case ("OBS"):
            return OBS;
        default:
            logger.error("Unknown mimicking algorithm: " + name);
            throw new IOException();
        }

    }

}
