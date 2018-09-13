package org.hobbit.odin.util;

/**
 * Enum class that includes all valid time stamps properties for each mimicking
 * algorithm data.
 * 
 * @author Kleanthi Georgala (georgala@informatik.uni-leipzig.de)
 * @version 1.0
 *
 */
public enum TimeStampProperty {
    TRANSPORT_DATA_TIMESTAMP("http://semweb.mmlab.be/ns/linkedconnections#departureTime"), TWIG_TIMESTAMP(
            "http://aksw.org/twig#tweetTime"), TT_DATA_TIMESTAMP(
                    "http://www.tomtom.com/ontologies/traces#hasTimestamp"), OBS_DATA_TIMESTAMP(
                            "http://purl.oclc.org/NET/ssnx/ssn#observationResultTime\thttp://www.agtinternational.com/ontologies/IoTCore#valueLiteral");
    private String timeStampProperty;

    TimeStampProperty(String mc) {
        this.timeStampProperty = mc;
    }

    public String timeStampProperty() {
        return this.timeStampProperty;
    }
}
