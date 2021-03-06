package org.hobbit.odin.util;

/**
 * Enum class that includes all valid event main classes for each mimicking
 * algorithm data.
 * 
 * @author Kleanthi Georgala (georgala@informatik.uni-leipzig.de)
 * @version 1.0
 *
 */
public enum MainClassProperty {

    TRANSPORT_DATA_MAINCLASS("http://semweb.mmlab.be/ns/linkedconnections#Connection"), TWIG_MAINCLASS(
            "http://aksw.org/twig#Tweet"), TT_DATA_MAINCLASS(
                    "http://www.tomtom.com/ontologies/traces#hasPoint"), OBS_DATA_MAINCLASS(
                            "http://www.agtinternational.com/ontologies/I4.0#MoldingMachineObservationGroup");

    private String mainClassProperty;

    MainClassProperty(String mc) {
        this.mainClassProperty = mc;
    }

    public String mainClassProperty() {
        return this.mainClassProperty;
    }

}
