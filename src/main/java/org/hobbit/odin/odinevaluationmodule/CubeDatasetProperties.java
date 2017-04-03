package org.hobbit.odin.odinevaluationmodule;

/**
 * Enum class that describes the valid properties of a Cube Dataset based on the
 * W3C Recommendation.
 * 
 * @author Kleanthi Georgala (georgala@informatik.uni-leipzig.de)
 * @version 1.0
 */
public enum CubeDatasetProperties {

    SUBJECT("http://purl.org/dc/terms/subject"), 
    PUBLISHER("http://purl.org/dc/terms/publisher"), 
    DATE("http://purl.org/dc/terms/issued"), 
    DESCRIPTION("http://purl.org/dc/terms/description"), 
    STRUCTURE("http://purl.org/linked-data/cube#structure"), 
    UNIT_MEASURE("http://purl.org/linked-data/sdmx/2009/attribute#unitMeasure"),

    COMPONENT("http://purl.org/linked-data/cube#component"), 
    DIMENSION("http://purl.org/linked-data/cube#dimension"), 
    DIMENSION_PROPERTY("http://purl.org/linked-data/cube#DimensionProperty"), 
    MEASURE("http://purl.org/linked-data/cube#measure"), 
    MEASURE_PROPERTY("http://purl.org/linked-data/cube#MeasureProperty"), 
    ATTRIBUTE("http://purl.org/linked-data/cube#attribute"), 
    REQUIRED("http://purl.org/linked-data/cube#componentRequired"), 
    ATTACHMENT("http://purl.org/linked-data/cube#componentAttachment");

    private String property;

    CubeDatasetProperties(String pro) {
        this.property = pro;
    }

    public String getPropertyURI() {
        return this.property;
    }
}
