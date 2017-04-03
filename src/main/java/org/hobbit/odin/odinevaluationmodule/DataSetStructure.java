package org.hobbit.odin.odinevaluationmodule;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;

/**
 * Enum class that describes the KPIs of each SELECT task.
 * 
 * @author Kleanthi Georgala (georgala@informatik.uni-leipzig.de)
 * @version 1.0
 *
 */
public enum DataSetStructure {
    RECALL, PRECISION, FMEASURE, TPS, DELAY;

    private String label;
    private String description;
    private String kpiProperty;
    private String structure;
    private String unitMeasure;
    private String measure;

    public static final String subject = "http://purl.org/linked-data/sdmx/2009/subject#2.9";
    public static final String publisher = "Kleanthi Georgala for HOBBIT";
    public static final String date = DateTimeFormatter.ofPattern("yyyy/MM/dd").format(LocalDate.now());
    public static final String datasetStrucute = "http://purl.org/linked-data/cube#DataStructureDefinition";
    public static final String dimension = "http://w3id.org/hobbit/experiments#taskID";
    public static final String unitMeasureObject = "http://purl.org/linked-data/sdmx/2009/attribute#unitMeasure";
    public static final String dataset = "http://purl.org/linked-data/cube#DataSet";
    public static final String observation = "http://purl.org/linked-data/cube#Observation";

    //////////////////////////////////////////////////////
    static {
        RECALL.kpiProperty = "http://w3id.org/bench#tasksRecall";
        RECALL.label = "Recall";
        RECALL.description = "Detailed evaluation of Recall performance for each SELECT SPARQL query";
        RECALL.structure = "http://w3id.org/hobbit/experiments#RecallStructure";
        RECALL.unitMeasure = "http://dbpedia.org/resource/Recall";
        RECALL.measure = "http://w3id.org/bench#recall";

        PRECISION.kpiProperty = "http://w3id.org/bench#tasksPrecision";
        PRECISION.label = "Precision";
        PRECISION.description = "Detailed evaluation of Precision performance for each SELECT SPARQL query";
        PRECISION.structure = "http://w3id.org/hobbit/experiments#PrecisionStructure";
        PRECISION.unitMeasure = "http://dbpedia.org/resource/Precision";
        PRECISION.measure = "http://w3id.org/bench#precision";

        FMEASURE.kpiProperty = "http://w3id.org/bench#tasksFmeasure";
        FMEASURE.label = "Fmeasure";
        FMEASURE.description = "Detailed evaluation of Fmeasure performance for each SELECT SPARQL query";
        FMEASURE.structure = "http://w3id.org/hobbit/experiments#FmeasureStructure";
        FMEASURE.unitMeasure = "http://dbpedia.org/resource/F1_score";
        FMEASURE.measure = "http://w3id.org/bench#fmeasure";

        TPS.kpiProperty = "http://w3id.org/bench#tasksTPS";
        TPS.label = "TPS";
        TPS.description = "Detailed evaluation of TPS for each SELECT SPARQL query";
        TPS.structure = "http://w3id.org/hobbit/experiments#TPSStructure";
        TPS.unitMeasure = "http://dbpedia.org/resource/Query_throughput";
        TPS.measure = "http://w3id.org/bench#tps";

        DELAY.kpiProperty = "http://w3id.org/bench#tasksAnswerDelay";
        DELAY.label = "Delay";
        DELAY.description = "Detailed evaluation of Task Delay for each SELECT SPARQL query";
        DELAY.structure = "http://w3id.org/hobbit/experiments#AnswerDelayStructure";
        DELAY.unitMeasure = "http://dbpedia.org/resource/Second";
        DELAY.measure = "http://w3id.org/bench#Second";

    }

    public String getLabel() {
        return label;
    }

    public String getDescription() {
        return description;
    }

    public String getKpiProperty() {
        return kpiProperty;
    }

    public String getStructure() {
        return structure;
    }

    public String getUnitMeasure() {
        return unitMeasure;
    }

    public String getMeasure() {
        return measure;
    }

    /**
     * Creates and returns a unique Resource of the particular KPI of a
     * particular experiment.
     * 
     * @param key,
     *            the experiment key
     * @return a String representation of the Resource URI
     */
    public String getDatasetResource(String key) {
        String resourceURI = "http://w3id.org/hobbit/experiments#" + this.label + "_Dataset_for_" + key.split("#")[1];
        return resourceURI;
    }

    /**
     * Creates a model that describes the Dimension Property of a Cube Dataset.
     * 
     * @param id,
     *            the Resource of the Dimension component.
     * @return the model of the Dimension component
     */
    public static Model defineModelDimension(Resource id) {
        Model model = ModelFactory.createDefaultModel();

        // exp:taskID a rdf:Property, qb:DimensionProperty;
        Resource dimProp = model.createResource(CubeDatasetProperties.DIMENSION_PROPERTY.getPropertyURI());
        model.add(id, RDF.type, RDF.Property);
        model.add(id, RDF.type, dimProp);

        // rdfs:label "Task ID"@en;
        model.add(id, RDFS.label, model.createLiteral(new String("Task ID"), "en"));

        // rdfs:range xsd:unsignedInt;
        model.add(id, RDFS.range, XSD.unsignedInt);

        // qb:concept sdmx-concept:completeness
        model.add(id, model.createProperty("http://purl.org/linked-data/cube#concept"),
                model.createResource("http://purl.org/linked-data/sdmx/2009/concept#completeness"));

        return model;
    }

    /**
     * Creates a model that describes the Measure Property of a Cube Dataset.
     * 
     * @param m,
     *            the Resource of the Measure component.
     * @return the model of the Measure component
     */
    public Model defineModelMeasure(Resource m) {
        Model model = ModelFactory.createDefaultModel();

        // bench:recall a qb:MeasureProperty;
        model.add(m, RDF.type, CubeDatasetProperties.MEASURE_PROPERTY.getPropertyURI());

        // rdfs:label "Recall"@en;
        model.add(m, RDFS.label, model.createLiteral(new String(this.label), "en"));

        // rdfs:comment "Recall = TP / (TP + FN)"@en;
        model.add(m, RDFS.comment, model.createLiteral(new String(this.description), "en"));

        // rdfs:range xsd:double .
        if (this.label.equals("Delay"))
            model.add(m, RDFS.range, XSD.xlong);
        else
            model.add(m, RDFS.range, XSD.xdouble);

        return model;
    }

}
