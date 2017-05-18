package org.hobbit.odin.odinevaluationmodule;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.hobbit.core.Constants;
import org.hobbit.core.components.AbstractEvaluationModule;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.odin.util.OdinConstants;
import org.hobbit.vocab.HOBBIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Odin Evaluation Module class. This class is responsible for evaluation each
 * task send to the system adapter, store the results, summarize the evaluation
 * of an experiment and of each task individually.
 * 
 * @author Kleanthi Georgala (georgala@informatik.uni-leipzig.de)
 * @version 1.0
 *
 */
public class OdinEvaluationModule extends AbstractEvaluationModule {
    private static final Logger LOGGER = LoggerFactory.getLogger(OdinEvaluationModule.class);

    /* Final evaluation model */
    private Model finalModel = ModelFactory.createDefaultModel();
    /* Number of total true positives */
    private int totalTruePositives = 0;
    /* Number of total fasl positives */
    private int totalFalsePositives = 0;
    /* Number of total false negatives */
    private int totalFalseNegatives = 0;

    /* Number of total recall */
    private double sumRecall = 0;
    /* Number of total precision */
    private double sumPrecision = 0;
    /* Maximum triples-per-seconds achieved by the triple store */
    private double maxTPS = 0;
    /*
     * Number of triples inserted into a triple store before recall felt below 1
     */
    private double sumStreamModel = 0;
    /*
     * Sum of stream intervals before recall felt below 1
     */
    private double sumStreamInterval = 0;
    /*
     * Total delay between sent and received task before recall felt below 1
     */
    private double sumTaskDelay = 0;
    /* Number of tasks */
    private int taskCounter = 0;
    /*
     * Map with keys the unique time stamps that tasks were sent to the System
     * Adapter and as values the corresponding tasks
     */
    private TreeMap<Long, ArrayList<TaskEvaluation>> tasks = new TreeMap<Long, ArrayList<TaskEvaluation>>();

    /* Property for micro-average-recall */
    private Property EVALUATION_MICRO_AVERAGE_RECALL = null;
    /* Property for for micro-average-precision */
    private Property EVALUATION_MICRO_AVERAGE_PRECISION = null;
    /* Property for micro-average-fmeasure */
    private Property EVALUATION_MICRO_AVERAGE_FMEASURE = null;

    /* Property for macro-average-recall */
    private Property EVALUATION_MACRO_AVERAGE_RECALL = null;
    /* Property for macro-average-precision */
    private Property EVALUATION_MACRO_AVERAGE_PRECISION = null;
    /* Property for macro-average-fmeasure */
    private Property EVALUATION_MACRO_AVERAGE_FMEASURE = null;

    /* Property for maximum triples-per-seconds */
    private Property EVALUATION_MAX_TPS = null;
    /* Property for average triples-per-seconds */
    private Property EVALUATION_AVERAGE_TPS = null;

    /* Property for task recall Cube Dataset */
    private Property EVALUATION_TASKS_EVALUATION_RECALL = null;
    /* Property for task precision Cube Dataset */
    private Property EVALUATION_TASKS_EVALUATION_PRECISION = null;
    /* Property for task fmeasure Cube Dataset */
    private Property EVALUATION_TASKS_EVALUATION_FMEASURE = null;
    /* Property for task delay Cube Dataset */
    private Property EVALUATION_AVERAGE_TASK_DELAY = null;
    /* Property for task triples-per-seconds Cube Dataset */
    private Property EVALUATION_TASKS_EVALUATION_TPS = null;

    /* Setters and Getters */
    public TreeMap<Long, ArrayList<TaskEvaluation>> getTasks() {
        return tasks;
    }

    public void setTasks(TreeMap<Long, ArrayList<TaskEvaluation>> tasks) {
        this.tasks = tasks;
    }

    public double getSumStreamModel() {
        return sumStreamModel;
    }

    public void setSumStreamModel(double sumStreamModel) {
        this.sumStreamModel = sumStreamModel;
    }

    public double getSumStreamInterval() {
        return sumStreamInterval;
    }

    public void setSumStreamInterval(double sumStreamInterval) {
        this.sumStreamInterval = sumStreamInterval;
    }

    public double getSumPrecision() {
        return sumPrecision;
    }

    public void setSumPrecision(double sumPrecision) {
        this.sumPrecision = sumPrecision;
    }

    public double getSumRecall() {
        return sumRecall;
    }

    public void setSumRecall(double sumRecall) {
        this.sumRecall = sumRecall;
    }

    public int getTotalFalseNegatives() {
        return totalFalseNegatives;
    }

    public void setTotalFalseNegatives(int totalFalseNegatives) {
        this.totalFalseNegatives = totalFalseNegatives;
    }

    public int getTotalFalsePositives() {
        return totalFalsePositives;
    }

    public void setTotalFalsePositives(int totalFalsePositives) {
        this.totalFalsePositives = totalFalsePositives;
    }

    public int getTotalTruePositives() {
        return totalTruePositives;
    }

    public void setTotalTruePositives(int totalTruePositives) {
        this.totalTruePositives = totalTruePositives;
    }

    public double getSumTaskDelay() {
        return sumTaskDelay;
    }

    public void setSumTaskDelay(double sumTaskDelay) {
        this.sumTaskDelay = sumTaskDelay;
    }

    public int getTaskCounter() {
        return taskCounter;
    }

    public void setTaskCounter(int taskCounter) {
        this.taskCounter = taskCounter;
    }

    public Property getEVALUATION_TASKS_EVALUATION_TPS() {
        return EVALUATION_TASKS_EVALUATION_TPS;
    }

    public void setEVALUATION_TASKS_EVALUATION_TPS(Property eVALUATION_TASKS_EVALUATION_TPS) {
        EVALUATION_TASKS_EVALUATION_TPS = eVALUATION_TASKS_EVALUATION_TPS;
    }

    private Property EVALUATION_TASKS_EVALUATION_DELAY = null;

    public Property getEVALUATION_TASKS_EVALUATION_DELAY() {
        return EVALUATION_TASKS_EVALUATION_DELAY;
    }

    public void setEVALUATION_TASKS_EVALUATION_DELAY(Property eVALUATION_TASKS_EVALUATION_DELAY) {
        EVALUATION_TASKS_EVALUATION_DELAY = eVALUATION_TASKS_EVALUATION_DELAY;
    }

    public Property getEVALUATION_TASKS_EVALUATION_RECALL() {
        return EVALUATION_TASKS_EVALUATION_RECALL;
    }

    public void setEVALUATION_TASKS_EVALUATION_RECALL(Property eVALUATION_TASKS_EVALUATION_RECALL) {
        EVALUATION_TASKS_EVALUATION_RECALL = eVALUATION_TASKS_EVALUATION_RECALL;
    }

    public Property getEVALUATION_TASKS_EVALUATION_PRECISION() {
        return EVALUATION_TASKS_EVALUATION_PRECISION;
    }

    public void setEVALUATION_TASKS_EVALUATION_PRECISION(Property eVALUATION_TASKS_EVALUATION_PRECISION) {
        EVALUATION_TASKS_EVALUATION_PRECISION = eVALUATION_TASKS_EVALUATION_PRECISION;
    }

    public Property getEVALUATION_TASKS_EVALUATION_FMEASURE() {
        return EVALUATION_TASKS_EVALUATION_FMEASURE;
    }

    public void setEVALUATION_TASKS_EVALUATION_FMEASURE(Property eVALUATION_TASKS_EVALUATION_FMEASURE) {
        EVALUATION_TASKS_EVALUATION_FMEASURE = eVALUATION_TASKS_EVALUATION_FMEASURE;
    }

    public Property getEVALUATION_AVERAGE_TASK_DELAY() {
        return EVALUATION_AVERAGE_TASK_DELAY;
    }

    public void setEVALUATION_AVERAGE_TASK_DELAY(Property eVALUATION_AVERAGE_TASK_DELAY) {
        EVALUATION_AVERAGE_TASK_DELAY = eVALUATION_AVERAGE_TASK_DELAY;
    }

    public Property getEVALUATION_MICRO_AVERAGE_RECALL() {
        return EVALUATION_MICRO_AVERAGE_RECALL;
    }

    public void setEVALUATION_MICRO_AVERAGE_RECALL(Property eVALUATION_MICRO_AVERAGE_RECALL) {
        EVALUATION_MICRO_AVERAGE_RECALL = eVALUATION_MICRO_AVERAGE_RECALL;
    }

    public Property getEVALUATION_MICRO_AVERAGE_PRECISION() {
        return EVALUATION_MICRO_AVERAGE_PRECISION;
    }

    public void setEVALUATION_MICRO_AVERAGE_PRECISION(Property eVALUATION_MICRO_AVERAGE_PRECISION) {
        EVALUATION_MICRO_AVERAGE_PRECISION = eVALUATION_MICRO_AVERAGE_PRECISION;
    }

    public Property getEVALUATION_MICRO_AVERAGE_FMEASURE() {
        return EVALUATION_MICRO_AVERAGE_FMEASURE;
    }

    public void setEVALUATION_MICRO_AVERAGE_FMEASURE(Property eVALUATION_MICRO_AVERAGE_FMEASURE) {
        EVALUATION_MICRO_AVERAGE_FMEASURE = eVALUATION_MICRO_AVERAGE_FMEASURE;
    }

    public Property getEVALUATION_MACRO_AVERAGE_RECALL() {
        return EVALUATION_MACRO_AVERAGE_RECALL;
    }

    public void setEVALUATION_MACRO_AVERAGE_RECALL(Property eVALUATION_MACRO_AVERAGE_RECALL) {
        EVALUATION_MACRO_AVERAGE_RECALL = eVALUATION_MACRO_AVERAGE_RECALL;
    }

    public Property getEVALUATION_MACRO_AVERAGE_PRECISION() {
        return EVALUATION_MACRO_AVERAGE_PRECISION;
    }

    public void setEVALUATION_MACRO_AVERAGE_PRECISION(Property eVALUATION_MACRO_AVERAGE_PRECISION) {
        EVALUATION_MACRO_AVERAGE_PRECISION = eVALUATION_MACRO_AVERAGE_PRECISION;
    }

    public Property getEVALUATION_MACRO_AVERAGE_FMEASURE() {
        return EVALUATION_MACRO_AVERAGE_FMEASURE;
    }

    public void setEVALUATION_MACRO_AVERAGE_FMEASURE(Property eVALUATION_MACRO_AVERAGE_FMEASURE) {
        EVALUATION_MACRO_AVERAGE_FMEASURE = eVALUATION_MACRO_AVERAGE_FMEASURE;
    }

    public Property getEVALUATION_MAX_TPS() {
        return EVALUATION_MAX_TPS;
    }

    public void setEVALUATION_MAX_TPS(Property eVALUATION_MAX_TPS) {
        EVALUATION_MAX_TPS = eVALUATION_MAX_TPS;
    }

    public Property getEVALUATION_AVERAGE_TPS() {
        return EVALUATION_AVERAGE_TPS;
    }

    public void setEVALUATION_AVERAGE_TPS(Property eVALUATION_AVERAGE_TPS) {
        EVALUATION_AVERAGE_TPS = eVALUATION_AVERAGE_TPS;
    }

    public OdinEvaluationModule() {
    }

    @Override
    public void init() throws Exception {
        LOGGER.info("Initialization begins.");
        super.init();
        this.internalInit();
        LOGGER.info("Initialization is over.");

    }

    /**
     * Internal initialization function. Assigns the corresponding values to all
     * class fields whose values are defined by the environmental variables.
     * 
     */
    public void internalInit() {
        Map<String, String> env = System.getenv();

        /* average task delay */
        if (!env.containsKey(OdinConstants.EVALUATION_AVERAGE_TASK_DELAY)) {
            throw new IllegalArgumentException("Couldn't get \"" + OdinConstants.EVALUATION_AVERAGE_TASK_DELAY
                    + "\" from the environment. Aborting.");
        }
        setEVALUATION_AVERAGE_TASK_DELAY(
                this.finalModel.createProperty(env.get(OdinConstants.EVALUATION_AVERAGE_TASK_DELAY)));

        /* micro average recall */
        if (!env.containsKey(OdinConstants.EVALUATION_MICRO_AVERAGE_RECALL)) {
            throw new IllegalArgumentException("Couldn't get \"" + OdinConstants.EVALUATION_MICRO_AVERAGE_RECALL
                    + "\" from the environment. Aborting.");
        }
        setEVALUATION_MICRO_AVERAGE_RECALL(
                this.finalModel.createProperty(env.get(OdinConstants.EVALUATION_MICRO_AVERAGE_RECALL)));

        /* micro average precision */
        if (!env.containsKey(OdinConstants.EVALUATION_MICRO_AVERAGE_PRECISION)) {
            throw new IllegalArgumentException("Couldn't get \"" + OdinConstants.EVALUATION_MICRO_AVERAGE_PRECISION
                    + "\" from the environment. Aborting.");
        }
        setEVALUATION_MICRO_AVERAGE_PRECISION(
                this.finalModel.createProperty(env.get(OdinConstants.EVALUATION_MICRO_AVERAGE_PRECISION)));

        /* micro average fmeasure */
        if (!env.containsKey(OdinConstants.EVALUATION_MICRO_AVERAGE_FMEASURE)) {
            throw new IllegalArgumentException("Couldn't get \"" + OdinConstants.EVALUATION_MICRO_AVERAGE_FMEASURE
                    + "\" from the environment. Aborting.");
        }
        setEVALUATION_MICRO_AVERAGE_FMEASURE(
                this.finalModel.createProperty(env.get(OdinConstants.EVALUATION_MICRO_AVERAGE_FMEASURE)));

        /* macro average recall */
        if (!env.containsKey(OdinConstants.EVALUATION_MACRO_AVERAGE_RECALL)) {
            throw new IllegalArgumentException("Couldn't get \"" + OdinConstants.EVALUATION_MACRO_AVERAGE_RECALL
                    + "\" from the environment. Aborting.");
        }
        setEVALUATION_MACRO_AVERAGE_RECALL(
                this.finalModel.createProperty(env.get(OdinConstants.EVALUATION_MACRO_AVERAGE_RECALL)));

        /* macro average precision */
        if (!env.containsKey(OdinConstants.EVALUATION_MACRO_AVERAGE_PRECISION)) {
            throw new IllegalArgumentException("Couldn't get \"" + OdinConstants.EVALUATION_MACRO_AVERAGE_PRECISION
                    + "\" from the environment. Aborting.");
        }
        setEVALUATION_MACRO_AVERAGE_PRECISION(
                this.finalModel.createProperty(env.get(OdinConstants.EVALUATION_MACRO_AVERAGE_PRECISION)));

        /* macro average fmeasure */
        if (!env.containsKey(OdinConstants.EVALUATION_MACRO_AVERAGE_FMEASURE)) {
            throw new IllegalArgumentException("Couldn't get \"" + OdinConstants.EVALUATION_MACRO_AVERAGE_FMEASURE
                    + "\" from the environment. Aborting.");
        }
        setEVALUATION_MACRO_AVERAGE_FMEASURE(
                this.finalModel.createProperty(env.get(OdinConstants.EVALUATION_MACRO_AVERAGE_FMEASURE)));

        /* max TPS */
        if (!env.containsKey(OdinConstants.EVALUATION_MAX_TPS)) {
            throw new IllegalArgumentException(
                    "Couldn't get \"" + OdinConstants.EVALUATION_MAX_TPS + "\" from the environment. Aborting.");
        }
        setEVALUATION_MAX_TPS(this.finalModel.createProperty(env.get(OdinConstants.EVALUATION_MAX_TPS)));

        /* average TPS */
        if (!env.containsKey(OdinConstants.EVALUATION_AVERAGE_TPS)) {
            throw new IllegalArgumentException(
                    "Couldn't get \"" + OdinConstants.EVALUATION_AVERAGE_TPS + "\" from the environment. Aborting.");
        }
        setEVALUATION_AVERAGE_TPS(this.finalModel.createProperty(env.get(OdinConstants.EVALUATION_AVERAGE_TPS)));

        /* tasks recall evaluation */
        if (!env.containsKey(OdinConstants.EVALUATION_TASKS_EVALUATION_RECALL)) {
            throw new IllegalArgumentException("Couldn't get \"" + OdinConstants.EVALUATION_TASKS_EVALUATION_RECALL
                    + "\" from the environment. Aborting.");
        }
        setEVALUATION_TASKS_EVALUATION_RECALL(
                this.finalModel.createProperty(env.get(OdinConstants.EVALUATION_TASKS_EVALUATION_RECALL)));
        /* tasks precision evaluation */
        if (!env.containsKey(OdinConstants.EVALUATION_TASKS_EVALUATION_PRECISION)) {
            throw new IllegalArgumentException("Couldn't get \"" + OdinConstants.EVALUATION_TASKS_EVALUATION_PRECISION
                    + "\" from the environment. Aborting.");
        }
        setEVALUATION_TASKS_EVALUATION_PRECISION(
                this.finalModel.createProperty(env.get(OdinConstants.EVALUATION_TASKS_EVALUATION_PRECISION)));

        /* tasks fmeasure evaluation */
        if (!env.containsKey(OdinConstants.EVALUATION_TASKS_EVALUATION_FMEASURE)) {
            throw new IllegalArgumentException("Couldn't get \"" + OdinConstants.EVALUATION_TASKS_EVALUATION_FMEASURE
                    + "\" from the environment. Aborting.");
        }
        setEVALUATION_TASKS_EVALUATION_FMEASURE(
                this.finalModel.createProperty(env.get(OdinConstants.EVALUATION_TASKS_EVALUATION_FMEASURE)));

        /* tasks tps evaluation */
        if (!env.containsKey(OdinConstants.EVALUATION_TASKS_EVALUATION_TPS)) {
            throw new IllegalArgumentException("Couldn't get \"" + OdinConstants.EVALUATION_TASKS_EVALUATION_TPS
                    + "\" from the environment. Aborting.");
        }
        setEVALUATION_TASKS_EVALUATION_TPS(
                this.finalModel.createProperty(env.get(OdinConstants.EVALUATION_TASKS_EVALUATION_TPS)));

        /* tasks delay evaluation */
        if (!env.containsKey(OdinConstants.EVALUATION_TASKS_EVALUATION_DELAY)) {
            throw new IllegalArgumentException("Couldn't get \"" + OdinConstants.EVALUATION_TASKS_EVALUATION_DELAY
                    + "\" from the environment. Aborting.");
        }
        setEVALUATION_TASKS_EVALUATION_DELAY(
                this.finalModel.createProperty(env.get(OdinConstants.EVALUATION_TASKS_EVALUATION_DELAY)));
    }

    /**
     * Converts the ResultSet obtain by performing a SELECT SPARQL query into a
     * list of maps. Each hashmap has as keys the variables and as value the
     * corresponding RDFNode of that binding.
     * 
     * @param results,
     *            the ResultSet obtained from performing a SELECT SPARQL query
     *            against the system
     * @return answers, a list that contains all bindings
     */
    protected ArrayList<HashMap<String, String>> getBindings(ResultSet results) {

        ArrayList<HashMap<String, String>> answers = new ArrayList<HashMap<String, String>>();
        if (results == null)
            return null;
        List<String> variables = results.getResultVars();

        while (results.hasNext()) {
            // get a binding - table row
            QuerySolution solution = results.next();
            int bindingsCounter = 0;
            // get value of each named variable in this binding
            HashMap<String, String> binding = new HashMap<String, String>();

            for (String variable : variables) {
                // if the variable is part of the binding
                if (solution.contains(variable) == true) {
                    // get the resource
                    RDFNode answer = solution.get(variable);
                    if (answer.isLiteral()) {
                        binding.put(variable, answer.asLiteral().getLexicalForm());
                    } else {
                        binding.put(variable, answer.asResource().getURI());
                    }
                } else {
                    binding.put(variable, null);
                }

            }
            answers.add(bindingsCounter, binding);
            bindingsCounter++;
        }
        return answers;
    }

    @Override
    public void evaluateResponse(byte[] expectedData, byte[] receivedData, long taskSentTimestamp,
            long responseReceivedTimestamp) throws Exception {

        LOGGER.info("Evaluation of task begins.");

        // delay must be in seconds
        double delay = (double) (responseReceivedTimestamp - taskSentTimestamp) / 1000l;
        this.setSumTaskDelay(this.getSumTaskDelay() + delay);
        this.setTaskCounter(this.getTaskCounter() + 1);
        // read expected data
        ByteBuffer expectedBuffer = ByteBuffer.wrap(expectedData);
        long modelSize = Long.valueOf(RabbitMQUtils.readString(expectedBuffer));
        // convert begin and end point of the stream into seconds
        double streamBeginPoint = (double) Long.valueOf(RabbitMQUtils.readString(expectedBuffer)) / 1000l;
        double streamEndPoint = (double) Long.valueOf(RabbitMQUtils.readString(expectedBuffer)) / 1000l;

        ArrayList<HashMap<String, String>> expectedAnswers = new ArrayList<HashMap<String, String>>();
        InputStream inExpected = new ByteArrayInputStream(
                RabbitMQUtils.readString(expectedBuffer).getBytes(StandardCharsets.UTF_8));
        ResultSet expected = ResultSetFactory.fromJSON(inExpected);
        expectedAnswers = this.getBindings(expected);
        ////////////////////////////////////////////////////////////////////////////////////////////////////
        ArrayList<HashMap<String, String>> receivedAnswers = new ArrayList<HashMap<String, String>>();
        InputStream inReceived = new ByteArrayInputStream(receivedData);
        ResultSet received = ResultSetFactory.fromJSON(inReceived);
        receivedAnswers = this.getBindings(received);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        int truePositives = 0;
        int falsePositives = 0;
        int falseNegatives = 0;

        for (HashMap<String, String> expectedBinding : expectedAnswers) {
            boolean tpFound = false;
            for (HashMap<String, String> receivedBinding : receivedAnswers) {
                if (expectedBinding.equals(receivedBinding)) {
                    tpFound = true;
                    break;
                }
            }
            if (tpFound == true)
                truePositives++;
            else
                falseNegatives++;
        }
        // what is not TP in the received answers, is a FP
        falsePositives = receivedAnswers.size() - truePositives;

        double recall = (double) truePositives / (double) (truePositives + falseNegatives);
        double precision = (double) truePositives / (double) (truePositives + falsePositives);
        double tps = (double) modelSize / (double) (streamEndPoint - streamBeginPoint);
        
        if (Double.isNaN(precision) == true)
            precision = 0.0d;
        this.setSumPrecision(this.getSumPrecision() + precision);

        if (Double.isNaN(recall) == true)
            recall = 0.0d;
        this.setSumRecall(this.getSumRecall() + recall);

        setSumStreamModel(getSumStreamModel() + modelSize);
        setSumStreamInterval(getSumStreamInterval() + (streamEndPoint - streamBeginPoint));
        setTotalFalseNegatives(getTotalFalseNegatives() + falseNegatives);
        setTotalFalsePositives(getTotalFalsePositives() + falsePositives);
        setTotalTruePositives(getTotalTruePositives() + truePositives);

        TaskEvaluation newEvaluation = new TaskEvaluation(recall, precision, tps, delay, receivedAnswers.size(),
                expectedAnswers.size());

        if (!getTasks().containsKey(taskSentTimestamp)) {
            getTasks().put(taskSentTimestamp, new ArrayList<TaskEvaluation>());
        }
        getTasks().get(taskSentTimestamp).add(newEvaluation);

        LOGGER.info("Evaluation of task is over.");

    }

    @Override
    public Model summarizeEvaluation() throws Exception {
        LOGGER.info("Summary of Evaluation begins.");
        LOGGER.info("Odin Evaluation Module has evaluated " + this.taskCounter + " tasks");

        if (this.experimentUri == null) {
            Map<String, String> env = System.getenv();
            this.experimentUri = env.get(Constants.HOBBIT_EXPERIMENT_URI_KEY);
        }

        double averageTaskDelay = (double) this.getSumTaskDelay() / (double) this.getTaskCounter();
        double averageTPS = (double) this.getSumStreamModel() / (double) this.getSumStreamInterval();

        // compute macro and micro averages KPIs
        double microAverageRecall = (double) this.getTotalTruePositives()
                / (double) (this.getTotalTruePositives() + this.getTotalFalseNegatives());
        if(Double.isNaN(microAverageRecall) == true)
            microAverageRecall = 0.0d;
        
        double microAveragePrecision = (double) this.getTotalTruePositives()
                / (double) (this.getTotalTruePositives() + this.getTotalFalsePositives());
        if(Double.isNaN(microAveragePrecision) == true)
            microAveragePrecision = 0.0d;
        
        
        double microAverageFmeasure = (double) (2.0 * microAverageRecall * microAveragePrecision)
                / (double) (microAverageRecall + microAveragePrecision);
        if(Double.isNaN(microAverageFmeasure) == true)
            microAverageFmeasure = 0.0d;
        
        double macroAverageRecall = (double) this.getSumRecall() / (double) this.getTaskCounter();
        if(Double.isNaN(macroAverageRecall) == true)
            macroAverageRecall = 0.0d;
        
        double macroAveragePrecision = (double) this.getSumPrecision() / (double) this.getTaskCounter();
        if(Double.isNaN(macroAveragePrecision) == true)
            macroAveragePrecision = 0.0d;
        
        double macroAverageFmeasure = (double) (2.0 * macroAverageRecall * macroAveragePrecision)
                / (double) (macroAverageRecall + macroAveragePrecision);
        if(Double.isNaN(macroAverageFmeasure) == true)
            macroAverageFmeasure = 0.0d;       
        
        //////////////////////////////////////////////////////////////////////////////////////////////
        Resource experiment = this.finalModel.createResource(experimentUri);
        this.finalModel.add(experiment, RDF.type, HOBBIT.Experiment);

        // Literal maxTPSLiteral =
        // this.finalModel.createTypedLiteral(this.maxTPS,
        // XSDDatatype.XSDdouble);

        Literal averageTaskDelayLiteral = finalModel.createTypedLiteral(averageTaskDelay, XSDDatatype.XSDdouble);
        finalModel.add(experiment, EVALUATION_AVERAGE_TASK_DELAY, averageTaskDelayLiteral);

        Literal microAverageRecallLiteral = finalModel.createTypedLiteral(microAverageRecall,
                XSDDatatype.XSDdouble);
        finalModel.add(experiment, EVALUATION_MICRO_AVERAGE_RECALL, microAverageRecallLiteral);

        Literal microAveragePrecisionLiteral = finalModel.createTypedLiteral(microAveragePrecision,
                XSDDatatype.XSDdouble);
        finalModel.add(experiment, EVALUATION_MICRO_AVERAGE_PRECISION, microAveragePrecisionLiteral);

        Literal microAverageFmeasureLiteral = finalModel.createTypedLiteral(microAverageFmeasure,
                XSDDatatype.XSDdouble);
        finalModel.add(experiment, EVALUATION_MICRO_AVERAGE_FMEASURE, microAverageFmeasureLiteral);

        Literal macroAverageRecallLiteral = finalModel.createTypedLiteral(macroAverageRecall,
                XSDDatatype.XSDdouble);
        finalModel.add(experiment, EVALUATION_MACRO_AVERAGE_RECALL, macroAverageRecallLiteral);

        Literal macroAveragePrecisionLiteral = finalModel.createTypedLiteral(macroAveragePrecision,
                XSDDatatype.XSDdouble);
        finalModel.add(experiment, EVALUATION_MACRO_AVERAGE_PRECISION, macroAveragePrecisionLiteral);

        Literal macroAverageFmeasureLiteral = finalModel.createTypedLiteral(macroAverageFmeasure,
                XSDDatatype.XSDdouble);
        finalModel.add(experiment, EVALUATION_MACRO_AVERAGE_FMEASURE, macroAverageFmeasureLiteral);

        Literal averageTPSLiteral = finalModel.createTypedLiteral(averageTPS, XSDDatatype.XSDdouble);
        finalModel.add(experiment, EVALUATION_AVERAGE_TPS, averageTPSLiteral);

        HashMap<String, Resource> evalResources = createCubeDatasets(experiment);
        addObservations(evalResources, experiment);

        LOGGER.info("Summary of Evaluation is over.");

        return finalModel;
    }

    /**
     * Creates RDF Cube Datasets for each KPI defined in the configuration file
     * and adds them to the final evaluation model. For each RDF Cube Dataset,
     * each observation value corresponds to the KPI value achieved for that
     * particular task evaluation.
     * 
     * @param experiment,
     *            the experiment Resource
     * @return a map with keys the KPIs labels and as values the corresponding
     *         resources
     */
    HashMap<String, Resource> createCubeDatasets(Resource experiment) {

        HashMap<String, Resource> evalResources = new HashMap<String, Resource>();

        DataSetStructure[] KPIs = DataSetStructure.class.getEnumConstants();
        for (DataSetStructure kpi : KPIs) {

            // e.g.
            // http://w3id.org/bench#Recall_Dataset_for_1493757615796
            String l = kpi.getDatasetResource(experimentUri);

            // bench:Recall_Dataset_for_1493757615796
            Resource taskEvaluationResource = finalModel.createResource(l);

            experiment.addProperty(finalModel.createProperty(kpi.getKpiProperty()), taskEvaluationResource);

            // -- Data Set --
            // bench:Recall_Dataset_for_1493757615796 a qb:DataSet;
            taskEvaluationResource.addProperty(RDF.type, finalModel.createResource((DataSetStructure.dataset)));

            // rdfs:label "Recall Evaluation of SELECT SPARQL queries"@en;
            taskEvaluationResource.addProperty(RDFS.label, finalModel.createLiteral(kpi.getLabel(), "en"));
            evalResources.put(kpi.getLabel(), taskEvaluationResource);

            // dct:description "Recall Evaluation of SELECT SPARQL queries"@en;
            taskEvaluationResource.addProperty(
                    finalModel.createProperty(CubeDatasetProperties.DESCRIPTION.getPropertyURI()),
                    finalModel.createLiteral(kpi.getDescription(), "en"));
            // dct:publisher bench:organization ; dct:issued
            taskEvaluationResource.addProperty(
                    finalModel.createProperty(CubeDatasetProperties.PUBLISHER.getPropertyURI()),
                    finalModel.createLiteral(DataSetStructure.publisher, "en"));

            // "2010-08-11"^^xsd:date;
            taskEvaluationResource.addProperty(
                    finalModel.createProperty(CubeDatasetProperties.DATE.getPropertyURI()),
                    finalModel.createTypedLiteral(DataSetStructure.date, XSDDatatype.XSDdateTime));

            // dct:subject sdmx-subject:2.9 ;
            taskEvaluationResource.addProperty(
                    finalModel.createProperty(CubeDatasetProperties.SUBJECT.getPropertyURI()),
                    finalModel.createResource(DataSetStructure.subject));

            // qb:structure bench:RecallStructure_for_1493757615796
            Resource kpiStructure = this.finalModel
                    .createResource(kpi.getStructure() + "_for_" + experimentUri.split("#")[1]);
            taskEvaluationResource.addProperty(
                    finalModel.createProperty(CubeDatasetProperties.STRUCTURE.getPropertyURI()), kpiStructure);

            // sdmx-attribute:unitMeasure :recall
            taskEvaluationResource.addProperty(
                    finalModel.createProperty(CubeDatasetProperties.UNIT_MEASURE.getPropertyURI()),
                    finalModel.createResource(kpi.getUnitMeasure()));

            /////////////////////////////////////////////////////////////////////////////////////////////
            // -- Data structure definition --
            kpiStructure.addProperty(RDF.type, finalModel.createResource(DataSetStructure.datasetStrucute));
            Property componentProperty = finalModel
                    .createProperty(CubeDatasetProperties.COMPONENT.getPropertyURI());

            // qb:component [ qb:dimension bench:taskID];
            Resource taskID = finalModel.createResource(DataSetStructure.dimension);
            // e.g.
            // http://w3id.org/bench#Delay_Dataset_for_1231433223_taskIDComponent
            Resource taskIDAnon = finalModel.createResource(l + "DimensionComponent");
            taskIDAnon.addProperty(finalModel.createProperty(CubeDatasetProperties.DIMENSION.getPropertyURI()),
                    taskID);
            kpiStructure.addProperty(componentProperty, taskIDAnon);

            // qb:component [ qb:measure bench:recall];
            Resource measure = finalModel.createResource(kpi.getMeasure());
            // e.g.
            // http://w3id.org/bench#Delay_Dataset_for_1231433223_MeasureComponent
            Resource measureAnon = finalModel.createResource(l + "MeasureComponent");
            measureAnon.addProperty(finalModel.createProperty(CubeDatasetProperties.MEASURE.getPropertyURI()),
                    measure);
            kpiStructure.addProperty(componentProperty, measureAnon);

            /*
             * qb:component [ qb:attribute sdmx-attribute:unitMeasure;
             * qb:componentRequired "true"^^xsd:boolean; qb:componentAttachment
             * qb:DataSet ] .
             * 
             */
            // e.g.
            // http://w3id.org/bench#Delay_Dataset_for_1231433223AttributeComponent
            Resource attributeAnon = finalModel.createResource(l + "AttributeComponent");
            
            attributeAnon.addProperty(finalModel.createProperty(CubeDatasetProperties.ATTRIBUTE.getPropertyURI()),
                    finalModel.createProperty(DataSetStructure.unitMeasureObject));
            
            attributeAnon.addProperty(finalModel.createProperty(CubeDatasetProperties.REQUIRED.getPropertyURI()),
                    finalModel.createTypedLiteral(new Boolean(true)));
            
            attributeAnon.addProperty(finalModel.createProperty(CubeDatasetProperties.ATTACHMENT.getPropertyURI()),
                    finalModel.createResource(DataSetStructure.dataset));

            kpiStructure.addProperty(componentProperty, attributeAnon);

            finalModel.add(DataSetStructure.defineModelDimension(taskID));

            finalModel.add(kpi.defineModelMeasure(measure));

        }
        return evalResources;

    }

    /**
     * Creates observations for each RDF Cube Dataset and adds them to the final
     * evaluation model. Each observation has as Dimension component the task ID
     * of the task and as Measure component the corresponding KPI.
     * 
     * @param evalResources,
     *            a map with keys the KPIs labels and as values the
     *            corresponding resources
     * @param experiment,
     *            the experiment Resource
     */
    void addObservations(HashMap<String, Resource> evalResources, Resource experiment) {
        int counter = 1;

        for (Entry<Long, ArrayList<TaskEvaluation>> cell : getTasks().entrySet()) {

            for (TaskEvaluation task : cell.getValue()) {
                // for each task, get all kpis
                DataSetStructure[] KPIs = DataSetStructure.class.getEnumConstants();

                for (DataSetStructure kpi : KPIs) {
                    // get the KPI label
                    String kpiLabel = kpi.getLabel();
                    // and retrieve the corresponding qb:DataSet
                    // bench:Recall_Dataset_for_1493757615796
                    Resource eval = evalResources.get(kpiLabel);

                    Property measure = finalModel.createProperty(kpi.getMeasure());
                    Property dimension = finalModel.createProperty(DataSetStructure.dimension);

                    // create observation resource
                    Resource obs = finalModel.createResource("http://w3id.org/bench#" + kpiLabel + "_Observation"
                            + counter + "_for_" + experimentUri.split("#")[1]);
                    // bench:recall1 a qb:Observation;
                    obs.addProperty(RDF.type, finalModel.createResource(DataSetStructure.observation));

                    // qb:dataSet bench:overallEvaluation1Recall ;
                    obs.addProperty(finalModel.createProperty(DataSetStructure.dataset), eval);

                    // bench:taskID
                    // "1"^^http://www.w3.org/2001/XMLSchema#unsignedInt ;
                    obs.addProperty(dimension, String.valueOf(counter), XSDDatatype.XSDinteger);

                    // bench:recall
                    // "0.88"^^http://www.w3.org/2001/XMLSchema#double .
                    obs.addProperty(measure, String.valueOf(task.getKPIs().get(kpiLabel)), XSDDatatype.XSDdouble);

                    if (kpiLabel.equalsIgnoreCase("recall")) {
                        double recall = task.getKPIs().get(kpiLabel);
                        if (recall == 1.0d) {
                            maxTPS = task.getKPIs().get("TPS");
                        }
                    }
                }
                counter++;
            }
        }
        Literal maxTPSLiteral = finalModel.createTypedLiteral(this.maxTPS, XSDDatatype.XSDdouble);
        finalModel.add(experiment, EVALUATION_MAX_TPS, maxTPSLiteral);

    }

}
