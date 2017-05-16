package org.hobbit.odin;

import org.hobbit.core.Commands;
import org.hobbit.core.components.AbstractBenchmarkController;
import org.hobbit.odin.systems.virtuoso.VirtuosoSystemAdapterConstants;
import org.hobbit.odin.util.OdinConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;

import org.apache.jena.rdf.model.NodeIterator;

/**
 * Odin Benchmark Controller. It is responsible for initializing all Odin
 * components.
 * 
 * @author Kleanthi Georgala (georgala@informatik.uni-leipzig.de)
 * @version 1.0
 *
 */
public class OdinBenchmarkController extends AbstractBenchmarkController {
    private static final Logger LOGGER = LoggerFactory.getLogger(OdinBenchmarkController.class);
    /* Data generator Docker image */
    private static final String STRUCTURED_DATA_GENERATOR_CONTAINER_IMAGE = "git.project-hobbit.eu:4567/kleanthie.georgala/odindatagenerator";
    /* Task generator Docker image */
    private static final String STRUCTURED_TASK_GENERATOR_CONTAINER_IMAGE = "git.project-hobbit.eu:4567/kleanthie.georgala/odintaskgenerator";
    /* Evaluation module Docker image */
    private static final String EVALUATION_MODULE_CONTAINER_IMAGE = "git.project-hobbit.eu:4567/kleanthie.georgala/odinevaluationmodule";

    private Semaphore minMaxTimestampMutex = new Semaphore(0);

    private Semaphore bulkLoadMutex = new Semaphore(0);
    private Semaphore sysAdapterMutex = new Semaphore(0);

    private int numberOfDataGenerators = -1;
    private int numberOfTaskGenerators = -1;

    private long timestampsMax = Long.MIN_VALUE;
    private long timestampsMin = Long.MAX_VALUE;
    private String[] envVariablesEvaluationModule = null;

    public OdinBenchmarkController() {
    }

    public OdinBenchmarkController(int a, int b) {
        this.numberOfDataGenerators = a;
        this.numberOfTaskGenerators = b;
    }

    @Override
    public void init() throws Exception {
        LOGGER.info("Initialization begins.");
        super.init();
        NodeIterator iterator;
        /* Number of data generators */
        if (numberOfDataGenerators == -1) {

            iterator = benchmarkParamModel.listObjectsOfProperty(
                    benchmarkParamModel.getProperty("http://w3id.org/bench#hasNumberOfDataGenerators"));
            numberOfDataGenerators = -1;
            if (iterator.hasNext()) {
                try {
                    numberOfDataGenerators = iterator.next().asLiteral().getInt();
                } catch (Exception e) {
                    LOGGER.error("Exception while parsing parameter.", e);
                }
            }
            if (numberOfDataGenerators < 0) {
                LOGGER.error(
                        "Couldn't get the number of data generators from the parameter model. Using the default value.");
                numberOfDataGenerators = 2;
            }
        }
        /* Number of task generators */
        if (numberOfTaskGenerators == -1) {
            iterator = benchmarkParamModel.listObjectsOfProperty(
                    benchmarkParamModel.getProperty("http://w3id.org/bench#hasNumberOfTaskGenerators"));
            numberOfTaskGenerators = -1;
            if (iterator.hasNext()) {
                try {
                    numberOfTaskGenerators = iterator.next().asLiteral().getInt();
                } catch (Exception e) {
                    LOGGER.error("Exception while parsing parameter.", e);
                }
            }
            if (numberOfTaskGenerators < 0) {
                LOGGER.error(
                        "Couldn't get the seed for the mimicking algorithm seed from the parameter model. Using the default value.");
                numberOfTaskGenerators = 1;
            }
        }
        /* Seed for mimicking algorithm */
        iterator = benchmarkParamModel
                .listObjectsOfProperty(benchmarkParamModel.getProperty("http://w3id.org/bench#hasSeed"));
        int seed = -1;
        if (iterator.hasNext()) {
            try {
                seed = iterator.next().asLiteral().getInt();
            } catch (Exception e) {
                LOGGER.error("Exception while parsing parameter.", e);
            }
        }
        if (seed < 0) {
            LOGGER.error(
                    "Couldn't get the seed for the mimicking algorithm seed from the parameter model. Using the default value.");
            seed = 100;
        }
        /* Number of triples */
        iterator = benchmarkParamModel
                .listObjectsOfProperty(benchmarkParamModel.getProperty("http://w3id.org/bench#hasPopulation"));
        int population = -1;
        if (iterator.hasNext()) {
            try {
                population = iterator.next().asLiteral().getInt();
            } catch (Exception e) {
                LOGGER.error("Exception while parsing parameter.", e);
            }
        }
        if (population < 0) {
            LOGGER.error("Couldn't get the number of triples from the parameter model. Using the default value.");
            population = 1000;
        }
        /* Name of mimicking algorithm */
        iterator = benchmarkParamModel
                .listObjectsOfProperty(benchmarkParamModel.getProperty("http://w3id.org/bench#hasMimickingAlgorithm"));
        String mimicking = null;
        if (iterator.hasNext()) {
            try {
                mimicking = iterator.next().asLiteral().getString();
            } catch (Exception e) {
                LOGGER.error("Exception while parsing parameter.", e);
            }
        }
        if (mimicking == null) {
            LOGGER.error(
                    "Couldn't get the name of the mimicking algorithm from the parameter model. Using the default value.");
            mimicking = "TRANSPORT_DATA";
        }

        /* Name of mimicking algorithm output folder */
        iterator = benchmarkParamModel
                .listObjectsOfProperty(benchmarkParamModel.getProperty("http://w3id.org/bench#hasMimickingOutput"));
        String mimickingOutput = null;
        if (iterator.hasNext()) {
            try {
                mimickingOutput = iterator.next().asLiteral().getString();
                if (!mimickingOutput.endsWith("/"))
                    mimickingOutput = mimickingOutput + "/";
                mimickingOutput = System.getProperty("user.dir") + "/" + mimickingOutput + this.numberOfDataGenerators
                        + "_" + this.numberOfTaskGenerators + "/";
                boolean success = false;
                File directory = new File(mimickingOutput);
                if (!directory.exists()) {
                    success = directory.mkdirs();
                    if (!success) {
                        try {
                            throw new IOException("Failed to create new directory: " + mimickingOutput);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Exception while parsing parameter.", e);
            }
        }
        if (mimickingOutput == null) {
            LOGGER.error(
                    "Couldn't get the name of the mimicking algorithm output folder from the parameter model. Using the default value.");
            mimickingOutput = "output_data/";
        }

        /* Number of inserted queries until a select query is performed */
        iterator = benchmarkParamModel.listObjectsOfProperty(
                benchmarkParamModel.getProperty("http://w3id.org/bench#hasNumberOfInsertQueries"));
        int numberOfInsertQueries = -1;
        if (iterator.hasNext()) {
            try {
                numberOfInsertQueries = iterator.next().asLiteral().getInt();
            } catch (Exception e) {
                LOGGER.error("Exception while parsing parameter.", e);
            }
        }
        if (numberOfInsertQueries == -1) {
            LOGGER.error(
                    "Couldn't get the number of inserted queries until a select query is performed from the parameter model. Using the default value.");
            numberOfInsertQueries = 10;
            
        }

        /* Benchmark duration */
        iterator = benchmarkParamModel
                .listObjectsOfProperty(benchmarkParamModel.getProperty("http://w3id.org/bench#hasDuration"));
        long duration = -1;
        if (iterator.hasNext()) {
            try {
                duration = iterator.next().asLiteral().getLong();
            } catch (Exception e) {
                LOGGER.error("Exception while parsing parameter.", e);
            }
        }
        if (duration == -1) {
            LOGGER.error("Couldn't get duration of benchmark. Using the default value.");
            duration = 600000l;
        }

        // data generators environmental values
        String[] envVariablesDataGenerator = new String[] { OdinConstants.GENERATOR_SEED + "=" + seed,
                OdinConstants.GENERATOR_POPULATION + "=" + population,
                OdinConstants.GENERATOR_DATASET + "=" + mimicking,
                OdinConstants.GENERATOR_MIMICKING_OUTPUT + "=" + mimickingOutput,
                OdinConstants.GENERATOR_INSERT_QUERIES_COUNT + "=" + numberOfInsertQueries,
                OdinConstants.GENERATOR_BENCHMARK_DURATION + "=" + duration };

        createDataGenerators(STRUCTURED_DATA_GENERATOR_CONTAINER_IMAGE, numberOfDataGenerators,
                envVariablesDataGenerator);

        createTaskGenerators(STRUCTURED_TASK_GENERATOR_CONTAINER_IMAGE, numberOfTaskGenerators, new String[] {});

        createEvaluationStorage();
        /////////////////////////////////////////////////////////////////////////
        // get KPIs for evaluation module
        this.envVariablesEvaluationModule = new String[] {
                OdinConstants.EVALUATION_AVERAGE_TASK_DELAY + "=" + "http://w3id.org/bench#averageTaskDelay",
                OdinConstants.EVALUATION_MICRO_AVERAGE_RECALL + "=" + "http://w3id.org/bench#microAverageRecall",
                OdinConstants.EVALUATION_MICRO_AVERAGE_PRECISION + "=" + "http://w3id.org/bench#microAveragePrecision",
                OdinConstants.EVALUATION_MICRO_AVERAGE_FMEASURE + "=" + "http://w3id.org/bench#microAverageFmeasure",
                OdinConstants.EVALUATION_MACRO_AVERAGE_RECALL + "=" + "http://w3id.org/bench#macroAverageRecall",
                OdinConstants.EVALUATION_MACRO_AVERAGE_PRECISION + "=" + "http://w3id.org/bench#macroAveragePrecision",
                OdinConstants.EVALUATION_MACRO_AVERAGE_FMEASURE + "=" + "http://w3id.org/bench#macroAverageFmeasure",
                OdinConstants.EVALUATION_MAX_TPS + "=" + "http://w3id.org/bench#maxTPS",
                OdinConstants.EVALUATION_AVERAGE_TPS + "=" + "http://w3id.org/bench#averageTPS",
                OdinConstants.EVALUATION_TASKS_EVALUATION_RECALL + "=" + "http://w3id.org/bench#tasksRecall",
                OdinConstants.EVALUATION_TASKS_EVALUATION_PRECISION + "=" + "http://w3id.org/bench#tasksPrecision",
                OdinConstants.EVALUATION_TASKS_EVALUATION_FMEASURE + "=" + "http://w3id.org/bench#tasksFmeasure",
                OdinConstants.EVALUATION_TASKS_EVALUATION_TPS + "=" + "http://w3id.org/bench#tasksTPS",
                OdinConstants.EVALUATION_TASKS_EVALUATION_DELAY + "=" + "http://w3id.org/bench#tasksAnswerDelay" };

        // wait until you retrieve min and max timestamps from each data
        // generator
        minMaxTimestampMutex.acquire(numberOfDataGenerators);
        // send overall min max timestamps to all data generators
        ByteBuffer data = ByteBuffer.allocate(16);
        data.putLong(this.timestampsMin);
        data.putLong(this.timestampsMax);
        sendToCmdQueue(OdinConstants.OVERALL_MIN_MAX, data.array());

        waitForComponentsToInitialize();
        LOGGER.info("Initialization is over.");

    }

    @Override
    public void receiveCommand(byte command, byte[] data) {
        if (OdinConstants.MIN_MAX_FROM_DATAGENERATOR == command) {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            // read from a data generator its max and min values
            // do smt with them
            long minTS = buffer.getLong();
            long maxTS = buffer.getLong();
            if (minTS < this.timestampsMin)
                this.timestampsMin = minTS;
            if (maxTS > this.timestampsMax)
                this.timestampsMax = maxTS;

            minMaxTimestampMutex.release();
        } else if (OdinConstants.BULK_LOAD_FROM_DATAGENERATOR == command) {
            //this will be send by data gens
            bulkLoadMutex.release();
        } else if (VirtuosoSystemAdapterConstants.BULK_LOADING_DATA_FINISHED == command) {
            //this will be send by the sys adapter
            sysAdapterMutex.release();
        }
        super.receiveCommand(command, data);
    }

    @Override
    protected void executeBenchmark() throws Exception {
        LOGGER.info("Executing benchmark has started.");

        // give the start signals
        sendToCmdQueue(Commands.TASK_GENERATOR_START_SIGNAL);
        sendToCmdQueue(Commands.DATA_GENERATOR_START_SIGNAL);
        LOGGER.info("Send start signal to Data and Task Generators.");
        // wait until all data gens are done sending this message
        LOGGER.info("Waiting until all Data Generators send message that they are done with bulk load phase.");
        bulkLoadMutex.acquire(numberOfDataGenerators);
        LOGGER.info("Signal from ALL Data Generators received.");
        // sends message to sys adapter
        LOGGER.info("Message sent to System Adapter that bulk load phase is over.");
        sendToCmdQueue(VirtuosoSystemAdapterConstants.BULK_LOAD_DATA_GEN_FINISHED);
        //wait until message is received from sys adapter to continue
        LOGGER.info("Waiting until System Adapter sends message that it's done with bulk load phase.");
        sysAdapterMutex.acquire(1);
        LOGGER.info("Signal from System Adapter received.");
        //send message to data gens to go ahead
        sendToCmdQueue(OdinConstants.BULK_LOAD_FROM_CONTROLLER);
        // wait for the data generators to finish their work
        waitForDataGenToFinish();
        // wait for the task generators to finish their work
        waitForTaskGenToFinish();
        // wait for the system to terminate
        waitForSystemToFinish();
        // data generators environmental values
        createEvaluationModule(EVALUATION_MODULE_CONTAINER_IMAGE, this.envVariablesEvaluationModule);
        // wait for the evaluation to finish
        waitForEvalComponentsToFinish();

        sendResultModel(this.resultModel);
        LOGGER.info("Executing benchmark is over.");

    }
}
