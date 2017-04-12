package org.hobbit.odin.odindatagenerator;

import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.hobbit.core.Constants;
import org.hobbit.core.components.AbstractDataGenerator;
import org.hobbit.core.mimic.DockerBasedMimickingAlg;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.odin.mimicking.MimickingFactory;
import org.hobbit.odin.mimicking.MimickingType;
import org.hobbit.odin.util.OdinConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data Generator class for Odin Benchmark. It is responsible for invoking the
 * mimicking algorithm, for converting the time stamps of the original data into
 * the time interval of benchmark, for retrieving and sending the INSERT SPARQL
 * query to the SystemAdapter, and for retrieving and sending the SELECT SPARQL
 * query to the Task Generator.
 * 
 * @author Kleanthi Georgala (georgala@informatik.uni-leipzig.de)
 * @version 1.0
 *
 */
public class OdinDataGenerator extends AbstractDataGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(OdinDataGenerator.class);
    /* Seed for the mimicking algorithm */
    private String DATA_GENERATOR_SEED = null;
    /* Number of expected events to be generated by a mimicking algorithm */
    private String DATA_GENERATOR_POPULATION;
    /* Name of mimicking algorithm */
    private String DATA_GENERATOR_DATASET_NAME;
    /* Mimicking algorithm output folder */
    private String DATA_GENERATOR_OUTPUT_DATASET;
    /* Number of insert queries after a select query must be performed */
    private int DATA_GENERATOR_INSERT_QUERIES;
    /* Initial select query delay */
    private long initialSelectDelay = 1000l;
    private String defaultGraph = null;

    /*
     * Map with keys the ID of each stream and as value the corresponding stream
     */
    private TreeMap<Integer, Stream> streams = new TreeMap<Integer, Stream>();
    /* for debugging purposes */
    private boolean flag = true;

    /* Benchmark duration */
    private long benchmarkEndPoint = 0l;
    /* Benchmark begin point */
    private long benchmarkBeginPoint = 0l; // always set on 0
    /* Dataset original duration */
    private long datasetEndPoint = 0l;
    /* Dataset original begin point */
    private long datasetBeginPoint = 0l;

    private Semaphore minMaxTimestampMutex = new Semaphore(0);
    private Semaphore bulkLoadMutex = new Semaphore(0);

    // private int insertCounter = 0;
    // private int selectCounter = 0;
    /* Byte array for the Task Generator */
    private byte[] task;

    /* Setters and getters */
    public TreeMap<Integer, Stream> getStreams() {
        return streams;
    }

    public void setStreams(TreeMap<Integer, Stream> streams) {
        this.streams = streams;
    }

    public String getDATA_GENERATOR_SEED() {
        return DATA_GENERATOR_SEED;
    }

    public void setDATA_GENERATOR_SEED(String dATA_GENERATOR_SEED) {
        DATA_GENERATOR_SEED = dATA_GENERATOR_SEED;
    }

    public String getDATA_GENERATOR_POPULATION() {
        return DATA_GENERATOR_POPULATION;
    }

    public void setDATA_GENERATOR_POPULATION(String dATA_GENERATOR_POPULATION) {
        DATA_GENERATOR_POPULATION = dATA_GENERATOR_POPULATION;
    }

    public String getDATA_GENERATOR_DATASET_NAME() {
        return DATA_GENERATOR_DATASET_NAME;
    }

    public void setDATA_GENERATOR_DATASET_NAME(String dATA_GENERATOR_DATASET_NAME) {
        DATA_GENERATOR_DATASET_NAME = dATA_GENERATOR_DATASET_NAME;
    }

    public String getDATA_GENERATOR_OUTPUT_DATASET() {
        return DATA_GENERATOR_OUTPUT_DATASET;
    }

    public void setDATA_GENERATOR_OUTPUT_DATASET(String dATA_GENERATOR_OUTPUT_DATASET) {
        DATA_GENERATOR_OUTPUT_DATASET = dATA_GENERATOR_OUTPUT_DATASET;
    }

    public int getDATA_GENERATOR_INSERT_QUERIES() {
        return DATA_GENERATOR_INSERT_QUERIES;
    }

    public void setDATA_GENERATOR_INSERT_QUERIES(int dATA_GENERATOR_INSERT_QUERIES) {
        DATA_GENERATOR_INSERT_QUERIES = dATA_GENERATOR_INSERT_QUERIES;
    }

    public boolean isFlag() {
        return flag;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }

    public long getBenchmarkEndPoint() {
        return benchmarkEndPoint;
    }

    public void setBenchmarkEndPoint(long newMax) {
        this.benchmarkEndPoint = newMax;
    }

    public long getDatasetEndPoint() {
        return datasetEndPoint;
    }

    public void setDatasetEndPoint(long oldMin) {
        this.datasetEndPoint = oldMin;
    }

    public long getDatasetBeginPoint() {
        return datasetBeginPoint;
    }

    public void getDatasetBeginPoint(long oldMax) {
        this.datasetBeginPoint = oldMax;
    }

    public OdinDataGenerator() {
        addCommandHeaderId(Constants.HOBBIT_SESSION_ID_FOR_BROADCASTS);

    }

    /**
     * Class responsible for retrieving the INSERT SPARQL queries. Each INSERT
     * query is assigned to an individual thread to reassure that each different
     * data insertion will be performed on time based on its delay factor.
     * 
     * @author Kleanthi Georgala
     *
     */
    public class InsertThread implements Runnable {
        /* Inserty Query instance */
        InsertQueryInfo insertQuery = null;

        /* Constructor */
        public InsertThread(InsertQueryInfo insertQuery) {
            this.insertQuery = insertQuery;
        }

        @Override
        public void run() {
            // retrieve insert query
            String insert = this.insertQuery.getUpdateRequestAsString();
            // serialize insert query into a byte array and send it to the
            // system adapter
            sentToSystemAdapter(RabbitMQUtils.writeByteArrays(
                    new byte[][] { RabbitMQUtils.writeString(defaultGraph), RabbitMQUtils.writeString(insert) }));
        }

        /**
         * Sends INSERT query as byte array to the system adapter.
         * 
         * @param data,
         *            the byte array to send to the System Adapter
         */
        public void sentToSystemAdapter(byte[] data) {
            if (isFlag() == true) {
                try {
                    sendDataToSystemAdapter(data);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    /**
     * Class responsible for retrieving the SELECT SPARQL queries. Each SELECT
     * query is assigned to an individual thread to reassure that each different
     * data insertion will be performed on time based on its delay factor.
     * 
     * @author Kleanthi Georgala
     *
     */
    public class SelectThread implements Runnable {
        /* Select Query instance */
        private SelectQueryInfo selectQuery = null;
        /* Current stream end point */
        private long beginPoint = 0l;
        /*
         * Size of the current stream (sum of all triples of all INSERT queries)
         */
        private long modelSize = 0l;

        /* Constructor */
        public SelectThread(SelectQueryInfo s, long size, long e) {
            this.selectQuery = s;
            this.modelSize = size;
            this.beginPoint = e;
        }

        @Override
        public void run() {

            String select = this.selectQuery.getSelectQueryAsString();
            byte[] expectedAnswers = this.selectQuery.getExpectedAnswers();
            // select query, modelsize, begin point, end point, answers
            byte[][] answers = new byte[4][];
            answers[0] = RabbitMQUtils.writeString(select);
            answers[1] = RabbitMQUtils.writeString(String.valueOf(this.modelSize));
            answers[2] = RabbitMQUtils.writeString(String.valueOf(this.beginPoint));
            answers[3] = expectedAnswers;
            setTask(RabbitMQUtils.writeByteArrays(answers));

            sentToTaskGenerator();
        }

        /**
         * Sends SELECT SPARQL as a byte array to the Task Generator.
         */
        public void sentToTaskGenerator() {
            if (isFlag() == true) {
                try {
                    sendDataToTaskGenerator(getTask());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public void init() throws Exception {
        LOGGER.info("Initialization begins.");
        super.init();
        this.internalInit();

        this.defaultGraph = "http://www.graph" + this.getGeneratorId() + ".com/";
        LOGGER.info("Invoking mimicking algorithm: " + this.getDATA_GENERATOR_DATASET_NAME());
        // call mimicking algorithm
        runMimicking();

        LOGGER.info("Divinding data based on the original generation time stamps.");
        // divide data into files based on their generation time stamp
        divideData();

        LOGGER.info("Assigning set of triples to time stamps.");
        TreeMap<Long, String> files = assignFilesToTimeStamps();

        // get overall min and max time stamps
        ByteBuffer data = ByteBuffer.allocate(16);
        data.putLong(files.firstKey());
        data.putLong(files.lastKey());
        sendToCmdQueue(OdinConstants.MIN_MAX_FROM_DATAGENERATOR, data.array());

        minMaxTimestampMutex.acquire();

        // convert old time stamps to new time stamps
        LOGGER.info("Converting time stamps to benchmark interval.");
        Map<Long, ArrayList<String>> insertList = convertTimeStampsToNewInterval(files);
        // divide insert queries into streams
        LOGGER.info("Creating streams..");
        createStreams(insertList);
        LOGGER.info("Initialization is over.");

    }

    public void createOutputDirectory(String name) {
        setDATA_GENERATOR_OUTPUT_DATASET(name);
        if (!getDATA_GENERATOR_OUTPUT_DATASET().endsWith("/"))
            setDATA_GENERATOR_OUTPUT_DATASET(getDATA_GENERATOR_OUTPUT_DATASET() + "/");

        ////////
        boolean success = false;
        File directory = new File(getDATA_GENERATOR_OUTPUT_DATASET());
        if (!directory.exists()) {
            success = directory.mkdirs();
            if (!success) {
                try {
                    throw new IOException("Failed to create new directory: " + getDATA_GENERATOR_OUTPUT_DATASET());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        setDATA_GENERATOR_OUTPUT_DATASET(getDATA_GENERATOR_OUTPUT_DATASET() + this.getDATA_GENERATOR_DATASET_NAME()
                + "_" + this.getDATA_GENERATOR_POPULATION() + "_" + this.getDATA_GENERATOR_INSERT_QUERIES() + "/input_"
                + String.valueOf(Integer.valueOf(getDATA_GENERATOR_SEED())) + "/");
        success = false;
        directory = new File(getDATA_GENERATOR_OUTPUT_DATASET());
        if (!directory.exists()) {
            success = directory.mkdirs();
            if (!success) {
                try {
                    throw new IOException("Failed to create new directory: " + getDATA_GENERATOR_OUTPUT_DATASET());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Internal initialization function. Assigns the corresponding values to all
     * class fields whose values are defined by the environmental variables and
     * creates the folder of the mimicking output.
     */
    public void internalInit() {

        Map<String, String> env = System.getenv();

        /* mimicking seed */
        if (this.DATA_GENERATOR_SEED == null) {
            if (!env.containsKey(OdinConstants.GENERATOR_SEED)) {
                throw new IllegalArgumentException(
                        "Couldn't get \"" + OdinConstants.GENERATOR_SEED + "\" from the environment. Aborting.");
            }
            setDATA_GENERATOR_SEED(
                    String.valueOf(Integer.valueOf(env.get(OdinConstants.GENERATOR_SEED)) + this.getGeneratorId()));
        }
        /* mimicking population */
        if (!env.containsKey(OdinConstants.GENERATOR_POPULATION)) {
            LOGGER.error("Couldn't get \"" + OdinConstants.GENERATOR_POPULATION + "\" from the properties. Aborting.");
            System.exit(1);
        }
        setDATA_GENERATOR_POPULATION(env.get(OdinConstants.GENERATOR_POPULATION));

        /* name of mimicking algorithm */
        if (!env.containsKey(OdinConstants.GENERATOR_DATASET)) {
            LOGGER.error("Couldn't get \"" + OdinConstants.GENERATOR_DATASET + "\" from the properties. Aborting.");
            System.exit(1);
        }
        setDATA_GENERATOR_DATASET_NAME(env.get(OdinConstants.GENERATOR_DATASET));

        /////////////////
        /* number of insert queries until a select query is performed */
        if (!env.containsKey(OdinConstants.GENERATOR_INSERT_QUERIES_COUNT)) {
            LOGGER.error("Couldn't get \"" + OdinConstants.GENERATOR_INSERT_QUERIES_COUNT
                    + "\" from the properties. Aborting.");
            System.exit(1);
        }
        setDATA_GENERATOR_INSERT_QUERIES(Integer.parseInt(env.get(OdinConstants.GENERATOR_INSERT_QUERIES_COUNT)));

        /* duration of benchmark */
        if (!env.containsKey(OdinConstants.GENERATOR_BENCHMARK_DURATION)) {
            LOGGER.error("Couldn't get \"" + OdinConstants.GENERATOR_BENCHMARK_DURATION
                    + "\" from the properties. Aborting.");
            System.exit(1);
        }
        setBenchmarkEndPoint(Long.parseLong(env.get(OdinConstants.GENERATOR_BENCHMARK_DURATION)));

        /* output folder of mimicking */
        if (!env.containsKey(OdinConstants.GENERATOR_MIMICKING_OUTPUT)) {
            LOGGER.error(
                    "Couldn't get \"" + OdinConstants.GENERATOR_MIMICKING_OUTPUT + "\" from the properties. Aborting.");
            System.exit(1);
        }
        this.createOutputDirectory(env.get(OdinConstants.GENERATOR_MIMICKING_OUTPUT));

    }

    /**
     * Initializes and runs a mimicking algorithm.
     * 
     * @throws IOException
     */
    public void runMimicking() {
        LOGGER.info("Running mimicking algorithm with seed " + getDATA_GENERATOR_SEED());
        // get mimicking type
        MimickingType mimickingType;
        try {
            mimickingType = MimickingFactory.getMimickingType(getDATA_GENERATOR_DATASET_NAME());
        } catch (IOException e1) {
            throw new RuntimeException();
        }

        // get mimicking arguments as a byte array
        String[] arguments;
        try {
            arguments = MimickingFactory.getMimickingArguments(mimickingType, getDATA_GENERATOR_POPULATION(),
                    mimickingType.getOutputType(), getDATA_GENERATOR_OUTPUT_DATASET(), getDATA_GENERATOR_SEED());
        } catch (IOException e1) {
            throw new RuntimeException();

        }

        switch (mimickingType) {
        case TWIG:
            ArrayList<String> twigArg = new ArrayList<String>(Arrays.asList(arguments));
            twigArg.add(0, MimickingType.TWIG.getExecuteCommand());
            try {
                ProcessBuilder pb = new ProcessBuilder(twigArg);
                pb.redirectErrorStream(true); // merge stdout, stderr of process

                Process p = pb.start();
                InputStreamReader isr = new InputStreamReader(p.getInputStream());
                BufferedReader br = new BufferedReader(isr);

                String lineRead;
                while ((lineRead = br.readLine()) != null) {
                    System.out.println(lineRead);
                }

                p.waitFor();
                int v = p.exitValue();
                if (v != 0) {
                    LOGGER.error("TWIG script terminated with exit code " + v);
                    throw new RuntimeException();
                }
            } catch (IOException e) {
                e.printStackTrace(); // or log it, or otherwise handle it
            } catch (InterruptedException ie) {
                ie.printStackTrace(); // or log it, or otherwise handle it
            }
            break;
        case TRANSPORT_DATA:
            DockerBasedMimickingAlg alg = new DockerBasedMimickingAlg(this, mimickingType.getExecuteCommand());
            try {
                alg.generateData(getDATA_GENERATOR_OUTPUT_DATASET(), arguments);
            } catch (Exception e) {
                e.printStackTrace();
                LOGGER.error("TRANSPORT_DATA script terminated.");
                throw new RuntimeException();
            }
            break;
        default:
            LOGGER.error("Unknown mimicking algorithm: " + getDATA_GENERATOR_DATASET_NAME());
            throw new RuntimeException();
        }
        LOGGER.info("Mimicking data has been received.");

    }

    /**
     * Calls the appropriate class that divides the mimicking algorithm's
     * dataset into files based on their time stamp generation.
     * 
     */
    public void divideData() {
        DataProcessor dataProcessor = null;
        dataProcessor = new DataProcessor(getDATA_GENERATOR_OUTPUT_DATASET(), getDATA_GENERATOR_DATASET_NAME());
        dataProcessor.divideData();
    }

    /**
     * Normalizes old time stamps to the benchmark duration interval.
     * 
     * @param files,
     *            a map where the keys are the unique new time stamps and the
     *            values are a set of files that include triples that were
     *            generated at the time stamp of the corresponding key
     */
    public Map<Long, ArrayList<String>> convertTimeStampsToNewInterval(TreeMap<Long, String> files) {

        Map<Long, ArrayList<String>> insertList = new TreeMap<Long, ArrayList<String>>();
        for (Map.Entry<Long, String> entry : files.entrySet()) {
            Long oldTimeStamp = entry.getKey();
            String subFiles = entry.getValue();
            // NewValue = ( ( (OldValue - OldMin) * (NewMax - NewMin) )
            // /
            // (OldMax - OldMin)) + NewMin
            // convert old time stamp to new interval

            BigInteger a = BigInteger.valueOf(oldTimeStamp);
            BigInteger b = BigInteger.valueOf(datasetBeginPoint);
            BigInteger c = a.subtract(b);

            BigInteger d = BigInteger.valueOf(benchmarkEndPoint);
            BigInteger e = BigInteger.valueOf(benchmarkBeginPoint);
            BigInteger f = d.subtract(e);

            BigInteger g = BigInteger.valueOf(datasetEndPoint);
            BigInteger h = BigInteger.valueOf(datasetBeginPoint);
            BigInteger i = g.subtract(h);

            BigInteger newTS = ((c.multiply(f)).divide(i)).add(e);
            if (!insertList.containsKey(newTS.longValue())) {
                insertList.put(newTS.longValue(), new ArrayList<String>());
            }
            insertList.get(newTS.longValue()).add(subFiles);
        }
        LOGGER.info("Number of unique transformed time stamps: " + insertList.values().size());
        return insertList;
    }

    /**
     * Creates streams of triple data. For each unique time stamp, the function
     * creates an INSERT SPARQL query. After a predefined number of files have
     * been converted into INSERT queries, the function creates a SELECT SPARQL
     * query for that particular stream. Additionally, the triples of each
     * INSERT query are loaded into a Jena TDB knowledge base. Once a SELECT
     * query is created, it is also performed against the Jena TDB instance and
     * the result set is stored and will be used as a reference set for this
     * query at the evaluation phase.
     * 
     * @param insertList,
     *            the map of unique time stamps and their corresponding triple
     *            files
     */
    public void createStreams(Map<Long, ArrayList<String>> insertList) {

        long originalPreviousTS = 0l;
        long newPreviousTS = 0l;
        // stream IDs begin with 1
        int streamID = 1;
        int iCounter = 1;
        ReferenceSet reference = new ReferenceSet(getDATA_GENERATOR_OUTPUT_DATASET() + "TDB");

        int rest = insertList.size() % getDATA_GENERATOR_INSERT_QUERIES();
        int d = insertList.size() / getDATA_GENERATOR_INSERT_QUERIES();

        for (Map.Entry<Long, ArrayList<String>> entry : insertList.entrySet()) {
            Long originalCurrentTS = entry.getKey();
            ArrayList<String> files = entry.getValue();

            // insert triple into Jena TDB
            reference.updateTDB(files, defaultGraph);

            long delay = (long) ((originalCurrentTS - originalPreviousTS) / (Math.pow(2, (streamID - 1))));
            // last stream must have 0 delay
            if (d > 1) { // if there is only one stream, keep original delays
                if (rest == 0) {
                    if (streamID == d) {
                        delay = 0l;
                    }
                } else {
                    if (streamID == (d + 1)) {
                        delay = 0l;
                    }
                }
            }
            long newCurrentTS = newPreviousTS + delay;

            InsertQueryInfo insert = new InsertQueryInfo(newCurrentTS, delay);
            insert.createInsertQuery(files, getDATA_GENERATOR_OUTPUT_DATASET(), iCounter, this.defaultGraph);

            originalPreviousTS = originalCurrentTS;
            newPreviousTS = newCurrentTS;

            if (!this.streams.containsKey(streamID)) {
                Stream stream = new Stream(0, new ArrayList<InsertQueryInfo>(), new SelectQueryInfo());
                this.streams.put(streamID, stream);
            }
            this.streams.get(streamID).setID(streamID);
            this.streams.get(streamID).addInsertQuery(insert);

            // if you are at the end of your list and you have some files
            // remaining that do not belong to a stream
            if (((iCounter == insertList.size()) && rest != 0)
                    || ((iCounter % getDATA_GENERATOR_INSERT_QUERIES()) == 0)) {

                // get last insert query
                int lastInsertIndex = this.streams.get(streamID).getSizeOfInserts() - 1;
                InsertQueryInfo lastInsertQuery = this.streams.get(streamID).getInsertQueryInfo(lastInsertIndex);

                // create the select query of the stream
                SelectQueryInfo selectQuery = new SelectQueryInfo();
                long selectQueryDelay = (long) (this.initialSelectDelay / Math.pow(2, (streamID - 1)));
                if (d > 1) { // if there is only one stream, keep original
                             // delays
                    if (rest == 0) {
                        if (streamID == d)
                            selectQueryDelay = 0l;
                    } else {
                        if (streamID == (d + 1))
                            selectQueryDelay = 0l;
                    }
                }
                long selectQueryTS = (long) (newCurrentTS + selectQueryDelay);
                // set the select query delay
                selectQuery.setDelay(selectQueryDelay);
                // set its time stamp
                selectQuery.setTimeStamp(selectQueryTS);
                // create the select query given the last insert query of the
                // current batch
                selectQuery.createSelectQuery(lastInsertQuery.getModelFile(), getDATA_GENERATOR_OUTPUT_DATASET(),
                        streamID, defaultGraph);
                // create a reference set for this select query
                String resultSetFile = reference.queryTDB(selectQuery.getSelectQueryAsString(),
                        getDATA_GENERATOR_OUTPUT_DATASET(), streamID);
                // store the reference set location
                selectQuery.setAnswersFile(resultSetFile);
                // add select query to the stream
                this.streams.get(streamID).setSelectQuery(selectQuery);
                // set begin and end point of stream
                long beginPoint = this.streams.get(streamID).getInsertQueryInfo(0).getTimeStamp();
                this.streams.get(streamID).setBeginPoint(beginPoint);
                this.streams.get(streamID).setEndPoint(selectQueryTS);

            }
            if ((iCounter % getDATA_GENERATOR_INSERT_QUERIES()) == 0) {
                streamID++;
            }

            iCounter++;

        }

    }

    @Override
    public void receiveCommand(byte command, byte[] data) {
        if (OdinConstants.OVERALL_MIN_MAX == command) {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            this.datasetBeginPoint = buffer.getLong();
            this.datasetEndPoint = buffer.getLong();
            minMaxTimestampMutex.release();
        } else if (OdinConstants.BULK_LOAD_FROM_CONTROLLER == command) {
            bulkLoadMutex.release();
        }
        super.receiveCommand(command, data);

    }

    /**
     * Assigns a set of files to a unique time stamp.
     * 
     * @return a map where the keys are the unique time stamps and the values
     *         are a set of files that include triples that were generated at
     *         the time stamp of the corresponding key
     */
    public TreeMap<Long, String> assignFilesToTimeStamps() {

        TreeMap<Long, String> files = new TreeMap<Long, String>();

        BufferedReader TSVFile = null;
        try {
            TSVFile = new BufferedReader(new FileReader(getDATA_GENERATOR_OUTPUT_DATASET() + "timeStamps.tsv"));
            String dataRow = TSVFile.readLine();
            while (dataRow != null) {
                try {
                    String timeStamp = dataRow.split("\t")[0];
                    String fileName = dataRow.split("\t")[1];
                    SimpleDateFormat df = null;

                    if (this.getDATA_GENERATOR_DATASET_NAME().equals("TWIG"))
                        df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                    else
                        df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
                    Date date = df.parse(timeStamp);

                    long newTimeStamp = date.getTime();
                    files.put(newTimeStamp, fileName);
                    dataRow = TSVFile.readLine();
                } catch (IOException | ParseException e) {
                    e.printStackTrace();
                    LOGGER.error("IOException or ParseException");
                    TSVFile.close();
                    throw new RuntimeException();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("IOException");
            throw new RuntimeException();
        }

        try {
            TSVFile.close();
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("Can't close file " + getDATA_GENERATOR_OUTPUT_DATASET() + "timeStamps.tsv");
            throw new RuntimeException();

        }
        LOGGER.info("Number of unique original time stamps: " + files.size());
        return files;
    }

    @Override
    protected void generateData() throws Exception {

        LOGGER.info("Start bulk loading phase for Data Genetaror " + this.getGeneratorId());
        byte[] graph = RabbitMQUtils.writeByteArrays(new byte[][] { RabbitMQUtils.writeString(this.defaultGraph) });
        sendDataToSystemAdapter(graph);
        sendToCmdQueue(OdinConstants.BULK_LOAD_FROM_DATAGENERATOR);
        bulkLoadMutex.acquire();
        LOGGER.info("Bulk loading phase finished for Data Generator " + this.getGeneratorId());

        //////////////////////////////////////////////////////////////////////////////////

        LOGGER.info("Data Generator " + this.getGeneratorId() + " is running..");
        int poolSize = streams.values().size() + streams.size();
        ExecutorService executor = Executors.newFixedThreadPool(poolSize);
        // for each time stamp
        for (Entry<Integer, Stream> entry : streams.entrySet()) {

            // get the the stream
            Stream stream = entry.getValue();
            LOGGER.info("Dealing with stream No." + stream.getID());
            ArrayList<InsertQueryInfo> insertQueries = stream.getInsertQueries();
            long streamBeginPoint = 0l;

            for (int i = 0; i < insertQueries.size(); i++) {
                // insertCounter++;
                // LOGGER.info("Sending INSERT SPARQL query No."+insertCounter);
                InsertQueryInfo currentInsertQuery = insertQueries.get(i);
                InsertThread insertThread = new InsertThread(currentInsertQuery);
                Thread.sleep(currentInsertQuery.getDelay());
                if (i == 0) {
                    streamBeginPoint = System.currentTimeMillis();
                }
                executor.execute(insertThread);

            }
            // selectCounter++;
            // LOGGER.info("Sending SELECT SPARQL query No."+selectCounter);
            SelectQueryInfo selectQuery = stream.getSelectQuery();
            long modelSize = stream.getStreamModelSize();
            SelectThread selectThread = new SelectThread(selectQuery, modelSize, streamBeginPoint);
            Thread.sleep(selectQuery.getDelay());
            executor.execute(selectThread);

        }
        executor.shutdown();
        executor.awaitTermination(2, TimeUnit.HOURS);
        LOGGER.info("Data Generator " + this.getGeneratorId() + " is done.");

    }

    public void setDatasetBeginPoint(long datasetBeginPoint) {
        this.datasetBeginPoint = datasetBeginPoint;
    }

    public byte[] getTask() {
        return task;
    }

    public void setTask(byte[] task) {
        this.task = task;
    }

}
