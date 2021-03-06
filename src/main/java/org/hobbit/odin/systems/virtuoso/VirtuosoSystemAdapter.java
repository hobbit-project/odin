package org.hobbit.odin.systems.virtuoso;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.aksw.jena_sparql_api.core.FluentQueryExecutionFactory;
import org.aksw.jena_sparql_api.core.QueryExecutionFactory;
import org.aksw.jena_sparql_api.core.UpdateExecutionFactoryHttp;
import org.aksw.jena_sparql_api.core.utils.UpdateRequestUtils;
import org.aksw.jena_sparql_api.http.QueryExecutionFactoryHttp;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.jena.atlas.web.auth.HttpAuthenticator;
import org.apache.jena.atlas.web.auth.SimpleAuthenticator;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.update.UpdateRequest;
import org.hobbit.core.components.AbstractSystemAdapter;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Virtuoso System Adapter class for Odin Benchmark.
 * 
 * @author Kleanthi Georgala (georgala@informatik.uni-leipzig.de)
 * @version 1.0
 *
 */
public class VirtuosoSystemAdapter extends AbstractSystemAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(VirtuosoSystemAdapter.class);
    public String virtuosoContName = null;

    private int selectsReceived = 0;
    private int selectsProcessed = 0;

    private int insertsReceived = 0;
    private int insertsProcessed = 0;
    private org.aksw.jena_sparql_api.core.QueryExecutionFactory queryExecFactory;
    private org.aksw.jena_sparql_api.core.UpdateExecutionFactory updateExecFactory;

    private AtomicInteger totalReceived = new AtomicInteger(0);
    private AtomicInteger totalSent = new AtomicInteger(0);

    private boolean phase2 = true;

    List<String> graphUris = new ArrayList<String>();

    public VirtuosoSystemAdapter() {
    }

    public VirtuosoSystemAdapter(int numberOfMessagesInParallel) {
        super(numberOfMessagesInParallel);
    }

    @Override
    public void init() throws Exception {
        LOGGER.info("Initialization begins.");
        super.init();
        internalInit();
        LOGGER.info("Initialization is over.");

    }

    /**
     * Internal initialization function. It builds the virtuoso image and runs
     * the container.
     * 
     */
    public void internalInit() {
        String[] envVariablesVirtuoso = new String[] { "SPARQL_UPDATE=true",
                "DEFAULT_GRAPH=http://www.virtuoso-graph.com/" };
        virtuosoContName = this.createContainer("tenforce/virtuoso:latest", envVariablesVirtuoso);

        QueryExecutionFactory qef = FluentQueryExecutionFactory.http("http://" + virtuosoContName + ":8890/sparql")
                .config().withPagination(50000).end().create();

        ResultSet testResults = null;
        while (testResults == null) {
            LOGGER.info("Using " + "http://" + virtuosoContName + ":8890/sparql" + " to run test select query");

            // Create a QueryExecution object from a query string ... // and
            // runit
            QueryExecution qe = qef.createQueryExecution("SELECT * { ?s a <http://ex.org/foo/bar> } LIMIT 1");
            try {
                TimeUnit.SECONDS.sleep(2);
                testResults = qe.execSelect();
            } catch (Exception e) {
            } finally {
                qe.close();
            }
        }
        qef.close();

        // create execution factory
        queryExecFactory = new QueryExecutionFactoryHttp("http://" + virtuosoContName + ":8890/sparql");
        // queryExecFactory = new
        // QueryExecutionFactoryPaginated(queryExecFactory, 100);

        // create update factory
        HttpAuthenticator auth = new SimpleAuthenticator("dba", "dba".toCharArray());
        updateExecFactory = new UpdateExecutionFactoryHttp("http://" + virtuosoContName + ":8890/sparql", auth);
    }

    @Override
    public void receiveCommand(byte command, byte[] data) {
        if (VirtuosoSystemAdapterConstants.BULK_LOAD_DATA_GEN_FINISHED == command) {

            Thread thread = new Thread() {
                public void run() {

                    ByteBuffer buffer = ByteBuffer.wrap(data);
                    int numberOfMessages = buffer.getInt();
                    boolean lastBulkLoad = buffer.get() != 0;

                    int messagesSent = totalSent.addAndGet(numberOfMessages);
                    int messagesReceived = totalReceived.get();

                    while (messagesSent != messagesReceived) {
                        LOGGER.info("Messages received and sent are not equal "+messagesSent+" "+messagesReceived);
                        
                        try {
                            TimeUnit.SECONDS.sleep(2);

                        } catch (Exception e) {
                        }
                        messagesReceived = totalReceived.get();

                    }
                    LOGGER.info("Bulk phase begins");
                    // LOGGER.info("Received graph URIs:"+graphUris.size());
                    for (String uri : graphUris) {
                        // LOGGER.info(uri);
                        String create = "CREATE GRAPH " + "<" + uri + ">";
                        UpdateRequest updateRequest = UpdateRequestUtils.parse(create);
                        updateExecFactory.createUpdateProcessor(updateRequest).execute();
                    }

                    try {
                        sendToCmdQueue(VirtuosoSystemAdapterConstants.BULK_LOADING_DATA_FINISHED);
                    } catch (IOException e) {
                        e.printStackTrace();
                        LOGGER.error("Couldn't send signal to BenchmarkController that bulk phase is over");
                        throw new RuntimeException();
                    }
                    LOGGER.info("Bulk phase is over.");
                }
            };

            thread.start();
            phase2 = false;

        }
        super.receiveCommand(command, data);
    }

    @Override
    public void receiveGeneratedData(byte[] arg0) {
        if (phase2 == true) {
            ByteBuffer buffer = ByteBuffer.wrap(arg0);
            // read the graph URI
            String graphUri = RabbitMQUtils.readString(buffer);
            LOGGER.info("Receiving graph URI " + graphUri);
            graphUris.add(graphUri);
            this.totalReceived.incrementAndGet();
        } else {
            //LOGGER.info("INSERT SPARQL query received.");
            this.insertsReceived++;
            ByteBuffer buffer = ByteBuffer.wrap(arg0);
            // read the insert query
            String insertQuery = RabbitMQUtils.readString(buffer);
            // insert query
            UpdateRequest updateRequest = UpdateRequestUtils.parse(insertQuery);

            try {
                updateExecFactory.createUpdateProcessor(updateRequest).execute();
            } catch (Exception e) {
                e.printStackTrace();
                LOGGER.error("Couldn't execute " + updateRequest.toString());
                LOGGER.error("Couldn't execute " + insertQuery);
                throw new RuntimeException();
            }

            //LOGGER.info("INSERT SPARQL query has been processed.");
            this.insertsProcessed++;
        }

    }

    @Override
    public void receiveGeneratedTask(String arg0, byte[] arg1) {
        //LOGGER.info("SELECT SPARQL query received.");
        this.selectsReceived++;
        String taskId = arg0;
        // read select query
        ByteBuffer buffer = ByteBuffer.wrap(arg1);
        String selectQuery = RabbitMQUtils.readString(buffer);

        // Create a QueryExecution object from a query string ...
        QueryExecution qe = queryExecFactory.createQueryExecution(selectQuery);
        // and run it.

        ResultSet results = qe.execSelect();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ResultSetFormatter.outputAsJSON(outputStream, results);
        
        try {
            this.sendResultToEvalStorage(taskId, outputStream.toByteArray());
        } catch (IOException e) {
            LOGGER.error("Got an exception while sending results to evaluation storage.", e);
            try {
                outputStream.close();
            } catch (IOException e1) {
                e1.printStackTrace();
                LOGGER.error("Couldn't close ByteArrayOutputStream");
                throw new RuntimeException();
            }
            throw new RuntimeException();
        }
        try {
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("Couldn't close ByteArrayOutputStream");
            throw new RuntimeException();
        }
        qe.close();

        //LOGGER.info("SELECT SPARQL query has been processed.");
        this.selectsProcessed++;

    }

    @Override
    public void close() throws IOException {
        if (this.insertsProcessed != this.insertsReceived) {
            LOGGER.error("INSERT queries received and processed are not equal:"+this.insertsProcessed+" "+this.insertsReceived);
        }
        if (this.selectsProcessed != this.selectsReceived) {
            LOGGER.error("SELECT queries received and processed are not equal:"+this.selectsProcessed+" "+this.selectsReceived);
        }

        try {
            queryExecFactory.close();
        } catch (Exception e) {
        }
        try {
            updateExecFactory.close();
        } catch (Exception e) {
        }
        this.stopContainer(virtuosoContName);
        super.close();
        LOGGER.info("Virtuoso has stopped.");

    }

}