package org.hobbit.odin.systems.virtuoso;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.aksw.commons.collections.diff.Diff;
import org.aksw.jena_sparql_api.core.DatasetListener;
import org.aksw.jena_sparql_api.core.SparqlService;
import org.aksw.jena_sparql_api.core.UpdateContext;
import org.aksw.jena_sparql_api.core.utils.UpdateRequestUtils;
import org.aksw.jena_sparql_api.http.QueryExecutionFactoryHttp;
import org.aksw.jena_sparql_api.pagination.core.QueryExecutionFactoryPaginated;
import org.aksw.jena_sparql_api.update.FluentSparqlService;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.jena.atlas.web.auth.HttpAuthenticator;
import org.apache.jena.atlas.web.auth.SimpleAuthenticator;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.update.UpdateRequest;
import org.hobbit.core.components.*;
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
    public String containerName = null;
    /* for debugging purposes */
    private boolean flag = true;

    public VirtuosoSystemAdapter() {
    }

    public VirtuosoSystemAdapter(int numberOfMessagesInParallel) {
        super(numberOfMessagesInParallel);
    }

    /* Getters and Setters */
    public boolean isFlag() {
        return flag;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }

    private ResultSet results;

    public ResultSet getResults() {
        return results;
    }

    public void setResults(ResultSet results) {
        this.results = results;
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
        containerName = this.createContainer("tenforce/virtuoso:1.1.1-virtuoso7.2.2", envVariablesVirtuoso);

        String test = "select ?x ?p ?o \n" + "where { \n" + "?x ?p ?o \n" + "}";

        ResultSet testResults = null;
        while (testResults == null) {
            LOGGER.info("Using " + "http://" + containerName + ":8890/sparql" + " to run test select query");
            org.aksw.jena_sparql_api.core.QueryExecutionFactory qef = new QueryExecutionFactoryHttp(
                    "http://" + containerName + ":8890/sparql", "http://www.virtuoso-graph.com/");

            /*
             * LOGGER.info("QueryExecutionFactoryRetry"); qef = new
             * QueryExecutionFactoryRetry(qef, 5, 1000);
             * LOGGER.info("QueryExecutionFactoryDelay"); // Add delay in order
             * to be nice to the remote server (delay in // milli seconds) qef =
             * new QueryExecutionFactoryDelay(qef, 5000);
             */
            QueryExecutionFactoryHttp foo = qef.unwrap(QueryExecutionFactoryHttp.class);
            // Add pagination
            LOGGER.info("QueryExecutionFactoryPaginated");
            qef = new QueryExecutionFactoryPaginated(qef, 100);
            // Create a QueryExecution object from a query string ...
            LOGGER.info("createQueryExecution");
            QueryExecution qe = qef.createQueryExecution(test);
            // and run it.
            LOGGER.info("execSelect");
            testResults = qe.execSelect();
        }

    }

    @Override
    public void receiveGeneratedData(byte[] arg0) {
        LOGGER.info("INSERT SPARQL query received.");

        ByteBuffer buffer = ByteBuffer.wrap(arg0);
        // read the insert query
        String insertQuery = RabbitMQUtils.readString(buffer);

        // insert query
        List<DatasetListener> listeners = Collections.<DatasetListener> singletonList(new DatasetListener() {
            @Override
            public void onPreModify(Diff<Set<Quad>> diff, UpdateContext updateContext) {
                // Print out any changes to the console
                System.out.println(diff);
            }
        });
        HttpAuthenticator auth = new SimpleAuthenticator("dba", "dba".toCharArray());
        SparqlService sparqlService = FluentSparqlService
                .http("http://" + containerName + ":8890/sparql", "http://www.virtuoso-graph.com/", auth)// service,
                // graph
                .config().configQuery().withPagination(100).end().end().create();

        UpdateRequest updateRequest = UpdateRequestUtils.parse(insertQuery);

        sparqlService.getUpdateExecutionFactory().createUpdateProcessor(updateRequest).execute();
        LOGGER.info("INSERT SPARQL query has been processed.");

    }

    @Override
    public void receiveGeneratedTask(String arg0, byte[] arg1) {
        LOGGER.info("SELECT SPARQL query received.");

        String taskId = arg0;
        // read select query
        ByteBuffer buffer = ByteBuffer.wrap(arg1);
        String selectQuery = RabbitMQUtils.readString(buffer);

        LOGGER.info("Using " + "http://" + containerName + ":8890/sparql" + " to run construct query");
        org.aksw.jena_sparql_api.core.QueryExecutionFactory qef = new QueryExecutionFactoryHttp(
                "http://" + containerName + ":8890/sparql", "http://www.virtuoso-graph.com/");

        /*
         * LOGGER.info("QueryExecutionFactoryRetry"); qef = new
         * QueryExecutionFactoryRetry(qef, 5, 1000);s
         * LOGGER.info("QueryExecutionFactoryDelay"); // Add delay in order to
         * be nice to the remote server (delay in // milli seconds) qef = new
         * QueryExecutionFactoryDelay(qef, 5000);
         */
        QueryExecutionFactoryHttp foo = qef.unwrap(QueryExecutionFactoryHttp.class);
        // Add pagination
        LOGGER.info("QueryExecutionFactoryPaginated");
        qef = new QueryExecutionFactoryPaginated(qef, 100);
        // Create a QueryExecution object from a query string ...
        LOGGER.info("createQueryExecution");
        QueryExecution qe = qef.createQueryExecution(selectQuery);
        // and run it.
        LOGGER.info("execSelect");
        results = qe.execSelect();
        // serialize results

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ResultSetFormatter.outputAsJSON(outputStream, results);

        if (flag == true) {
            try {
                this.sendResultToEvalStorage(taskId, outputStream.toByteArray());
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        LOGGER.info("SELECT SPARQL query has been processed.");

    }

    @Override
    public void close() throws IOException {
        super.close();
        this.stopContainer(containerName);
        LOGGER.info("Virtuoso has stopped.");
    }

}
