package org.hobbit.odin.odintaskgenerator;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.query.ResultSetFormatter;
import org.hobbit.core.components.AbstractTaskGenerator;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Odin Task Generator. It is responsible for sending the SELECT SPARQL queries
 * to the System Adapter and the reference set of the corresponding SELECT
 * query, the begin and end time stamp of the stream the SELECT query belongs to
 * and the number of triples that were inserted during the duration of that
 * stream to the Evaluation Module.
 * 
 * @author Kleanthi Georgala (georgala@informatik.uni-leipzig.de)
 * @version 1.0
 *
 */
public class OdinTaskGenerator extends AbstractTaskGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(OdinTaskGenerator.class);

    public OdinTaskGenerator() {
        super(1);
    };

    @Override
    public void init() throws Exception {
        LOGGER.info("Initialization begins.");
        super.init();
        LOGGER.info("Initialization is over.");
    }

    @Override
    protected void generateTask(byte[] data) throws Exception {
        // get an ID for the task
        String taskId = getNextTaskId();

        ////////////////// SYSTEM ADAPTER QUERY/////////////////////////////
        ByteBuffer buffer = ByteBuffer.wrap(data);
        // read the select query for System Adapter
        String selectQuery = RabbitMQUtils.readString(buffer);
        // Create the task and the expected answer
        byte[] task = RabbitMQUtils.writeByteArrays(new byte[][] { RabbitMQUtils.writeString(selectQuery) });
        /////////////////////////////////////////////////////////////////////////

        ////////////////// EVALUATION MODULE DATA/////////////////////////////
        String modelSize = RabbitMQUtils.readString(buffer);
        String beginPoint = RabbitMQUtils.readString(buffer);
        // read expected answers from buffer
        InputStream inExpected = new ByteArrayInputStream(
                RabbitMQUtils.readString(buffer).getBytes(StandardCharsets.UTF_8));
        // convert them to ResultSet and then to JSON format
        ResultSet expected = ResultSetFactory.fromJSON(inExpected);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ResultSetFormatter.outputAsJSON(outputStream, expected);

        // convert expected answer into byte[][] and add the model size as well
        byte[][] expectedAnswer = new byte[4][];
        expectedAnswer[0] = RabbitMQUtils.writeString(modelSize);
        expectedAnswer[1] = RabbitMQUtils.writeString(beginPoint);
        long taskSentTimestamp = System.currentTimeMillis();
        expectedAnswer[2] = RabbitMQUtils.writeString(String.valueOf(taskSentTimestamp));
        expectedAnswer[3] = outputStream.toByteArray();
        byte[] expectedAnswerData = RabbitMQUtils.writeByteArrays(expectedAnswer);
        /////////////////////////////////////////////////////////////////////////
        
        // Send the task to the system (and store the timestamp)
        sendTaskToSystemAdapter(taskId, task);
        LOGGER.info("Data sent to System Adapter.");

        sendTaskToEvalStorage(taskId, taskSentTimestamp, expectedAnswerData);
        LOGGER.info("Data sent to Evaluation Storage.");

    }

}
