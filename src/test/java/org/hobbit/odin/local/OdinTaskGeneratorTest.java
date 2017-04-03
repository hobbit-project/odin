package org.hobbit.odin.local;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.query.ResultSetFormatter;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.junit.Test;

public class OdinTaskGeneratorTest {

    @Test
    public void testGenerateTask() {
        Query select = QueryFactory.read(System.getProperty("user.dir")
                + "/src/test/resources/data/debug_data/taskGenerator/selectQuery.sparql");
        if (select.isSelectType() == false)
            throw new RuntimeException("Expected SELECT SPARQL query.");

        byte[] expectedAnswers = null;
        Path path = Paths.get(System.getProperty("user.dir")
                + "/src/test/resources/data/debug_data/taskGenerator/referenceSet.sparql");
        try {
            expectedAnswers = Files.readAllBytes(path);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // select query, modelsize, begin point, end point, answers
        byte[][] answers = new byte[5][];
        String selStr = select.serialize();
        answers[0] = RabbitMQUtils.writeString(selStr);
        answers[1] = RabbitMQUtils.writeString(String.valueOf(100));
        answers[2] = RabbitMQUtils.writeString(String.valueOf(100000));
        answers[3] = RabbitMQUtils.writeString(String.valueOf(200000));
        answers[4] = expectedAnswers;
        byte[] task = RabbitMQUtils.writeByteArrays(answers);
        ////////////////////////////////////////////////////////////////////
        ByteBuffer buffer = ByteBuffer.wrap(task);
        // read the select query for System Adapter
        String selectQuery = RabbitMQUtils.readString(buffer);
        assertTrue(selStr.equals(selectQuery));

        String modelSize = RabbitMQUtils.readString(buffer);
        assertTrue(Long.valueOf(modelSize) == 100l);

        String begin = RabbitMQUtils.readString(buffer);
        assertTrue(Long.valueOf(begin) == 100000);

        String end = RabbitMQUtils.readString(buffer);
        assertTrue(Long.valueOf(end) == 200000);

        InputStream inExpected = new ByteArrayInputStream(
                RabbitMQUtils.readString(buffer).getBytes(StandardCharsets.UTF_8));
        // convert them to ResultSet
        ResultSet expected = ResultSetFactory.fromJSON(inExpected);
        
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ResultSetFormatter.outputAsJSON(outputStream, expected);
        
        assertTrue(Arrays.equals(outputStream.toByteArray(),expectedAnswers));
        
        
    }

}
