package org.hobbit.odin.local;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.hobbit.core.Constants;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.odin.odinevaluationmodule.OdinEvaluationModule;
import org.hobbit.odin.odinevaluationmodule.ResultValue;
import org.hobbit.odin.util.OdinConstants;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class OdinEvaluationModuleTest {
    @Rule
    public final EnvironmentVariables envVariablesEvaluationModule = new EnvironmentVariables();

    //@Test
    public void testInternalInit() {
        envVariablesEvaluationModule.set(Constants.HOBBIT_SESSION_ID_KEY, "0");
        envVariablesEvaluationModule.set(Constants.HOBBIT_EXPERIMENT_URI_KEY, Constants.EXPERIMENT_URI_NS + "123");

        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_AVERAGE_TASK_DELAY,
                "http://w3id.org/bench#averageTaskDelay");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MICRO_AVERAGE_RECALL,
                "http://w3id.org/bench#microAverageRecall");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MICRO_AVERAGE_PRECISION,
                "http://w3id.org/bench#microAveragePrecision");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MICRO_AVERAGE_FMEASURE,
                "http://w3id.org/bench#microAverageFmeasure");

        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MACRO_AVERAGE_RECALL,
                "http://w3id.org/bench#macroAverageRecall");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MACRO_AVERAGE_PRECISION,
                "http://w3id.org/bench#macroAveragePrecision");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MACRO_AVERAGE_FMEASURE,
                "http://w3id.org/bench#macroAverageFmeasure");

        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MAX_TPS, "http://w3id.org/bench#maxTPS");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_AVERAGE_TPS, "http://w3id.org/bench#averageTPS");

        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_RECALL,
                "http://w3id.org/bench#tasksRecall");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_TPS,
                "http://w3id.org/bench#tasksTPS");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_DELAY,
                "http://w3id.org/bench#tasksAnswerDelay");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_PRECISION,
                "http://w3id.org/bench#tasksPrecision");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_FMEASURE,
                "http://w3id.org/bench#tasksFmeasure");

        OdinEvaluationModule module = new OdinEvaluationModule();
        module.internalInit();
        assertTrue(module.getEVALUATION_AVERAGE_TASK_DELAY()
                .equals(ResourceFactory.createProperty("http://w3id.org/bench#averageTaskDelay")));

        assertTrue(module.getEVALUATION_MICRO_AVERAGE_RECALL()
                .equals(ResourceFactory.createProperty("http://w3id.org/bench#microAverageRecall")));

        assertTrue(module.getEVALUATION_MICRO_AVERAGE_PRECISION()
                .equals(ResourceFactory.createProperty("http://w3id.org/bench#microAveragePrecision")));

        assertTrue(module.getEVALUATION_MICRO_AVERAGE_FMEASURE()
                .equals(ResourceFactory.createProperty("http://w3id.org/bench#microAverageFmeasure")));

        assertTrue(module.getEVALUATION_MACRO_AVERAGE_RECALL()
                .equals(ResourceFactory.createProperty("http://w3id.org/bench#macroAverageRecall")));

        assertTrue(module.getEVALUATION_MACRO_AVERAGE_PRECISION()
                .equals(ResourceFactory.createProperty("http://w3id.org/bench#macroAveragePrecision")));

        assertTrue(module.getEVALUATION_MACRO_AVERAGE_FMEASURE()
                .equals(ResourceFactory.createProperty("http://w3id.org/bench#macroAverageFmeasure")));

        assertTrue(
                module.getEVALUATION_MAX_TPS().equals(ResourceFactory.createProperty("http://w3id.org/bench#maxTPS")));

        assertTrue(module.getEVALUATION_AVERAGE_TPS()
                .equals(ResourceFactory.createProperty("http://w3id.org/bench#averageTPS")));
    }

    @Test
    public void testEvaluationWithOneTask() {
        envVariablesEvaluationModule.set(Constants.HOBBIT_SESSION_ID_KEY, "Test1");
        envVariablesEvaluationModule.set(Constants.HOBBIT_EXPERIMENT_URI_KEY, Constants.EXPERIMENT_URI_NS + "123");

        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_AVERAGE_TASK_DELAY,
                "http://w3id.org/bench#averageTaskDelay");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MICRO_AVERAGE_RECALL,
                "http://w3id.org/bench#microAverageRecall");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MICRO_AVERAGE_PRECISION,
                "http://w3id.org/bench#microAveragePrecision");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MICRO_AVERAGE_FMEASURE,
                "http://w3id.org/bench#microAverageFmeasure");

        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MACRO_AVERAGE_RECALL,
                "http://w3id.org/bench#macroAverageRecall");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MACRO_AVERAGE_PRECISION,
                "http://w3id.org/bench#macroAveragePrecision");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MACRO_AVERAGE_FMEASURE,
                "http://w3id.org/bench#macroAverageFmeasure");

        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MAX_TPS, "http://w3id.org/bench#maxTPS");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_AVERAGE_TPS, "http://w3id.org/bench#averageTPS");

        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_RECALL,
                "http://w3id.org/bench#tasksRecall");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_TPS,
                "http://w3id.org/bench#tasksTPS");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_DELAY,
                "http://w3id.org/bench#tasksAnswerDelay");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_PRECISION,
                "http://w3id.org/bench#tasksPrecision");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_FMEASURE,
                "http://w3id.org/bench#tasksFmeasure");

        OdinEvaluationModule module = new OdinEvaluationModule();

        module.internalInit();
        // module.setEVALUATION_PARAMETER_KEY("Test1");
        /* FIRST TASK */

        Path path1 = Paths.get(System.getProperty("user.dir")
                + "/src/test/resources/data/debug_data/evaluationModule/referenceSet1.sparql");
        System.out.println(path1.toString());
        byte[] dataExpected = null;
        try {
            dataExpected = Files.readAllBytes(path1);
        } catch (IOException e) {
            e.printStackTrace();
        }

        byte[][] expectedAnswers1 = new byte[4][];
        expectedAnswers1[0] = RabbitMQUtils.writeString(String.valueOf(1000));
        expectedAnswers1[1] = RabbitMQUtils.writeString(String.valueOf(60));
        expectedAnswers1[2] = RabbitMQUtils.writeString(String.valueOf(760));
        expectedAnswers1[3] = dataExpected;

        byte[] expectedTask1 = RabbitMQUtils.writeByteArrays(expectedAnswers1);
        ////////////////////////////////////////////////////////////////////////

        Path path2 = Paths.get(System.getProperty("user.dir")
                + "/src/test/resources/data/debug_data/evaluationModule/receivedSet1.sparql");
        byte[] dataReceived = null;
        try {
            dataReceived = Files.readAllBytes(path2);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            module.evaluateResponse(expectedTask1, dataReceived, 1350, 2000);
            Model model = module.summarizeEvaluation();
            StmtIterator it = model.listStatements();
            while (it.hasNext()) {
                Statement statement = it.next();
                System.out.println(statement.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testEvaluationWithMultipleTasks() {
        envVariablesEvaluationModule.set(Constants.HOBBIT_SESSION_ID_KEY, "Test2");
        envVariablesEvaluationModule.set(Constants.HOBBIT_EXPERIMENT_URI_KEY, Constants.EXPERIMENT_URI_NS + "123");

        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_AVERAGE_TASK_DELAY,
                "http://w3id.org/bench#averageTaskDelay");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MICRO_AVERAGE_RECALL,
                "http://w3id.org/bench#microAverageRecall");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MICRO_AVERAGE_PRECISION,
                "http://w3id.org/bench#microAveragePrecision");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MICRO_AVERAGE_FMEASURE,
                "http://w3id.org/bench#microAverageFmeasure");

        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MACRO_AVERAGE_RECALL,
                "http://w3id.org/bench#macroAverageRecall");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MACRO_AVERAGE_PRECISION,
                "http://w3id.org/bench#macroAveragePrecision");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MACRO_AVERAGE_FMEASURE,
                "http://w3id.org/bench#macroAverageFmeasure");

        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MAX_TPS, "http://w3id.org/bench#maxTPS");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_AVERAGE_TPS, "http://w3id.org/bench#averageTPS");

        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_RECALL,
                "http://w3id.org/bench#tasksRecall");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_TPS,
                "http://w3id.org/bench#tasksTPS");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_DELAY,
                "http://w3id.org/bench#tasksAnswerDelay");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_PRECISION,
                "http://w3id.org/bench#tasksPrecision");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_FMEASURE,
                "http://w3id.org/bench#tasksFmeasure");

        OdinEvaluationModule module = new OdinEvaluationModule();
        module.internalInit();
        // module.setEVALUATION_PARAMETER_KEY("Test2");
        /* FIRST TASK */

        Path path1 = Paths.get(System.getProperty("user.dir")
                + "/src/test/resources/data/debug_data/evaluationModule/referenceSet1.sparql");
        System.out.println(path1.toString());
        byte[] dataExpected = null;
        try {
            dataExpected = Files.readAllBytes(path1);
        } catch (IOException e) {
            e.printStackTrace();
        }

        byte[][] expectedAnswers1 = new byte[4][];
        expectedAnswers1[0] = RabbitMQUtils.writeString(String.valueOf(1100));
        expectedAnswers1[1] = RabbitMQUtils.writeString(String.valueOf(10));
        expectedAnswers1[2] = RabbitMQUtils.writeString(String.valueOf(2000));
        expectedAnswers1[3] = dataExpected;

        byte[] expectedTask1 = RabbitMQUtils.writeByteArrays(expectedAnswers1);
        ////////////////////////////////////////////////////////////////////////

        Path path2 = Paths.get(System.getProperty("user.dir")
                + "/src/test/resources/data/debug_data/evaluationModule/receivedSet1.sparql");
        byte[] dataReceived = null;
        try {
            dataReceived = Files.readAllBytes(path2);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            module.evaluateResponse(expectedTask1, dataReceived, 400, 2000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        ///////////////////////////////////////////////////////////////////////////////////
        /* SECOND TASK */
        path1 = Paths.get(System.getProperty("user.dir")
                + "/src/test/resources/data/debug_data/evaluationModule/referenceSet2.sparql");
        System.out.println(path1.toString());
        dataExpected = null;
        try {
            dataExpected = Files.readAllBytes(path1);
        } catch (IOException e) {
            e.printStackTrace();
        }

        expectedAnswers1 = new byte[4][];
        expectedAnswers1[0] = RabbitMQUtils.writeString(String.valueOf(500));
        expectedAnswers1[1] = RabbitMQUtils.writeString(String.valueOf(10));
        expectedAnswers1[2] = RabbitMQUtils.writeString(String.valueOf(1000));
        expectedAnswers1[3] = dataExpected;

        expectedTask1 = RabbitMQUtils.writeByteArrays(expectedAnswers1);

        path2 = Paths.get(System.getProperty("user.dir")
                + "/src/test/resources/data/debug_data/evaluationModule/receivedSet2.sparql");
        try {
            dataReceived = Files.readAllBytes(path2);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            module.evaluateResponse(expectedTask1, dataReceived, 1350, 2000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        /* THIRD TASK */
        path1 = Paths.get(System.getProperty("user.dir")
                + "/src/test/resources/data/debug_data/evaluationModule/referenceSet3.sparql");
        System.out.println(path1.toString());
        dataExpected = null;
        try {
            dataExpected = Files.readAllBytes(path1);
        } catch (IOException e) {
            e.printStackTrace();
        }

        expectedAnswers1 = new byte[4][];
        expectedAnswers1[0] = RabbitMQUtils.writeString(String.valueOf(1000));
        expectedAnswers1[1] = RabbitMQUtils.writeString(String.valueOf(60));
        expectedAnswers1[2] = RabbitMQUtils.writeString(String.valueOf(760));
        expectedAnswers1[3] = dataExpected;

        expectedTask1 = RabbitMQUtils.writeByteArrays(expectedAnswers1);

        path2 = Paths.get(System.getProperty("user.dir")
                + "/src/test/resources/data/debug_data/evaluationModule/receivedSet3.sparql");
        try {
            dataReceived = Files.readAllBytes(path2);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            module.evaluateResponse(expectedTask1, dataReceived, 450, 3430);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Model model;
        try {
            model = module.summarizeEvaluation();
            StmtIterator it = model.listStatements();
            while (it.hasNext()) {
                Statement statement = it.next();
                System.out.println(statement.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testEvaluationWithUnion() {
        envVariablesEvaluationModule.set(Constants.HOBBIT_SESSION_ID_KEY, "Test4");
        envVariablesEvaluationModule.set(Constants.HOBBIT_EXPERIMENT_URI_KEY, Constants.EXPERIMENT_URI_NS + "123");

        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_AVERAGE_TASK_DELAY,
                "http://w3id.org/bench#averageTaskDelay");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MICRO_AVERAGE_RECALL,
                "http://w3id.org/bench#microAverageRecall");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MICRO_AVERAGE_PRECISION,
                "http://w3id.org/bench#microAveragePrecision");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MICRO_AVERAGE_FMEASURE,
                "http://w3id.org/bench#microAverageFmeasure");

        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MACRO_AVERAGE_RECALL,
                "http://w3id.org/bench#macroAverageRecall");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MACRO_AVERAGE_PRECISION,
                "http://w3id.org/bench#macroAveragePrecision");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MACRO_AVERAGE_FMEASURE,
                "http://w3id.org/bench#macroAverageFmeasure");

        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MAX_TPS, "http://w3id.org/bench#maxTPS");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_AVERAGE_TPS, "http://w3id.org/bench#averageTPS");

        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_RECALL,
                "http://w3id.org/bench#tasksRecall");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_TPS,
                "http://w3id.org/bench#tasksTPS");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_DELAY,
                "http://w3id.org/bench#tasksAnswerDelay");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_PRECISION,
                "http://w3id.org/bench#tasksPrecision");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_FMEASURE,
                "http://w3id.org/bench#tasksFmeasure");

        OdinEvaluationModule module = new OdinEvaluationModule();

        module.internalInit();
        // module.setEVALUATION_PARAMETER_KEY("Test1");
        /* FIRST TASK */

        Path path1 = Paths.get(System.getProperty("user.dir")
                + "/src/test/resources/data/debug_data/evaluationModule/referenceSet4.sparql");
        System.out.println(path1.toString());
        byte[] dataExpected = null;
        try {
            dataExpected = Files.readAllBytes(path1);
        } catch (IOException e) {
            e.printStackTrace();
        }

        byte[][] expectedAnswers1 = new byte[4][];
        expectedAnswers1[0] = RabbitMQUtils.writeString(String.valueOf(1000));
        expectedAnswers1[1] = RabbitMQUtils.writeString(String.valueOf(60));
        expectedAnswers1[2] = RabbitMQUtils.writeString(String.valueOf(760));
        expectedAnswers1[3] = dataExpected;

        byte[] expectedTask1 = RabbitMQUtils.writeByteArrays(expectedAnswers1);
        ////////////////////////////////////////////////////////////////////////

        Path path2 = Paths.get(System.getProperty("user.dir")
                + "/src/test/resources/data/debug_data/evaluationModule/receivedSet4.sparql");
        byte[] dataReceived = null;
        try {
            dataReceived = Files.readAllBytes(path2);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            module.evaluateResponse(expectedTask1, dataReceived, 1350, 2000);
            Model model = module.summarizeEvaluation();
            StmtIterator it = model.listStatements();
            while (it.hasNext()) {
                Statement statement = it.next();
                Resource subject = statement.getSubject();
                if (subject.asResource().getURI().contains("Observation"))
                    System.out.println(statement.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testEvaluationWithUnion2() {
        envVariablesEvaluationModule.set(Constants.HOBBIT_SESSION_ID_KEY, "Test5");
        envVariablesEvaluationModule.set(Constants.HOBBIT_EXPERIMENT_URI_KEY, Constants.EXPERIMENT_URI_NS + "123");

        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_AVERAGE_TASK_DELAY,
                "http://w3id.org/bench#averageTaskDelay");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MICRO_AVERAGE_RECALL,
                "http://w3id.org/bench#microAverageRecall");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MICRO_AVERAGE_PRECISION,
                "http://w3id.org/bench#microAveragePrecision");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MICRO_AVERAGE_FMEASURE,
                "http://w3id.org/bench#microAverageFmeasure");

        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MACRO_AVERAGE_RECALL,
                "http://w3id.org/bench#macroAverageRecall");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MACRO_AVERAGE_PRECISION,
                "http://w3id.org/bench#macroAveragePrecision");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MACRO_AVERAGE_FMEASURE,
                "http://w3id.org/bench#macroAverageFmeasure");

        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_MAX_TPS, "http://w3id.org/bench#maxTPS");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_AVERAGE_TPS, "http://w3id.org/bench#averageTPS");

        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_RECALL,
                "http://w3id.org/bench#tasksRecall");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_TPS,
                "http://w3id.org/bench#tasksTPS");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_DELAY,
                "http://w3id.org/bench#tasksAnswerDelay");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_PRECISION,
                "http://w3id.org/bench#tasksPrecision");
        envVariablesEvaluationModule.set(OdinConstants.EVALUATION_TASKS_EVALUATION_FMEASURE,
                "http://w3id.org/bench#tasksFmeasure");

        OdinEvaluationModule module = new OdinEvaluationModule();

        module.internalInit();
        // module.setEVALUATION_PARAMETER_KEY("Test1");
        /* FIRST TASK */

        Path path1 = Paths.get(System.getProperty("user.dir")
                + "/src/test/resources/data/debug_data/evaluationModule/referenceSet4.sparql");
        System.out.println(path1.toString());
        byte[] dataExpected = null;
        try {
            dataExpected = Files.readAllBytes(path1);
        } catch (IOException e) {
            e.printStackTrace();
        }

        byte[][] expectedAnswers1 = new byte[4][];
        expectedAnswers1[0] = RabbitMQUtils.writeString(String.valueOf(1000));
        expectedAnswers1[1] = RabbitMQUtils.writeString(String.valueOf(60));
        expectedAnswers1[2] = RabbitMQUtils.writeString(String.valueOf(760));
        expectedAnswers1[3] = dataExpected;

        byte[] expectedTask1 = RabbitMQUtils.writeByteArrays(expectedAnswers1);
        ////////////////////////////////////////////////////////////////////////

        Path path2 = Paths.get(System.getProperty("user.dir")
                + "/src/test/resources/data/debug_data/evaluationModule/receivedSet5.sparql");
        byte[] dataReceived = null;
        try {
            dataReceived = Files.readAllBytes(path2);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            module.evaluateResponse(expectedTask1, dataReceived, 1350, 2000);
            Model model = module.summarizeEvaluation();
            StmtIterator it = model.listStatements();
            while (it.hasNext()) {
                Statement statement = it.next();
                Resource subject = statement.getSubject();
                if (subject.asResource().getURI().contains("Observation"))
                    System.out.println(statement.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
