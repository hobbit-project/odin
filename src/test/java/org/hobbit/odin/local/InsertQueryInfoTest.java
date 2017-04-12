package org.hobbit.odin.local;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.aksw.jena_sparql_api.core.utils.UpdateRequestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.modify.request.UpdateData;
import org.apache.jena.sparql.modify.request.UpdateDataInsert;
import org.apache.jena.update.Update;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.apache.log4j.Logger;
import org.hobbit.odin.odindatagenerator.InsertQueryInfo;
import org.junit.Test;

public class InsertQueryInfoTest {
    protected static final Logger logger = Logger.getLogger(InsertQueryInfoTest.class.getName());

    @Test
    public void test() throws Exception {
        logger.info("Testing Insert Query Info");

        FileUtils.deleteDirectory(new File(
                System.getProperty("user.dir") + "/src/test/resources/data/debug_data/insertQuery/insertQueries/"));

        String directory = System.getProperty("user.dir") + "/src/test/resources/data/debug_data/insertQuery/";
        File[] listOfFiles = (new File(directory + "/clean/")).listFiles();

        int filesCounter = 0;
        for (File file : listOfFiles) {
            filesCounter++;
            String filePath = directory + "/clean/" + file.getName();

            InsertQueryInfo insertQueryInfo = new InsertQueryInfo((long) filesCounter, (long) filesCounter * 100);
            ArrayList<String> files = new ArrayList<String>();
            files.add(filePath);
            insertQueryInfo.createInsertQuery(files, directory, filesCounter, "http://www.virtuoso-graph.com/");

            // check if insert file field is filled
            assertTrue(insertQueryInfo.getInsertFile() != null);
            // check if insert file exists
            assertTrue(new File(insertQueryInfo.getInsertFile()).exists() == true);
            // check if insert query model is not zero
            assertTrue(insertQueryInfo.getModelSize() != 0);

            // check if insert query is not empty
            String fileContent = insertQueryInfo.getUpdateRequestAsString();
            assertTrue(fileContent != null);

        }
        assertTrue(filesCounter == 2);
    }

}
