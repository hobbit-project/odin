package org.hobbit.odin.local;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.hobbit.odin.odindatagenerator.InsertQueryInfo;
import org.hobbit.odin.odindatagenerator.SelectQueryInfo;
import org.junit.Test;

public class SelectQueryInfoTest {

    @Test
    public void test() throws IOException {

        String directory = System.getProperty("user.dir")
                + "/src/test/resources/data/debug_data/selectQuery/insertQueries/";
        FileUtils.deleteDirectory(new File(
                System.getProperty("user.dir") + "/src/test/resources/data/debug_data/referenceSet/selectQuery/selectQueries"));
        File[] listOfFiles = (new File(directory)).listFiles();
        int filesCounter = 1;
        for (File file : listOfFiles) {
            String filePath = directory + file.getName();
            InsertQueryInfo insertQueryInfo = new InsertQueryInfo((long) filesCounter, (long) filesCounter * 100);
            insertQueryInfo.setInsertFile(filePath);
            String insertQueryAsString = insertQueryInfo.getUpdateRequestAsString();

            SelectQueryInfo selectQueryInfo = new SelectQueryInfo((long) filesCounter, null);
            selectQueryInfo.createSelectQuery(insertQueryAsString,
                    System.getProperty("user.dir") + "/src/test/resources/data/debug_data/selectQuery/",
                    filesCounter);

            assertTrue(selectQueryInfo.getSelectQueryAsString() != null);

            filesCounter++;
        }
    }

}
