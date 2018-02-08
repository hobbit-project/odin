package org.hobbit.odin.local;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.hobbit.odin.odindatagenerator.InsertQueryInfo;
import org.hobbit.odin.odindatagenerator.SelectQueryInfo;
import org.junit.Test;

public class SelectQueryInfoTest {

    @Test
    public void test2() throws Exception {
        String directory = System.getProperty("user.dir")
                + "/src/test/resources/data/debug_data/selectQuery2/insertQueries/";
        FileUtils.deleteDirectory(new File(
                System.getProperty("user.dir") + "/src/test/resources/data/debug_data/selectQuery2/selectQueries"));
        ArrayList<InsertQueryInfo> insertQueries = new ArrayList<InsertQueryInfo>();

        File[] listOfFiles = (new File(directory)).listFiles();
        int filesCounter = 1;
        for (File file : listOfFiles) {
            if (file.getName().endsWith(".ttl"))
                continue;

            String filePath = directory + file.getName();
            InsertQueryInfo insertQueryInfo = new InsertQueryInfo((long) filesCounter, (long) filesCounter * 100);
            insertQueryInfo.setInsertFile(filePath);

            String modelFile = null;
            if (file.getName().indexOf("1") != -1) {
                modelFile = directory + "model1.ttl";
            } else if (file.getName().indexOf("2") != -1)
                modelFile = directory + "model2.ttl";
            else if (file.getName().indexOf("3") != -1)
                modelFile = directory + "model3.ttl";

            insertQueryInfo.setModelFile(modelFile);

            insertQueries.add(insertQueryInfo);
            filesCounter++;
        }

        SelectQueryInfo selectQueryInfo = new SelectQueryInfo((long) filesCounter, null);
        selectQueryInfo.createSelectQuery(insertQueries,
                System.getProperty("user.dir") + "/src/test/resources/data/debug_data/selectQuery2/", 1,
                "http://www.virtuoso-graph.com/");

        assertTrue(selectQueryInfo.getSelectQueryAsString() != null);
        assertTrue(selectQueryInfo.getSelectQueryAsString().indexOf("FROM") != -1);

    }

    
}
