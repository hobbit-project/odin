package org.hobbit.odin.local;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.hobbit.odin.odindatagenerator.ReferenceSet;
import org.junit.Test;

public class ReferenceSetTest {

    @Test
    public void test() throws IOException {
        // once a directory is created it remains there
        // delete expected results folder
        FileUtils.deleteDirectory(new File(
                System.getProperty("user.dir") + "/src/test/resources/data/debug_data/referenceSet/expectedResults/"));

        // delete it and make it again
        FileUtils.deleteDirectory(
                new File(System.getProperty("user.dir") + "/src/test/resources/data/debug_data/referenceSet/TDB/"));
        
        Path path2 = Paths.get(System.getProperty("user.dir") + "/src/test/resources/data/debug_data/referenceSet/TDB/");
        Files.createDirectories(path2);
        String directoryTDB = System.getProperty("user.dir") + "/src/test/resources/data/debug_data/referenceSet/TDB/";
        
        // this stays
        String directoryFiles = System.getProperty("user.dir")
                + "/src/test/resources/data/debug_data/referenceSet/data/";

        String selectQuery = null;
        // this stays
        Query select = QueryFactory.read(
                System.getProperty("user.dir") + "/src/test/resources/data/debug_data/referenceSet/selectQuery.sparql");
        if (select.isSelectType() == false)
            throw new RuntimeException("Expected SELECT SPARQL query.");
        selectQuery = select.serialize();

        ReferenceSet rset = new ReferenceSet(directoryTDB);

        ArrayList<String> resultFiles = new ArrayList<String>();

        File[] listOfFiles = (new File(directoryFiles)).listFiles();
        TreeSet<String> files2 = new TreeSet<String>();
        for (File file : listOfFiles) {
            files2.add(directoryFiles + file.getName());
        }

        int filesCounter = 1;
        for (String filePath : files2) {

            ArrayList<String> files = new ArrayList<String>();
            files.add(filePath);

            rset.updateTDB(files);

            String resultsFile = rset.queryTDB(selectQuery,
                    System.getProperty("user.dir") + "/src/test/resources/data/debug_data/referenceSet/", filesCounter);

            resultFiles.add(resultsFile);

            filesCounter++;
        }

        for (String answerFile : resultFiles) {
            Path path = Paths.get(answerFile);
            byte[] data = null;
            try {
                data = Files.readAllBytes(path);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            InputStream in = new ByteArrayInputStream(data);
            ResultSet received = ResultSetFactory.fromJSON(in);

            assertTrue(received.toString() != null);

        }

    }

}
