package org.hobbit.odin.odindatagenerator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.tdb.TDBFactory;
import org.apache.log4j.Logger;

/**
 * Reference Set class. Responsible for creating a Jena TDB, for updating it and
 * for retrieving a result set upon performing a SELECT SPARQL query.
 * 
 * @author Kleanthi Georgala (georgala@informatik.uni-leipzig.de)
 * @version 1.0
 *
 */
public class ReferenceSet {
    /* Location of the Jena TDB */
    private String TDBDirectory = null;
    /* Name of the Jena Dataset */
    private Dataset dataset = null;
    protected static final Logger logger = Logger.getLogger(ReferenceSet.class.getName());

    /* Constructor */
    public ReferenceSet(String directory) {
        this.TDBDirectory = directory;
        this.dataset = TDBFactory.createDataset(this.TDBDirectory);
        
    }

    /**
     * Updates the Jena TDB by inserting new triples to the KB.
     * 
     * @param files,
     *            the set of new files to be added
     */
    public void updateTDB(ArrayList<String> files, String graphName) {
        dataset.begin(ReadWrite.WRITE);
        try {
            Model tdb = dataset.getNamedModel(graphName);
            // read the input files
            for (String file : files) {
                Model m = RDFDataMgr.loadModel(file);
                tdb.add(m);
            }
            dataset.commit();
            tdb.close();
        } finally {
            dataset.end();
        }

    }

    /**
     * Performs a SELECT SPARQL against the current Jena TDB. It stores the
     * result set to a file.
     * 
     * @param selectQuery,
     *            the SELECT SPARQL query to be performed
     * @param outputFolder,
     *            the output folder to store the result set
     * @param streamID,
     *            the ID of the stream the SELECT query belongs to
     * @return the name of the file that the reference set is stored
     */
    public String queryTDB(String selectQuery, String outputFolder, int streamID) {
        outputFolder = outputFolder + "expectedResults/";
        File newFolder = new File(outputFolder);
        if (!newFolder.exists())
            newFolder.mkdir();
        String fileName = outputFolder + "expectedResults" + streamID + ".sparql";

        dataset.begin(ReadWrite.READ);
        ResultSet rs = null;
        try (QueryExecution qExec = QueryExecutionFactory.create(selectQuery, dataset)) {
            rs = qExec.execSelect();
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ResultSetFormatter.outputAsJSON(outputStream, rs);
            logger.info(streamID+" Size of expected: "+outputStream.size());
            try {
                FileUtils.writeByteArrayToFile(new File(fileName), outputStream.toByteArray());
            } catch (IOException e) {
                e.printStackTrace();
            }
                
            
            try {
                outputStream.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } finally {
            dataset.end();
        }

        return fileName;
    }

}
