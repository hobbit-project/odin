package org.hobbit.odin.odindatagenerator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.hobbit.storage.queries.SparqlQueries;

/**
 * Insert Query class. Responsible for creating and storing an INSERT SPARQL
 * query given a set of files.
 * 
 * @author Kleanthi Georgala (georgala@informatik.uni-leipzig.de)
 * @version 1.0
 *
 */
public class InsertQueryInfo {
    /* Time stamp of executing the INSERT query */
    private long timeStamp = 0l;
    /*
     * Delay of executing the INSERT query from the beginning of the benchmark
     */
    private long delay = 0l;
    /* Location file where the query is stored */
    private String insertFile = null;
    /* Number of triples included in the query */
    private long modelSize = 0l;
    /* Location file where the model is stored */
    private String modelFile = null;

    /* Getters and Setters */
    public String getModelFile() {
        return modelFile;
    }

    public void setModelFile(String modelFile) {
        this.modelFile = modelFile;
    }

    public long getModelSize() {
        return modelSize;
    }

    public void setModelSize(long modelSize) {
        this.modelSize = modelSize;
    }

    public InsertQueryInfo(long ts, long d) {
        this.timeStamp = ts;
        this.delay = d;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public long getDelay() {
        return delay;
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }

    public String getInsertFile() {
        return this.insertFile;
    }

    public void setInsertFile(String files) {
        this.insertFile = files;
    }

    /**
     * Creates an INSERT SPARQL query given a set of rdf files. All files are
     * loaded into a Jena Model and then the Model is converted into an Update
     * Request. Finally the query is stored into a file using UTF-8 encoding.
     * 
     * @param files,
     *            the set of input files
     * @param outputFolder,
     *            the folder to store the query
     * @param insertCounter,
     *            the ID of the query
     */
    public void createInsertQuery(ArrayList<String> files, String outputFolder, int insertCounter, String graphName) {
        outputFolder = outputFolder + "insertQueries/";
        File newFolder = new File(outputFolder);
        if (!newFolder.exists())
            newFolder.mkdir();

        Model completeModel = ModelFactory.createDefaultModel();
        // create model from the files with the same dilatation factor
        for (String file : files) {
            Model model = RDFDataMgr.loadModel(file);
            completeModel.add(model);
        }
        // save model for the select query
        modelFile = outputFolder + "model" + insertCounter + ".ttl";
        try {
            OutputStream o = new FileOutputStream(modelFile);
            completeModel.write(o, "ttl");

        } catch (FileNotFoundException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        // create insert query
        String str = SparqlQueries.getUpdateQueryFromDiff(ModelFactory.createDefaultModel(), completeModel, graphName);
        UpdateRequest insertQuery = UpdateFactory.create(str);
        // save to output
        OutputStream outStream = null;
        String fileName = outputFolder + "insertQuery" + insertCounter + ".sparql";
        try {
            outStream = new FileOutputStream(fileName);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        IndentedWriter out = new IndentedWriter(outStream);
        insertQuery.output(out);

        this.insertFile = fileName;
        this.modelSize = completeModel.size();
    }

    /**
     * Reads the INSERT SPARQL query from a file and retrieves as UTF-8 encoded
     * String.
     * 
     * @return UTF-8 encoded String representation of the INSERT SPARQL query
     */
    public String getUpdateRequestAsString() {
        String fileContent = null;
        try {
            fileContent = FileUtils.readFileToString(new File(this.insertFile), Charsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileContent;
    }

}
