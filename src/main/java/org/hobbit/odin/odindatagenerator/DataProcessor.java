package org.hobbit.odin.odindatagenerator;

import java.util.*;
import java.util.Map.Entry;

import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.RDF;
import org.apache.log4j.Logger;
import org.hobbit.odin.util.MainClassProperty;
import org.hobbit.odin.util.TimeStampProperty;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Responsible for the pre-processing of the input data. Divides mimicking
 * algorithm data into files based on the generation time stamp of each event.
 * 
 * @author Kleanthi Georgala (georgala@informatik.uni-leipzig.de)
 * @version 1.0
 *
 */
public class DataProcessor {
    protected static final Logger logger = Logger.getLogger(DataProcessor.class.getName());
    /*
     * Map with keys the unique time stamps and values the set of filenames that
     * include triples generated at the corresponding time stamp
     */
    TreeMap<String, String> timeStamps = new TreeMap<String, String>();
    /* Output directory of all datasets */
    String outputDirectory = null;
    /* Time stamp property of a dataset */
    String timeStampProperty = "";
    /* Main class property of a dataset */
    String classProperty = null;
    /* rdf:type property */
    Property typeProperty = RDF.type;
    HashMap<String, Long> filesCounter = new HashMap<String, Long>();
    int ID;

    /* Constructor */
    public DataProcessor(String outputDirectory, String mimickingDataset, int id) {
        this.outputDirectory = outputDirectory;
        this.getProperties(mimickingDataset);
        filesCounter = new HashMap<String, Long>();
        timeStamps = new TreeMap<String, String>();
        this.ID = id;
    }

    /* Getters */
    public Property getTypeProperty() {
        return typeProperty;
    }

    public String getTimeStampProperty() {
        return timeStampProperty;
    }

    public String getClassProperty() {
        return classProperty;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Assigns the main event class and the main time stamp generation property
     * given a valid name of a mimicking algorithm.
     * 
     * @param mimickingAlgorithm,
     *            name of the mimicking algorithm
     */
    public void getProperties(String mimickingAlgorithm) {
        if (mimickingAlgorithm.contains("TRANSPORT_DATA")) {
            this.classProperty = MainClassProperty.TRANSPORT_DATA_MAINCLASS.mainClassProperty();
            this.timeStampProperty = TimeStampProperty.TRANSPORT_DATA_TIMESTAMP.timeStampProperty();
        } else if (mimickingAlgorithm.contains("TWIG")) {
            this.classProperty = MainClassProperty.TWIG_MAINCLASS.mainClassProperty();
            this.timeStampProperty = TimeStampProperty.TWIG_TIMESTAMP.timeStampProperty();
        } else if (mimickingAlgorithm.contains("TT")) {
            this.classProperty = MainClassProperty.TT_DATA_MAINCLASS.mainClassProperty();
            this.timeStampProperty = TimeStampProperty.TT_DATA_TIMESTAMP.timeStampProperty();
        } else if (mimickingAlgorithm.contains("OBS")) {
            this.classProperty = MainClassProperty.OBS_DATA_MAINCLASS.mainClassProperty();
            this.timeStampProperty = TimeStampProperty.OBS_DATA_TIMESTAMP.timeStampProperty();
        } else {
            logger.error("Invalid mimicking algorithm name " + mimickingAlgorithm);
            throw new RuntimeException();
        }
    }

    /**
     * Creates a model from a specified file.
     * 
     * @param file,
     *            the file that contains the model
     * @return model
     */
    public Model getModel(String file) {
        File dataFile = new File(file);
        if (!dataFile.exists()) {
            logger.error("Data file doesn't exist." + file);
            throw new RuntimeException();
        }
        Model model = ModelFactory.createDefaultModel();
        if (file.endsWith(".ttl"))
            try {
                model.read(file, "ttl");
            } catch (Exception e) {
                logger.error("Couldn't read model from " + file);
                throw new RuntimeException();
            }
        else {
            try {
                model.read(new FileInputStream(file), null, "N-TRIPLES");
            } catch (FileNotFoundException e) {
                logger.error("Couldn't read model from " + file);
                throw new RuntimeException();
            }
        }
        return model;
    }

    /**
     * Returns the sub-models of an existing model that includes (1) all inlinks
     * and corresponding nodes with root a predefined resource and (2) no output
     * links for that predefined resource.
     * 
     * @param resource,
     *            the root resource
     * @param newModel,
     *            the new model
     * @param oldModel,
     *            the existing model
     * 
     * @return the sub-model
     */
    public Model getInLinks(RDFNode resource, Model newModel, Model oldModel) {

        if (newModel.contains(null, null, (RDFNode) resource))
            return ModelFactory.createDefaultModel();
        else {

            StmtIterator it = oldModel.listStatements(null, null, resource);
            while (it.hasNext()) {
                Statement s = it.next();
                if (!newModel.contains(s)) {
                    // there is not need to check if the subject is a resource
                    // because it will used as object on the next call
                    newModel.add(s);
                    newModel.add(getInLinks(s.getSubject(), newModel, oldModel));

                }
            }
        }
        return newModel;
    }

    /**
     * Returns the sub-model of an existing model that includes (1) all outlinks
     * and corresponding nodes with root a predefined resource and (2) no input
     * links for that predefined resource.
     * 
     * @param resource,
     *            the root resource
     * @param newModel,
     *            the new model
     * @param oldModel,
     *            the existing model
     * 
     * @return the sub-model
     */
    public Model getOutLinks(Resource resource, Model newModel, Model oldModel) {

        if (newModel.contains(resource, null, (RDFNode) null))
            return ModelFactory.createDefaultModel();
        else {
            StmtIterator it = oldModel.listStatements(resource, null, (RDFNode) null);
            while (it.hasNext()) {
                Statement s = it.next();
                if (!newModel.contains(s)) {
                    newModel.add(s);
                    // check if the object is a resource because the next call
                    // of this function will use it to find statements were the
                    // object is the subject
                    if (s.getObject().isResource()) {
                        String predicate = s.getPredicate().asResource().getURI();
                        // for connections data only
                        if (!predicate.contains("nextConnection"))
                            newModel.add(getOutLinks(s.getObject().asResource(), newModel, oldModel));
                    }
                }
            }
        }
        return newModel;
    }

    /**
     * Returns the generation time stamp given a resource event and a model.
     * 
     * @param subject,
     *            the predefined resource
     * @param model,
     *            the existing model that includes the resource
     * @return the statement that includes the predefined resource as subject
     *         and the generation time stamp as object
     */
    public Statement getTimeStamp(Resource subject, Model model) {

        String[] timeProperties = timeStampProperty.split("\t");
        Statement timeStampStatement = model
                .listStatements(subject, ResourceFactory.createProperty(timeProperties[0]), (RDFNode) null).next();

        if (timeStampStatement == null) {
            logger.error("Problem with triple: " + timeStampStatement + ". Couldn't parse object.");
            throw new RuntimeException();
        }
        for (int i = 1; i < timeProperties.length; i++) {
            RDFNode object = timeStampStatement.getObject();
            if (!object.isResource()) {
                logger.error("Problem with triple: " + timeStampStatement + ". Object is not a resource.");
                throw new RuntimeException();
            }
            timeStampStatement = model.listStatements(object.asResource(),
                    ResourceFactory.createProperty(timeProperties[i]), (RDFNode) null).next();
        }

        return timeStampStatement;
    }

    /**
     * Given a model and a resource, it creates the subgraph with all inlinks
     * and outlinks from this resource and stores it into a file.
     * 
     * @param folder,
     *            the output folder
     * @param subject,
     *            the predefined subject
     * @param model,
     *            the existing model that includes the subject
     * 
     */
    public void createSubModel(String folder, Resource subject, Model model) {
        // get all outlinks from this URI
        Model outlinks = ModelFactory.createDefaultModel();
        outlinks = getOutLinks(subject, ModelFactory.createDefaultModel(), model);

        // get all inlinks from this URI
        Model inlinks = ModelFactory.createDefaultModel();
        if (!this.classProperty.equals(MainClassProperty.TRANSPORT_DATA_MAINCLASS.mainClassProperty()))
            inlinks = getInLinks((RDFNode) subject, ModelFactory.createDefaultModel(), model);

        // find all their timestamps
        Statement timeStampStatement = getTimeStamp(subject, model);
        // if there are not timestamps found, then we have a problem
        if (timeStampStatement == null) {
            logger.error("No time-stamp found for " + subject.asResource().getURI());
            throw new RuntimeException();
        }

        String timeStamp = timeStampStatement.getObject().asLiteral().getLexicalForm();
        if (!filesCounter.containsKey(timeStamp)) {
            SimpleDateFormat df = null;
            Date date = null;
            try {// 2017-02-03T13:34:44Z
                if (this.classProperty.equals(MainClassProperty.TWIG_MAINCLASS.mainClassProperty()))
                    df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                else if (this.classProperty.equals(MainClassProperty.TT_DATA_MAINCLASS.mainClassProperty()))
                    df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
                else if (this.classProperty.equals(MainClassProperty.OBS_DATA_MAINCLASS.mainClassProperty()))
                    df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                else
                    df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
                date = df.parse(timeStamp);
            } catch (ParseException e) {
                e.printStackTrace();
                logger.error("Couldn't parse date: " + timeStamp);
                throw new RuntimeException();
            }
            filesCounter.put(timeStamp, date.getTime());
        }
        long c = filesCounter.get(timeStamp);

        File newFolder = new File(folder + "clean/");
        if (!newFolder.exists())
            newFolder.mkdir();

        String fileName = folder + "clean/" + c + ".ttl";

        Model existingModel = ModelFactory.createDefaultModel();
        Model newModel = ModelFactory.createDefaultModel();
        if ((new File(fileName).exists())) {
            existingModel.read(fileName);
            newModel.add(existingModel);
        }
        newModel.add(outlinks);
        newModel.add(inlinks);
        FileOutputStream writer = null;
        try {
            writer = new FileOutputStream(fileName, false);
            newModel.write(writer, "ttl");
        } catch (IOException e) {
            logger.error("Couldn't write model in : " + fileName);
            e.printStackTrace();
            throw new RuntimeException();
        }

        timeStamps.put(timeStamp, fileName);

        try {
            writer.close();
        } catch (IOException e) {
            logger.error("Couldn't close file : " + fileName);
            e.printStackTrace();
            throw new RuntimeException();
        }

    }

    /**
     * Writes the time stamps and the corresponding set of files that includes
     * the triple into an output file.
     * 
     * @param output,
     *            the output folder
     */

    public void writeTimeStamps(String output) {

        if (timeStamps.isEmpty()) {
            logger.error("TimeStamps are empty.");
            throw new RuntimeException();
        }

        String logFile = output + "timeStamps.tsv";
        logger.info(ID + " Writing timestamps into file: " + logFile);
        BufferedWriter writer = null;

        try {
            writer = new BufferedWriter(new FileWriter(logFile, true));
        } catch (IOException e1) {
            e1.printStackTrace();
            logger.error("Problem with writing in " + logFile);
            throw new RuntimeException();
        }

        for (Entry<String, String> entry : timeStamps.entrySet()) {
            String date = entry.getKey();
            String file = entry.getValue();
            try {
                writer.write(date + "\t" + file + "\n");
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("Couldn't write in " + logFile);
                try {
                    writer.close();
                } catch (IOException e1) {
                    logger.error("Couldn't close file " + logFile);
                    throw new RuntimeException();
                }
                throw new RuntimeException();
            }
        }
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Couldn't close file " + logFile);
            throw new RuntimeException();
        }

        if (!(new File(logFile)).exists()) {
            logger.error("File " + logFile + " doesn't exist.");
            throw new RuntimeException();
        }
    }

    /**
     * Get statements from an input model given the main class property of the
     * corresponding data.
     * 
     * @param model,
     *            the input model
     * @return the set of statements that include as subjects the different
     *         events, as described in the main class property
     */
    public StmtIterator getStatements(Model model) {
        StmtIterator statements = null;
        if (this.classProperty.equals(MainClassProperty.TRANSPORT_DATA_MAINCLASS.mainClassProperty()))
            statements = model.listStatements(null, typeProperty, ResourceFactory.createResource(classProperty));
        else if (this.classProperty.equals(MainClassProperty.TWIG_MAINCLASS.mainClassProperty()))
            statements = model.listStatements(null, typeProperty, ResourceFactory.createResource(classProperty));
        else if (this.classProperty.equals(MainClassProperty.TT_DATA_MAINCLASS.mainClassProperty())) {
            Property property = ResourceFactory.createProperty(MainClassProperty.TT_DATA_MAINCLASS.mainClassProperty());
            statements = model.listStatements(null, property, (RDFNode) null);
        } else if (this.classProperty.equals(MainClassProperty.OBS_DATA_MAINCLASS.mainClassProperty()))
            statements = model.listStatements(null, typeProperty, ResourceFactory.createResource(classProperty));
        if (statements == null) {
            logger.error("Model includes no subjects of type " + classProperty);
            throw new RuntimeException();
        }
        return statements;
    }

    /**
     * Get the main event resource.
     * 
     * @param statement,
     *            the statement that includes the main event.
     * @return main event resource
     */
    public Resource getMainResource(Statement statement) {
        Resource mainResource = null;
        if (this.classProperty.equals(MainClassProperty.TRANSPORT_DATA_MAINCLASS.mainClassProperty())) {
            mainResource = statement.getSubject();
        } else if (this.classProperty.equals(MainClassProperty.TWIG_MAINCLASS.mainClassProperty())) {
            mainResource = statement.getSubject();
        } else if (this.classProperty.equals(MainClassProperty.TT_DATA_MAINCLASS.mainClassProperty())) {
            mainResource = (Resource) statement.getObject();
        } else if (this.classProperty.equals(MainClassProperty.OBS_DATA_MAINCLASS.mainClassProperty())) {
            mainResource = (Resource) statement.getSubject();
        }
        if (mainResource == null) {
            logger.error("Statement " + statement.toString() + " includes no subjects of type " + classProperty);
            throw new RuntimeException();
        }
        return mainResource;
    }

    /**
     * Divides all generated triple into files based on their generation time
     * stamp.
     * 
     */
    public void divideData() {
        // output directory of all mimicking files
        String fullNameDirectory = outputDirectory;
        File[] listOfFiles = (new File(fullNameDirectory)).listFiles();

        if (listOfFiles.length == 0) {
            logger.error("Mimicking algorithm did not return any data files.");
            throw new RuntimeException();
        }
        if (this.classProperty.equals(MainClassProperty.OBS_DATA_MAINCLASS.mainClassProperty())
                || this.classProperty.equals(MainClassProperty.TT_DATA_MAINCLASS.mainClassProperty())) {
            int sizeCounter = 0;
            int eventCounter = 0;
            for (File file : listOfFiles) {
                Model model = getModel(fullNameDirectory + file.getName());
                sizeCounter += model.size();
                StmtIterator statements = getStatements(model);

                // find all events that are instances of classProperty
                while (statements.hasNext()) {
                    eventCounter++;
                    Statement statement = statements.next();
                    Resource mainResource = getMainResource(statement);
                    createSubModel(fullNameDirectory, mainResource, model);

                }

            }
            logger.info(ID + " Number of triples: " + sizeCounter);
            logger.info(ID + " Number of events: " + eventCounter);
            writeTimeStamps(fullNameDirectory);

        } else {
            Model model = getModel(fullNameDirectory + listOfFiles[0].getName());
            logger.info(ID + " Number of triples: " + model.size());

            StmtIterator statements = getStatements(model);
            int counter = 0;
            // find all events that are instances of classProperty
            while (statements.hasNext()) {
                counter++;
                Statement statement = statements.next();
                Resource mainResource = getMainResource(statement);
                createSubModel(fullNameDirectory, mainResource, model);

            }
            logger.info(ID + " Number of events: " + counter);
            writeTimeStamps(fullNameDirectory);
        }

    }

}
