package org.hobbit.odin.odindatagenerator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.log4j.Logger;

/**
 * Select Query class. Responsible for creating and storing a SELECT SPARQL
 * query given a file.
 * 
 * @author Kleanthi Georgala (georgala@informatik.uni-leipzig.de)
 * @version 1.0
 *
 */
public class SelectQueryInfo {

    protected static final Logger logger = Logger.getLogger(SelectQueryInfo.class.getName());

    /* Time stamp of executing the SELECT query */
    private long timeStamp;
    /*
     * Delay of executing the SELECT query from the beginning of the benchmark
     */
    private long delay;
    /* Location file where the query is stored */
    private String selectQueryFile = null;
    /* Location file where the reference set is stored */
    private String answersFile = null;

    /* Constructors */
    public SelectQueryInfo() {
    }

    public SelectQueryInfo(long ts, String file) {
        this.timeStamp = ts;
        this.selectQueryFile = file;
    }

    /* Getters and Setters */
    public String getAnswersFile() {
        return answersFile;
    }

    public void setAnswersFile(String answersFile) {
        this.answersFile = answersFile;
    }

    public long getDelay() {
        return delay;
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    /**
     * Reads the SELECT SPARQL query from a file and retrieves as UTF-8 encoded
     * String.
     * 
     * @return a UTF-8 encoded String representation of the SELECT query
     */
    public String getSelectQueryAsString() {
        Query select = QueryFactory.read(this.selectQueryFile);
        if (select.isSelectType() == false) {
            logger.error("Expected SELECT SPARQL query.");
            throw new RuntimeException();
        }
        return select.serialize();
    }

    /**
     * Reads the reference set from a file and retrieves it as a byte array.
     * 
     * @return the reference set of the current SELECT query as byte array
     */
    public byte[] getExpectedAnswers() {
        Path path = Paths.get(this.answersFile);
        byte[] data = null;
        try {
            data = Files.readAllBytes(path);
        } catch (IOException e) {
            logger.error("Unable to retrieve expected answers from " + this.answersFile);
            throw new RuntimeException();
        }
        return data;
    }

    /**
     * Creates a SELECT SPARQL query given an INSERT SPARQL query (as a UTF-8
     * encoded String) and stores it into a file. This method uses a Least
     * General Generalization (LGG) technique. The aim of this function is
     * twofold: create a SELECT query (1) with the least possible triple
     * patterns so that (2) the resulting query is able identify if all the
     * triples of the input INSERT query were inserted into the triple store.
     * Firstly, each triple is converted into a triple pattern with one
     * variable. For each variable and its corresponding RDFNode, the function
     * identifies to which initial triples the pair is found in. Then, the pairs
     * of variables and RDFNodes are ordered (descending) based on the number of
     * the initial triples that they are found in. Finally, the algorithm starts
     * with the most popular variable-RDFNode pair, selects the corresponding
     * triple patterns and adds them to the SELECT query. The algorithm
     * continues until all initial triples are covered. No initial triples are
     * covered more than once.
     * 
     * 
     * @param insertQueryAsString,
     *            a UTF-8 encoded String representation of the INSERT query
     * @param outputFolder,
     *            the output folder to store the SELECT query
     * @param streamCounter,
     *            the ID of the stream that the SELECT query belongs to
     */
    public void createSelectQuery(String modelFile, String outputFolder, int streamCounter, String graphName) {

        outputFolder = outputFolder + "selectQueries/";
        File newFolder = new File(outputFolder);
        if (!newFolder.exists())
            newFolder.mkdir();

        Model model = RDFDataMgr.loadModel(modelFile);
        HashMap<Integer, ArrayList<Triple>> patternsToTriples = new HashMap<Integer, ArrayList<Triple>>();
        Map<Node, HashMap<Integer, ArrayList<Integer>>> resultSet = new HashMap<Node, HashMap<Integer, ArrayList<Integer>>>();
        StmtIterator it = model.listStatements();
        int quadCounter = 1;
        
        while (it.hasNext()) {
            Statement statement = it.next();
            Triple triple = statement.asTriple();

            Node subject = triple.getSubject();
            Node predicate = triple.getPredicate();
            Node object = triple.getObject();

            // create one triple pattern for each triple Node
            Triple newTriple = null;
            patternsToTriples.put(quadCounter, new ArrayList<Triple>());

            // for subject:
            Node subjectVariable = NodeFactory.createVariable("?");
            newTriple = new Triple(subjectVariable, predicate, object);

            patternsToTriples.get(quadCounter).add(0, newTriple);

            if (!resultSet.containsKey(subject)) {
                resultSet.put(subject, new HashMap<Integer, ArrayList<Integer>>());
            }
            HashMap<Integer, ArrayList<Integer>> map = resultSet.get(subject);
            if (!map.containsKey(quadCounter)) {
                map.put(quadCounter, new ArrayList<Integer>());
            }
            map.get(quadCounter).add(0);
            //////////////////////////////

            // for predicate:
            Node predicateVariable = NodeFactory.createVariable("?");
            newTriple = new Triple(subject, predicateVariable, object);
            patternsToTriples.get(quadCounter).add(1, newTriple);

            if (!resultSet.containsKey(predicate)) {
                resultSet.put(predicate, new HashMap<Integer, ArrayList<Integer>>());
            }
            map = resultSet.get(predicate);
            if (!map.containsKey(quadCounter)) {
                map.put(quadCounter, new ArrayList<Integer>());
            }
            map.get(quadCounter).add(1);
            //////////////////////////////

            // for object:
            Node objectVariable = NodeFactory.createVariable("?");
            newTriple = new Triple(subject, predicate, objectVariable);
            patternsToTriples.get(quadCounter).add(2, newTriple);

            if (!resultSet.containsKey(object)) {
                resultSet.put(object, new HashMap<Integer, ArrayList<Integer>>());
            }
            map = resultSet.get(object);
            if (!map.containsKey(quadCounter)) {
                map.put(quadCounter, new ArrayList<Integer>());
            }
            map.get(quadCounter).add(2);
            //////////////////////////////

            quadCounter++;
        

        }

        // sort result set based on how many triples they appear
        List<Map.Entry<Node, HashMap<Integer, ArrayList<Integer>>>> entries = new ArrayList<>(resultSet.entrySet());
        Collections.sort(entries, new Comparator<Map.Entry<Node, HashMap<Integer, ArrayList<Integer>>>>() {
            public int compare(Map.Entry<Node, HashMap<Integer, ArrayList<Integer>>> a,
                    Map.Entry<Node, HashMap<Integer, ArrayList<Integer>>> b) {
                return Integer.compare(b.getValue().size(), a.getValue().size());
            }
        });
        LinkedHashMap<Node, HashMap<Integer, ArrayList<Integer>>> results = new LinkedHashMap<>();
        for (Entry<Node, HashMap<Integer, ArrayList<Integer>>> entry : entries) {
            results.put(entry.getKey(), entry.getValue());
        }

        HashMap<Node, String> variables = new HashMap<Node, String>();
        // keep track of used triple patterns
        ArrayList<String> used = new ArrayList<String>();
        Op op = null;
        BasicPattern pat = new BasicPattern();

        int variableCounter = 0;
        for (Entry<Node, HashMap<Integer, ArrayList<Integer>>> entry : results.entrySet()) {

            HashMap<Integer, ArrayList<Integer>> patterns = entry.getValue();
            Node node = entry.getKey();
            variableCounter++;

            if (patternsToTriples.isEmpty())
                break;
            for (Entry<Integer, ArrayList<Integer>> pattern : patterns.entrySet()) {
                int sentIndex = pattern.getKey();
                int patternIndex = pattern.getValue().get(0);

                if (patternsToTriples.containsKey(sentIndex)) {
                    Triple triple = patternsToTriples.get(sentIndex).get(patternIndex);

                    Node subject = triple.getSubject();
                    Node predicate = triple.getPredicate();
                    Node object = triple.getObject();
                    Triple newPattern = null;

                    if (subject.isVariable()) {
                        if (!variables.containsKey(node))
                            variables.put(node, "x" + variableCounter);

                        newPattern = Triple.create(Var.alloc(variables.get(node)), predicate, object);
                        if (!used.contains(newPattern.toString())) {
                            pat.add(newPattern);
                            used.add(newPattern.toString());
                        }
                    } else if (predicate.isVariable()) {
                        if (!variables.containsKey(node))
                            variables.put(node, "x" + variableCounter);

                        newPattern = Triple.create(subject, Var.alloc(variables.get(node)), object);
                        if (!used.contains(newPattern.toString())) {
                            pat.add(newPattern);
                            used.add(newPattern.toString());
                        }
                    } else {
                        if (!variables.containsKey(node))
                            variables.put(node, "x" + variableCounter);

                        newPattern = Triple.create(subject, predicate, Var.alloc(variables.get(node)));
                        if (!used.contains(newPattern.toString())) {
                            pat.add(newPattern);
                            used.add(newPattern.toString());
                        }
                    }

                    patternsToTriples.remove(sentIndex);
                    if (patternsToTriples.isEmpty())
                        break;
                }

            }

        }

        op = new OpBGP(pat);
        Query q = OpAsQuery.asQuery(op); // Convert to a query
        q.addGraphURI(graphName);
        q.setQuerySelectType();
        // save to output
        OutputStream outStream = null;
        String fileName = outputFolder + "selectQuery" + streamCounter + ".sparql";
        try {
            outStream = new FileOutputStream(fileName);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            logger.error("File doesn't exist. " + fileName);
            throw new RuntimeException();
        }
        IndentedWriter out = new IndentedWriter(outStream);
        q.output(out);

        this.selectQueryFile = fileName;

    }

}
