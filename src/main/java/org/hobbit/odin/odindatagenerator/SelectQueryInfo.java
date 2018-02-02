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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
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
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.log4j.Logger;

/**
 * Select Query class. Responsible for creating and storing a SELECT SPARQL
 * query given a file.
 * 
 * @author Kleanthi Georgala (georgala@informatik.uni-leipzig.de)
 * @version 2.0
 *
 */

public class SelectQueryInfo {

    public Set<Integer> getTriplesCovered() {
        return triplesCovered;
    }

    public void setTriplesCovered(Set<Integer> triplesCovered) {
        this.triplesCovered = triplesCovered;
    }

    public int getVariableCounter() {
        return variableCounter;
    }

    public void setVariableCounter(int variableCounter) {
        this.variableCounter = variableCounter;
    }

    public String getSelectQueryFile() {
        return selectQueryFile;
    }

    public void setSelectQueryFile(String selectQueryFile) {
        this.selectQueryFile = selectQueryFile;
    }

    protected static final Logger logger = Logger.getLogger(SelectQueryInfo.class.getName());

    HashMap<Triple, HashMap<Integer, Node>> tps = new HashMap<Triple, HashMap<Integer, Node>>();

    HashSet<Integer> quadCounter = new HashSet<Integer>();
    Set<Integer> triplesCovered = new TreeSet<Integer>();
    int variableCounter = 0;
    HashMap<String, HashSet<Node>> variables = new HashMap<String, HashSet<Node>>();

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
     * Iterates over the statements of a model and creates all possible triple
     * patterns with one variable. Then, it places each TP into 3 subsets: (i)
     * subjectTPs: set of TPs where only the subject is a variable. (ii)
     * predicateTPs: set of TPs where only the predicate is a variable. (iii)
     * objectTPs: set of TPs where only the object is a variable. We call this
     * every TP that belongs to this set, objectTP.
     * 
     * 
     * @param model,
     *            the model that includes the statements to be transformed into
     *            triple patterns
     */
    public void createTriplePatterns(Model model) {
        StmtIterator it = model.listStatements();
        int counter = 0;
        while (it.hasNext()) {
            counter++;
            Statement statement = it.next();
            Triple triple = statement.asTriple();
            Node subject = triple.getSubject();
            Node predicate = triple.getPredicate();
            Node object = triple.getObject();

            // create one triple pattern for each triple Node
            Triple newTriple = null;

            // for subject:
            Node subjectVariable = NodeFactory.createVariable("?");
            newTriple = new Triple(subjectVariable, predicate, object);

            if (!tps.containsKey(newTriple)) {
                tps.put(newTriple, new HashMap<Integer, Node>());
            }
            HashMap<Integer, Node> temp = tps.get(newTriple);
            temp.put(counter, subject);
            tps.put(newTriple, temp);
            //////////////////////////////
            // for predicate:
            Node predicateVariable = NodeFactory.createVariable("?");
            newTriple = new Triple(subject, predicateVariable, object);

            if (!tps.containsKey(newTriple)) {
                tps.put(newTriple, new HashMap<Integer, Node>());
            }
            temp = tps.get(newTriple);
            temp.put(counter, predicate);
            tps.put(newTriple, temp);
            //////////////////////////////
            // for object:
            Node objectVariable = NodeFactory.createVariable("?");
            newTriple = new Triple(subject, predicate, objectVariable);

            if (!tps.containsKey(newTriple)) {
                tps.put(newTriple, new HashMap<Integer, Node>());
            }
            temp = tps.get(newTriple);
            temp.put(counter, object);
            tps.put(newTriple, temp);
            //////////////////////////////
            this.quadCounter.add(counter);

        }

    }

    /**
     * Creates a triple pattern given a map of triple ids and their
     * corresponding subjects.
     * 
     * 
     * @param subjects,
     *            the map of triple ids and their corresponding subjects
     * @param i,
     *            the id number of the triple
     * @return the merged triple
     */
    public Triple createSubjectPattern(Triple triple, HashMap<Integer, Node> list) {

        HashSet<Integer> ids = new HashSet<Integer>(list.keySet());
        Set<Integer> intersection = new HashSet<Integer>(this.triplesCovered);
        intersection.retainAll(ids);

        // all new triples
        if (intersection.isEmpty()) {
            HashSet<Node> answers = new HashSet<Node>(list.values());

            String var = null;
            if (!variables.containsValue(answers)) {
                variables.put("x" + variableCounter, answers);
                this.variableCounter++;
            }
            for (Entry<String, HashSet<Node>> entry : variables.entrySet()) {
                HashSet<Node> tempAnswers = entry.getValue();
                if (tempAnswers.equals(answers)) {
                    var = entry.getKey();
                    break;
                }
            }

            Triple newPattern = Triple.create(Var.alloc(var), triple.getPredicate(), triple.getObject());
            this.triplesCovered.addAll(list.keySet());
            return newPattern;
        } else {
            // all triples are covered
            // or
            // there is some overlap
            return null;
        }

    }

    /**
     * Creates a triple pattern given a map of triple ids and their
     * corresponding predicates.
     * 
     * 
     * @param subjects,
     *            the map of triple ids and their corresponding predicates
     * @param i,
     *            the id number of the triple
     * @return the merged triple
     */
    public Triple createPredicatePattern(Triple triple, HashMap<Integer, Node> list) {

        HashSet<Integer> ids = new HashSet<Integer>(list.keySet());
        Set<Integer> intersection = new HashSet<Integer>(this.triplesCovered);
        intersection.retainAll(ids);

        // all new triples
        if (intersection.isEmpty()) {
            HashSet<Node> answers = new HashSet<Node>(list.values());

            String var = null;
            if (!variables.containsValue(answers)) {
                variables.put("x" + variableCounter, answers);
                this.variableCounter++;
            }
            for (Entry<String, HashSet<Node>> entry : variables.entrySet()) {
                HashSet<Node> tempAnswers = entry.getValue();
                if (tempAnswers.equals(answers)) {
                    var = entry.getKey();
                    break;
                }
            }

            Triple newPattern = Triple.create(triple.getSubject(), Var.alloc(var), triple.getObject());
            this.triplesCovered.addAll(list.keySet());
            return newPattern;
        } else {
            // all triples are covered
            // or
            // there is some overlap
            return null;
        }

    }

    /**
     * Creates a triple pattern given a map of triple ids and their
     * corresponding objects.
     * 
     * 
     * @param subjects,
     *            the map of triple ids and their corresponding objects
     * @param i,
     *            the id number of the triple
     * @return the merged triple
     */
    public Triple createObjectPattern(Triple triple, HashMap<Integer, Node> list) {

        HashSet<Integer> ids = new HashSet<Integer>(list.keySet());
        Set<Integer> intersection = new HashSet<Integer>(this.triplesCovered);
        intersection.retainAll(ids);

        // all new triples
        if (intersection.isEmpty()) {
            HashSet<Node> answers = new HashSet<Node>(list.values());

            String var = null;
            if (!variables.containsValue(answers)) {
                variables.put("x" + variableCounter, answers);
                this.variableCounter++;
            }
            for (Entry<String, HashSet<Node>> entry : variables.entrySet()) {
                HashSet<Node> tempAnswers = entry.getValue();
                if (tempAnswers.equals(answers)) {
                    var = entry.getKey();
                    break;
                }
            }

            Triple newPattern = Triple.create(triple.getSubject(), triple.getPredicate(), Var.alloc(var));
            this.triplesCovered.addAll(list.keySet());
            return newPattern;
        } else {
            // all triples are covered
            // or
            // there is some overlap
            return null;
        }

    }

    private Map<Triple, HashMap<Integer, Node>> sortBySizeOfValues() {

        List<Entry<Triple, HashMap<Integer, Node>>> list = new LinkedList<Entry<Triple, HashMap<Integer, Node>>>(
                tps.entrySet());

        // Sorting the list based on values
        Collections.sort(list, new Comparator<Entry<Triple, HashMap<Integer, Node>>>() {
            public int compare(Entry<Triple, HashMap<Integer, Node>> o1, Entry<Triple, HashMap<Integer, Node>> o2) {
                HashMap<Integer, Node> list1 = o1.getValue();
                HashMap<Integer, Node> list2 = o2.getValue();
                Integer length1 = list1.size();
                Integer length2 = list2.size();
                return length2.compareTo(length1);
            }
        });

        // Maintaining insertion order with the help of LinkedList
        Map<Triple, HashMap<Integer, Node>> sortedMap = new LinkedHashMap<Triple, HashMap<Integer, Node>>();
        for (Entry<Triple, HashMap<Integer, Node>> entry : list) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }

    /**
     * Creates a SELECT SPARQL query given a set of INSERT SPARQL queries (as a
     * UTF-8 encoded String) and stores it into a file. This method uses a Least
     * General Generalization (LGG) technique. The aim of this function is
     * twofold: create a SELECT query (1) with the least possible triple
     * patterns so that (2) the resulting query is able identify if all the
     * triples of the input INSERT query were inserted into the triple store.
     * 
     * Firstly, for each triple, we find the triple pattern (TPs) with one
     * variable. We divide set of TPs into 3 subsets: (i) subjectTPs: set of TPs
     * where only the subject is a variable. We call this every TP that belongs
     * to this set, subjectTP. (ii) predicateTPs: set of TPs where only the
     * predicate is a variable. We call this every TP that belongs to this set,
     * predicateTP. (iii) objectTPs: set of TPs where only the object is a
     * variable. We call this every TP that belongs to this set, objectTP.
     * 
     * Then, for the first triple, we compare its subjectTP with the subjectTPs
     * of the other triples. If two subjectTPs are compatible (their
     * non-variable parts are the same), we store the subjectTP of the other
     * triple in the set A. We follow the same procedure for the predicateTP and
     * the objectTP of the first triple, where we get as a result two more sets:
     * B and C resp.
     * 
     * We compare the size of A, B and C and we keep the one with the most
     * elements, e.g. A. We want to keep the set with the most elements cause we
     * can cover more triples with one single TP.
     * 
     * Then we merge the first triple's subjectTP with all the subjectTPs
     * included A into one TP.
     * 
     * We continue the same procedure with the rest of the triples. We don't
     * care about triples that have already been covered.
     * 
     * At the end, we have a select query that covers all of the triples in the
     * minimum size.
     * 
     * @param insertQueries,
     *            a set of UTF-8 encoded String representations of the INSERT
     *            queries
     * @param outputFolder,
     *            the output folder to store the SELECT query
     * @param streamCounter,
     *            the ID of the stream that the SELECT query belongs to
     * @param graphName,
     *            the name of the graph that the select query will be performed
     *            against
     */
    //TODO: minimize query
    public void createSelectQuery(ArrayList<InsertQueryInfo> insertQueries, String outputFolder, int streamCounter,
            String graphName) {

        outputFolder = outputFolder + "selectQueries/";
        File newFolder = new File(outputFolder);
        if (!newFolder.exists())
            newFolder.mkdir();

        Model model = ModelFactory.createDefaultModel();
        for (InsertQueryInfo insertQuery : insertQueries) {
            String fileName = insertQuery.getModelFile();
            Model tempModel = ModelFactory.createDefaultModel();
            tempModel.read(fileName);
            model.add(tempModel);
        }

        //logger.info("Size of model: " + model.size());
        //logger.info("Read all files");
        createTriplePatterns(model);
        //logger.info("Created triple patterns");

        Map<Triple, HashMap<Integer, Node>> sortedTps = sortBySizeOfValues();

        Op op = null;
        LinkedHashMap<Node, HashSet<Triple>> triples = new LinkedHashMap<Node, HashSet<Triple>>();

        for (Map.Entry<Triple, HashMap<Integer, Node>> entry : sortedTps.entrySet()) {

            Triple key = entry.getKey();
            HashMap<Integer, Node> value = entry.getValue();

            if (key.getSubject().isVariable()) {
                Triple newPattern = this.createSubjectPattern(key, value);
                if (newPattern != null) {
                    Node var = newPattern.getSubject();
                    if (!triples.containsKey(var)) {
                        triples.put(var, new HashSet<Triple>());
                    }
                    HashSet<Triple> temp = triples.get(var);
                    temp.add(newPattern);
                    triples.put(var, temp);
                }
            } else if (key.getPredicate().isVariable()) {
                Triple newPattern = this.createPredicatePattern(key, value);
                if (newPattern != null) {
                    Node var = newPattern.getPredicate();
                    if (!triples.containsKey(var)) {
                        triples.put(var, new HashSet<Triple>());
                    }
                    HashSet<Triple> temp = triples.get(var);
                    temp.add(newPattern);
                    triples.put(var, temp);
                }
            } else {
                Triple newPattern = this.createObjectPattern(key, value);
                if (newPattern != null) {
                    Node var = newPattern.getObject();
                    if (!triples.containsKey(var)) {
                        triples.put(var, new HashSet<Triple>());
                    }
                    HashSet<Triple> temp = triples.get(var);
                    temp.add(newPattern);
                    triples.put(var, temp);
                }

            }
            if (this.triplesCovered.containsAll(this.quadCounter))
                break;

        }
        // logger.info(date.toString()+" I am done with the quads");
        op = null;
        BasicPattern pattern = null;
        for (Entry<Node, HashSet<Triple>> entry : triples.entrySet()) {
            HashSet<Triple> value = entry.getValue();
            pattern = new BasicPattern();
            for (Triple triple : value) {
                pattern.add(triple);
            }
            op = OpUnion.create(op, new OpBGP(pattern));
        }

        Query q = OpAsQuery.asQuery(op); // Convert to a query
        q.addGraphURI(graphName);
        q.setQuerySelectType();

        // save to output
        OutputStream outStream = null;
        String fileName = outputFolder + "selectQuery" + streamCounter + ".sparql";
        try {
            outStream = new FileOutputStream(fileName);
        } catch (FileNotFoundException e) {
            logger.error("File doesn't exist. " + fileName);
            e.printStackTrace();
            throw new RuntimeException();
        }
        // logger.info(date.toString()+" Created FileOutputStream");

        IndentedWriter out = new IndentedWriter(outStream);
        q.output(out);
        out.close();

        try {
            outStream.close();
        } catch (IOException e) {
            logger.error("Can't close file " + fileName);
            e.printStackTrace();
            throw new RuntimeException();
        }
        this.selectQueryFile = fileName;

    }

}