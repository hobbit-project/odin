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

<<<<<<< HEAD
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
    public void createSets(Model model) {
        StmtIterator it = model.listStatements();

        while (it.hasNext()) {
            quadCounter++;
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
            subjectTPs.put(quadCounter, new ImmutablePair<Triple, Node>(newTriple, subject));
            //////////////////////////////
            // for predicate:
            Node predicateVariable = NodeFactory.createVariable("?");
            newTriple = new Triple(subject, predicateVariable, object);
            predicateTPs.put(quadCounter, new ImmutablePair<Triple, Node>(newTriple, predicate));
            //////////////////////////////
            // for object:
            Node objectVariable = NodeFactory.createVariable("?");
            newTriple = new Triple(subject, predicate, objectVariable);
            objectTPs.put(quadCounter, new ImmutablePair<Triple, Node>(newTriple, object));
            //////////////////////////////

        }

    }

    /**
     * Creates a map of triples ids and their corresponding subject given the
     * subject of a triple. It identifies other triples with the same predicate
     * and object as the initial triple and stores the id number of the triple
     * and its subject into a map.
     * 
     * @param i,
     *            the id number of the triple
     * @return the set of matching triples id and their subject
     */
    public HashMap<Integer, Pair<Triple, Node>> createSubjectSet(int i) {
        HashMap<Integer, Pair<Triple, Node>> subjects = new HashMap<Integer, Pair<Triple, Node>>();
        Pair<Triple, Node> tp1 = subjectTPs.get(i);

        Node predicate1 = tp1.getLeft().getPredicate();
        Node object1 = tp1.getLeft().getObject();

        for (Entry<Integer, Pair<Triple, Node>> entry : subjectTPs.entrySet()) {
            int j = entry.getKey();
            if (i == j)
                continue;
            if (triplesCovered.contains(j))
                continue;

            // compare non-variable parts
            Pair<Triple, Node> tp2 = entry.getValue();
            Node predicate2 = tp2.getLeft().getPredicate();
            Node object2 = tp2.getLeft().getObject();

            if (predicate1.equals(predicate2) && object1.equals(object2)) {
                subjects.put(j, tp2);
            }

        }
        return subjects;
    }

    /**
     * Creates a map of triples ids and their corresponding predicate given the
     * predicate of a triple. It identifies other triples with the same subject
     * and object as the initial triple and stores the id number of the triple
     * and its predicate into a map.
     * 
     * @param i,
     *            the id number of the triple
     * @return the set of matching triples id and their predicate
     */
    public HashMap<Integer, Pair<Triple, Node>> createPredicateSet(int i) {

        HashMap<Integer, Pair<Triple, Node>> predicates = new HashMap<Integer, Pair<Triple, Node>>();
        Pair<Triple, Node> tp1 = predicateTPs.get(i);
        Node subject1 = tp1.getLeft().getSubject();
        Node object1 = tp1.getLeft().getObject();

        for (Entry<Integer, Pair<Triple, Node>> entry : predicateTPs.entrySet()) {
            int j = entry.getKey();
            if (i == j)
                continue;
            if (triplesCovered.contains(j))
                continue;

            // compare non-variable parts
            Pair<Triple, Node> tp2 = entry.getValue();
            Node subject2 = tp2.getLeft().getSubject();
            Node object2 = tp2.getLeft().getObject();

            if (subject1.equals(subject2) && object1.equals(object2)) {
                predicates.put(j, tp2);
            }

        }
        return predicates;
    }

    /**
     * Creates a map of triples ids and their corresponding object given the
     * object of a triple. It identifies other triples with the same subject and
     * predicate as the initial triple and stores the id number of the triple
     * and its object into a map.
     * 
     * @param i,
     *            the id number of the triple
     * @return the set of matching triples id and their object
     */
    public HashMap<Integer, Pair<Triple, Node>> createObjectSet(int i) {

        HashMap<Integer, Pair<Triple, Node>> objects = new HashMap<Integer, Pair<Triple, Node>>();
        Pair<Triple, Node> tp1 = objectTPs.get(i);
        Node subject1 = tp1.getLeft().getSubject();
        Node predicate1 = tp1.getLeft().getPredicate();

        for (Entry<Integer, Pair<Triple, Node>> entry : predicateTPs.entrySet()) {
            int j = entry.getKey();
            if (i == j)
                continue;
            if (triplesCovered.contains(j))
                continue;

            // compare non-variable parts
            Pair<Triple, Node> tp2 = entry.getValue();
            Node subject2 = tp2.getLeft().getSubject();
            Node predicate2 = tp2.getLeft().getPredicate();

            if (subject1.equals(subject2) && predicate1.equals(predicate2)) {
                objects.put(j, tp2);
            }

        }
        return objects;
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
    public Triple mergeSubjectTPs(HashMap<Integer, Pair<Triple, Node>> subjects, int i) {
        Pair<Triple, Node> tp1 = subjectTPs.get(i);

        Node predicate1 = tp1.getLeft().getPredicate();
        Node object1 = tp1.getLeft().getObject();

        // get answer set for the variable
        Node answerSubject = tp1.getRight();
        HashSet<Node> answers = new HashSet<Node>();
        answers.add(answerSubject);
        for (Pair<Triple, Node> pair : subjects.values()) {
            answers.add(pair.getRight());
        }

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

        Triple newPattern = Triple.create(Var.alloc(var), predicate1, object1);

        Set<Integer> keys = subjects.keySet();
        this.triplesCovered.addAll(keys);
        this.triplesCovered.add(i);

        return newPattern;
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
    public Triple mergePredicateTPs(HashMap<Integer, Pair<Triple, Node>> predicates, int i) {

        Pair<Triple, Node> tp1 = predicateTPs.get(i);

        Node subject1 = tp1.getLeft().getSubject();
        Node object1 = tp1.getLeft().getObject();

        Node answerPredicate = tp1.getRight();
        HashSet<Node> answers = new HashSet<Node>();
        answers.add(answerPredicate);
        for (Pair<Triple, Node> pair : predicates.values()) {
            answers.add(pair.getRight());
        }

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

        Triple newPattern = Triple.create(subject1, Var.alloc(var), object1);

        Set<Integer> keys = predicates.keySet();
        this.triplesCovered.addAll(keys);
        this.triplesCovered.add(i);

        return newPattern;

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
    public Triple mergeObjectTPs(HashMap<Integer, Pair<Triple, Node>> objects, int i) {

        Pair<Triple, Node> tp1 = objectTPs.get(i);

        Node subject1 = tp1.getLeft().getSubject();
        Node predicate1 = tp1.getLeft().getPredicate();

        Node objectPredicate = tp1.getRight();

        HashSet<Node> answers = new HashSet<Node>();
        answers.add(objectPredicate);
        for (Pair<Triple, Node> pair : objects.values()) {
            answers.add(pair.getRight());
        }

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
        Triple newPattern = Triple.create(subject1, predicate1, Var.alloc(var));

        Set<Integer> keys = objects.keySet();
        this.triplesCovered.addAll(keys);
        this.triplesCovered.add(i);

        return newPattern;

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

    public void createSelectQuery(ArrayList<InsertQueryInfo> insertQueries, String outputFolder, int streamCounter,
            String graphName) {

        logger.info("Creating SELECT query for stream no."+streamCounter);
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
        logger.info("Stream no."+streamCounter+". Model size: "+model.size());
        createSets(model);

        Op op = null;
        BasicPattern pat = new BasicPattern();

        for (int i = 1; i <= quadCounter; i++) {
            if (triplesCovered.contains(i))
                continue;

            HashMap<Integer, Pair<Triple, Node>> subjects = createSubjectSet(i);
            HashMap<Integer, Pair<Triple, Node>> predicates = createPredicateSet(i);
            HashMap<Integer, Pair<Triple, Node>> objects = createObjectSet(i);

            Triple newPattern = null;

            if (subjects.size() == 0 && predicates.size() == 0 && objects.size() == 0) {
                Pair<Triple, Node> tp1 = subjectTPs.get(i);

                Node predicate1 = tp1.getLeft().getPredicate();
                Node object1 = tp1.getLeft().getObject();

                Node answerSubject = tp1.getRight();
                HashSet<Node> answers = new HashSet<Node>();
                answers.add(answerSubject);

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

                newPattern = Triple.create(Var.alloc(var), predicate1, object1);

                this.triplesCovered.add(i);
            } else {
                int max = Math.max(subjects.size(), Math.max(predicates.size(), objects.size()));
                if (max == subjects.size()) {
                    newPattern = mergeSubjectTPs(subjects, i);
                } else if (max == predicates.size()) {
                    newPattern = mergePredicateTPs(predicates, i);
                } else if (max == objects.size()) {
                    newPattern = mergeObjectTPs(objects, i);
                }

            }
            pat.add(newPattern);

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
        try {
            outStream.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        IndentedWriter out = new IndentedWriter(outStream);
        q.output(out);

        this.selectQueryFile = fileName;

    }

=======
>>>>>>> d4c6c344f104669117a6857d8d385aa9360a7abe
}
