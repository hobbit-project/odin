package org.hobbit.odin.odindatagenerator;

import java.util.ArrayList;

/**
 * Stream class. Class responsible for storing the set of INSERT queries and
 * SELECT query locations that are going to be performed against the system
 * during the duration of the stream.
 * 
 * @author Kleanthi Georgala
 *
 */
public class Stream {
    /* Stream ID */
    private int ID;
    /* Set of INSERT queries of the stream */
    private ArrayList<InsertQueryInfo> insertQueries = null;
    /* SELECT query of the stream */
    private SelectQueryInfo selectQuery = null;
    /* Number of INSERT queries to be performed during the stream */
    private int sizeOfInserts = 0;
    /* Time stamp of the first INSERT query of the stream */
    private long beginPoint = 0l;
    /* Time stamp of the SELECT query of the stream */
    private long endPoint = 0l;

    /* Constructor */
    public Stream(int id, ArrayList<InsertQueryInfo> inserts, SelectQueryInfo select) {
        this.ID = id;
        this.insertQueries = inserts;
        this.selectQuery = select;
    }

    /* Setters and Getters */
    public long getBeginPoint() {
        return beginPoint;
    }

    public long getEndPoint() {
        return endPoint;
    }

    public int getSizeOfInserts() {
        return sizeOfInserts;
    }

    public void setSizeOfInserts(int sizeOfInserts) {
        this.sizeOfInserts = sizeOfInserts;
    }

    public int getID() {
        return ID;
    }

    public void setID(int iD) {
        ID = iD;
    }

    public ArrayList<InsertQueryInfo> getInsertQueries() {
        return insertQueries;
    }

    public void setInsertQueries(ArrayList<InsertQueryInfo> insertQueries) {
        this.insertQueries = insertQueries;
    }

    public SelectQueryInfo getSelectQuery() {
        return selectQuery;
    }

    public void setSelectQuery(SelectQueryInfo selectQuery) {
        this.selectQuery = selectQuery;
    }

    public void setBeginPoint(long firstStreamInsertTS) {
        this.beginPoint = firstStreamInsertTS;

    }

    public void setEndPoint(long lastStreamInsertTS) {
        this.endPoint = lastStreamInsertTS;

    }

    /**
     * Adds a new INSERT query to the list of INSERT queries of the stream
     * 
     * @param insert,
     *            the new INSERT query to be inserted
     */
    public void addInsertQuery(InsertQueryInfo insert) {
        if (insertQueries == null) {
            this.insertQueries = new ArrayList<InsertQueryInfo>();
        }
        this.insertQueries.add(insert);
        this.sizeOfInserts = this.insertQueries.size();
    }

    /**
     * Returns a INSERT query given a specified index.
     * 
     * @param index,
     *            the index of the correspond INSERT query
     * @return the INSERT query at position "index"
     */
    public InsertQueryInfo getInsertQueryInfo(int index) {
        InsertQueryInfo insert = null;
        if (index < 0 || index >= this.insertQueries.size())
            throw new RuntimeException("Index out of range. I can't find the INSERT query you want.");
        else
            insert = this.insertQueries.get(index);
        return insert;
    }

    /**
     * Returns the total number of triples that were inserted during the current
     * stream.
     * 
     * @return sum of all triples of all INSERT queries of this stream
     */
    public long getStreamModelSize() {
        long size = 0l;
        for (InsertQueryInfo insertQuery : this.insertQueries) {
            size += insertQuery.getModelSize();
        }
        return size;
    }

}
