
package com.akiban.sql.embedded;

import com.akiban.qp.operator.Cursor;

import java.sql.ResultSet;
import java.util.Queue;

class ExecuteResults
{
    private int updateCount;
    private Cursor cursor;
    private Queue<ResultSet> additionalResultSets;

    /** No results. */
    public ExecuteResults() {
        this.updateCount = -1;
    }

    /** Ordinary select result. 
     * Transaction remains open while it is visited.
     */
    public ExecuteResults(Cursor cursor) {
        this.updateCount = -1;
        this.cursor = cursor;
    }

    /** Update result, possibly with returned keys. 
     * These keys are already copied in order to get update count correct.
     */
    public ExecuteResults(int updateCount, Cursor generatedKeys) {
        this.updateCount = updateCount;
        this.cursor = generatedKeys;
    }

    /** Stored procedure returning result sets of unknown provenance. */
    public ExecuteResults(Queue<ResultSet> resultSets) {
        this.updateCount = -1;
        this.additionalResultSets = resultSets;
    }
    
    public int getUpdateCount() {
        return updateCount;
    }
    
    public Cursor getCursor() {
        return cursor;
    }

    public boolean hasResultSet() {
        return (updateCount < 0);
    }
    
    public Queue<ResultSet> getAdditionalResultSets() {
        return additionalResultSets;
    }
    
}
