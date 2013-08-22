/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.embedded;

import com.foundationdb.qp.operator.RowCursor;

import java.sql.ResultSet;
import java.util.Queue;

class ExecuteResults
{
    private int updateCount;
    private RowCursor cursor;
    private Queue<ResultSet> additionalResultSets;

    /** No results. */
    public ExecuteResults() {
        this.updateCount = -1;
    }

    /** Ordinary select result. 
     * Transaction remains open while it is visited.
     */
    public ExecuteResults(RowCursor cursor) {
        this.updateCount = -1;
        this.cursor = cursor;
    }

    /** Update result, possibly with returned keys. 
     * These keys are already copied in order to get update count correct.
     */
    public ExecuteResults(int updateCount, RowCursor generatedKeys) {
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
    
    public RowCursor getCursor() {
        return cursor;
    }

    public boolean hasResultSet() {
        return (updateCount < 0);
    }
    
    public Queue<ResultSet> getAdditionalResultSets() {
        return additionalResultSets;
    }
    
}
