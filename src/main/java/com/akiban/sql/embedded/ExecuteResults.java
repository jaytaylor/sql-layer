/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.embedded;

import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;

import java.sql.ResultSet;
import java.util.Deque;

class ExecuteResults
{
    private int updateCount;
    private Operator operator;
    private Cursor generatedKeys;
    private Deque<ResultSet> additionalResultSets;

    /** No results. */
    public ExecuteResults() {
        this.updateCount = -1;
    }

    /** Ordinary select result. 
     * Execution does not really happen until transaction is opened. 
     */
    public ExecuteResults(Operator operator) {
        this.updateCount = -1;
        this.operator = operator;
    }

    /** Update result, possibly with returned keys. 
     * These keys are already copied in order to get update count correct.
     */
    public ExecuteResults(int updateCount, Cursor generatedKeys) {
        this.updateCount = updateCount;
        this.generatedKeys = generatedKeys;
    }

    /** Stored procedure returning result sets of unknown provenance. */
    public ExecuteResults(Deque<ResultSet> resultSets) {
        this.updateCount = -1;
        this.additionalResultSets = resultSets;
    }
    
    public int getUpdateCount() {
        return updateCount;
    }
    
    public Operator getOperator() {
        return operator;
    }

    public Cursor getGeneratedKeys() {
        return generatedKeys;
    }

    public Deque<ResultSet> getAdditionalResultSets() {
        return additionalResultSets;
    }
    
}
