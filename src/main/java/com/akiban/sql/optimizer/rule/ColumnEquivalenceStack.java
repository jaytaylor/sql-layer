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

package com.akiban.sql.optimizer.rule;

import com.akiban.sql.optimizer.plan.BaseQuery;
import com.akiban.sql.optimizer.plan.ColumnExpression;
import com.akiban.sql.optimizer.plan.PlanNode;

import java.util.ArrayDeque;
import java.util.Deque;

public final class ColumnEquivalenceStack {
    private static final int EQUIVS_DEQUE_SIZE = 3;
    private Deque<EquivalenceFinder<ColumnExpression>> stack
            = new ArrayDeque<EquivalenceFinder<ColumnExpression>>(EQUIVS_DEQUE_SIZE);
    
    public boolean enterNode(PlanNode n) {
        if (n instanceof BaseQuery) {
            BaseQuery bq = (BaseQuery) n;
            stack.push(bq.getColumnEquivalencies());
            return true;
        }
        return false;
    }
    
    public EquivalenceFinder<ColumnExpression> leaveNode(PlanNode n) {
        if (n instanceof BaseQuery) {
            return stack.pop();
        }
        return null;
    }
    
    public EquivalenceFinder<ColumnExpression> get() {
        return stack.element();
    }

    public boolean isEmpty() {
        return stack.isEmpty();
    }
}
