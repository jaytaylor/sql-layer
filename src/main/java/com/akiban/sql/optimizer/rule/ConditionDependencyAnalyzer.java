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

import com.akiban.sql.optimizer.plan.*;

import java.util.*;

/** Track which tables a conditional expression depends on that are
 * not bound from an outer context.
 * Used to determine exactly when the condition can be tested.
 */
public class ConditionDependencyAnalyzer implements PlanVisitor, ExpressionVisitor {
    private enum State { GATHER, ANALYZE };
    private State state;
    private Set<ColumnSource> upstreamTables;
    private Set<ColumnSource> referencedTables;
    private Set<ColumnExpression> referencedColumns;

    /** Make an analyzer using the upstream tables that input to the
     * given node. */
    public ConditionDependencyAnalyzer(PlanNode node) {
        upstreamTables = new HashSet<ColumnSource>();
        state = State.GATHER;
        node.accept(this);
    }

    /** Make an analyzer that recognizes tables from either of two sources. */
    public ConditionDependencyAnalyzer(ConditionDependencyAnalyzer a1,
                                       ConditionDependencyAnalyzer a2) {
        upstreamTables = new HashSet<ColumnSource>(a1.upstreamTables);
        upstreamTables.addAll(a2.upstreamTables);
    }

    /** Get the tables that are not bound by some outer contour and so
     * must be available at the point of execution. */
    public Set<ColumnSource> getUpstreamTables() {
        return upstreamTables;
    }

    /** Analyze the given condition and if it acccesses just a single
     * table, return that.  Otherwise, return <code>null</code>.
     */
    public ColumnSource analyze(ConditionExpression cond) {
        referencedTables = new HashSet<ColumnSource>();
        referencedColumns = new HashSet<ColumnExpression>();
        state = State.ANALYZE;
        cond.accept(this);
        if (referencedTables.size() == 1)
            return referencedTables.iterator().next();
        else
            return null;
    }

    /** Get the tables that were referenced by the condition. */
    public Set<ColumnSource> getReferencedTables() {
        return referencedTables;
    }

    /** Get the columns that were referenced by the condition. */
    public Set<ColumnExpression> getReferencedColumns() {
        return referencedColumns;
    }

    @Override
    public boolean visitEnter(PlanNode n) {
        return visit(n);
    }

    @Override
    public boolean visitLeave(PlanNode n) {
        return true;
    }

    @Override
    public boolean visit(PlanNode n) {
        if (state == State.GATHER) {
            if (n instanceof ColumnSource)
                upstreamTables.add((ColumnSource)n);
            else if (n instanceof IndexScan)
                upstreamTables.addAll(((IndexScan)n).getTables());
        }
        return true;
    }

    @Override
    public boolean visitEnter(ExpressionNode n) {
        return visit(n);
    }

    @Override
    public boolean visitLeave(ExpressionNode n) {
        return true;
    }

    @Override
    public boolean visit(ExpressionNode n) {
        if (state == State.ANALYZE) {
            if (n instanceof ColumnExpression) {
                ColumnExpression column = (ColumnExpression)n;
                ColumnSource table = column.getTable();
                if (upstreamTables.contains(table)) {
                    referencedTables.add(table);
                    referencedColumns.add(column);
                }
            }
        }
        return true;
    }
}
