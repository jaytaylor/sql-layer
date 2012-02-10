/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.optimizer.rule.join_enum;

import com.akiban.sql.optimizer.plan.*;
import static com.akiban.sql.optimizer.plan.JoinNode.JoinType;

import java.util.*;

/** 
 * Hypergraph-based dynamic programming for join ordering enumeration.
 * See "Dynamic Programming Strikes Back", doi:10.1145/1376616.1376672
 *
 * Summary:<ul>
 * <li>Nodes are joined tables.</li>
 * <li>Edges represent predicates and operator (LEFT, FULL OUTER,
 * SEMI, ...) reordering constraints.</li>
 * <li>DP happens by considering larger
 * sets made up from pairs of connected (based on edges) subsets.</li></ul>
 */
public abstract class DPhyp<P>
{
    // The leaves of the join tree: tables, derived tables, and
    // possibly joins handled atomically wrt this phase.
    private List<Joinable> tables;
    // The join operators, from JOINs and WHERE clause.
    private List<JoinOperator> operators;
    // The hypergraph: since these are unordered, traversal pattern is
    // to go through in order pairing with adjacent (complement bit 1).
    private long[] edges;
    private int noperators, nedges;
    
    // The "plan class" is the set of retained plans for the given tables.
    private Object[] plans;
    
    private P getPlan(long s) {
        return (P)plans[(int)s];
    }
    private void setPlan(long s, P plan) {
        if (false)
            System.out.println(JoinableBitSet.toString(s, tables) + ": " + plan);
        plans[(int)s] = plan;
    }

    public P run(Joinable root, ConditionList whereConditions) {
        init(root, whereConditions);
        return solve();
    }

    /** Return minimal set of neighbors of <code>s</code> not in the exclusion set. */
    // TODO: Need to eliminate subsumed hypernodes.
    public long neighborhood(long s, long exclude) {
        exclude = JoinableBitSet.union(s, exclude);
        long result = 0;
        for (int e = 0; e < nedges; e++) {
            if (JoinableBitSet.isSubset(edges[e], s) &&
                !JoinableBitSet.overlaps(edges[e^1], exclude)) {
                result |= JoinableBitSet.minSubset(edges[e^1]);
            }
        }
        return result;
    }

    /** Run dynamic programming and return best overall plan(s). */
    public P solve() {
        int ntables = tables.size();
        plans = new Object[1 << ntables];
        // Start with single tables.
        for (int i = 0; i < ntables; i++) {
            setPlan(JoinableBitSet.of(i), evaluateTable(tables.get(i)));
        }
        for (int i = ntables - 1; i >= 0; i--) {
            long ts = JoinableBitSet.of(i);
            emitCsg(ts);
            enumerateCsgRec(ts, JoinableBitSet.through(i));
        }
        return getPlan(plans.length-1); // One that does all the joins together.
    }

    /** Recursively extend the given connected subgraph. */
    public void enumerateCsgRec(long s1, long exclude) {
        long neighborhood = neighborhood(s1, exclude);
        if (neighborhood == 0) return;
        long subset = 0;
        do {
            subset = JoinableBitSet.nextSubset(subset, neighborhood);
            long next = JoinableBitSet.union(s1, subset);
            if (getPlan(next) != null)
                emitCsg(next);
        } while (!JoinableBitSet.equals(subset, neighborhood));
        subset = 0;             // Start over.
        exclude = JoinableBitSet.union(exclude, neighborhood);
        do {
            subset = JoinableBitSet.nextSubset(subset, neighborhood);
            long next = JoinableBitSet.union(s1, subset);
            enumerateCsgRec(next, exclude);
        } while (!JoinableBitSet.equals(subset, neighborhood));
    }

    /** Generate seeds for connected complements of the given connected subgraph. */
    public void emitCsg(long s1) {
        long exclude = JoinableBitSet.union(s1, JoinableBitSet.through(JoinableBitSet.min(s1)));
        long neighborhood = neighborhood(s1, exclude);
        if (neighborhood == 0) return;
        for (int i = tables.size() - 1; i >= 0; i--) {
            long s2 = JoinableBitSet.of(i);
            if (JoinableBitSet.overlaps(neighborhood, s2)) {
                boolean connected = false;
                for (int e = 0; e < nedges; e++) {
                    if (JoinableBitSet.isSubset(edges[e], s1) &&
                        JoinableBitSet.isSubset(edges[e^1], s2)) {
                        connected = true;
                        break;
                    }
                }
                if (connected)
                    emitCsgCmp(s1, s2);
                enumerateCmpRec(s1, s2, exclude);
            }
        }        
    }

    /** Extend complement <code>s2</code> until a csg-cmp pair is
     * reached, excluding the given tables to avoid duplicate
     * enumeration, */
    public void enumerateCmpRec(long s1, long s2, long exclude) {
        long neighborhood = neighborhood(s2, exclude);
        if (neighborhood == 0) return;
        long subset = 0;
        do {
            subset = JoinableBitSet.nextSubset(subset, neighborhood);
            long next = JoinableBitSet.union(s2, subset);
            if (getPlan(next) != null) {
                boolean connected = false;
                for (int e = 0; e < nedges; e++) {
                    if (JoinableBitSet.isSubset(edges[e], s1) &&
                        JoinableBitSet.isSubset(edges[e^1], next)) {
                        connected = true;
                        break;
                    }
                }
                if (connected)
                    emitCsgCmp(s1, next);
            }
        } while (!JoinableBitSet.equals(subset, neighborhood));
        subset = 0;             // Start over.
        exclude = JoinableBitSet.union(exclude, neighborhood);
        do {
            subset = JoinableBitSet.nextSubset(subset, neighborhood);
            long next = JoinableBitSet.union(s2, subset);
            enumerateCmpRec(s1, next, exclude);
        } while (!JoinableBitSet.equals(subset, neighborhood));
    }

    /** Emit the connected subgraph / complement pair.
     * That is, build and cost a plan that joins them and maybe
     * register it as the new best such plan for the pair.
     */
    public void emitCsgCmp(long s1, long s2) {
        P p1 = getPlan(s1);
        P p2 = getPlan(s2);
        long s = JoinableBitSet.union(s1, s2);
        JoinOperator operator = null;
        for (int e = 0; e < nedges; e++) {
            if (JoinableBitSet.isSubset(edges[e], s1) &&
                JoinableBitSet.isSubset(edges[e^1], s2)) {
                // TODO: When can more than one apply?
                assert (operator == null);
                operator = operators.get(e/2); // The one that produced this edge.
            }
        }
        assert (operator != null);
        P plan = getPlan(s);
        plan = evaluateJoin(p1, p2, plan, 
                            operator.getJoinType(), operator.joinConditions);
        if (isCommutative(operator.getJoinType()))
            plan = evaluateJoin(p2, p1, plan, 
                                operator.getJoinType(), operator.joinConditions);
        setPlan(s, plan);
    }

    public abstract P evaluateTable(Joinable table);
    public abstract P evaluateJoin(P p1, P p2, P existing, 
                                   JoinType joinType, ConditionList joinConditions);

    /** Initialize state from the given join tree. */
    public void init(Joinable root, ConditionList whereConditions) {
        tables = new ArrayList<Joinable>();
        addTables(root);
        ExpressionTables visitor = new ExpressionTables(tables);
        noperators = 0;
        JoinOperator rootOp = initSES(root, visitor);
        noperators += whereConditions.size(); // Maximum possible.
        operators = new ArrayList<JoinOperator>(noperators);
        nedges = noperators * 2;
        edges = new long[nedges];
        calcTES(rootOp);
        addWhereConditions(whereConditions, visitor);
        noperators = operators.size();
        nedges = noperators * 2;
        if (false) {
            for (int e = 0; e < nedges; e += 2) {
                System.out.println(JoinableBitSet.toString(edges[e], tables) + " <-> " +
                                   JoinableBitSet.toString(edges[e^1], tables));
            }
        }
    }

    protected void addTables(Joinable n) {
        if (n instanceof JoinNode) {
            JoinNode join = (JoinNode)n;
            addTables(join.getLeft());
            addTables(join.getRight());
        }
        else {
            tables.add(n);
        }
    }

    static class JoinOperator {
        JoinNode join;          // If from an actual join.
        ConditionList joinConditions;
        JoinOperator left, right, parent;
        long leftTables, rightTables, predicateTables, tes;
        
        public JoinOperator(JoinNode join) {
            this.join = join;
            joinConditions = join.getJoinConditions();
        }

        public JoinOperator(ConditionExpression condition) {
            joinConditions = new ConditionList(1);
            joinConditions.add(condition);
        }

        public long getTables() {
            return JoinableBitSet.union(leftTables, rightTables);
        }

        public JoinType getJoinType() {
            if (join != null)
                return join.getJoinType();
            else
                return JoinType.INNER;
        }
    }

    /** Starting state of the TES is just those tables used syntactically. */
    protected JoinOperator initSES(Joinable n, ExpressionTables visitor) {
        if (n instanceof JoinNode) {
            JoinNode join = (JoinNode)n;
            JoinOperator op = new JoinOperator(join);
            Joinable left = join.getLeft();
            JoinOperator leftOp = initSES(left, visitor);
            if (leftOp != null) {
                leftOp.parent = op;
                op.left = leftOp;
                op.leftTables = leftOp.getTables();
            }
            else {
                op.leftTables = JoinableBitSet.of(tables.indexOf(left));
            }
            Joinable right = join.getRight();
            JoinOperator rightOp = initSES(right, visitor);
            if (rightOp != null) {
                rightOp.parent = op;
                op.right = rightOp;
                op.rightTables = rightOp.getTables();
            }
            else {
                op.rightTables = JoinableBitSet.of(tables.indexOf(right));
            }
            op.predicateTables = visitor.getTables(op.joinConditions);
            if (visitor.wasNullTolerant() && !allInnerJoins(op))
                op.tes = op.getTables();
            else
                op.tes = JoinableBitSet.intersection(op.getTables(), op.predicateTables);
            noperators++;
            return op;
        }
        return null;
    }

    /** Extend TES for join operators based on reordering conflicts. */
    public void calcTES(JoinOperator op) {
        if (op != null) {
            calcTES(op.left);
            calcTES(op.right);
            addConflicts(op, op.left, true);
            addConflicts(op, op.right, false);
            long r = JoinableBitSet.intersection(op.tes, op.rightTables);
            long l = JoinableBitSet.difference(op.tes, r);
            int o = operators.size();
            operators.add(op);
            // Add an edge for the TES of this join operator.
            edges[o*2] = l;
            edges[o*2+1] = r;
        }
    }

    /** Add conflicts to <code>o1</code> from descendant <code>o2</code>. */
    protected void addConflicts(JoinOperator o1, JoinOperator o2, boolean left) {
        if (o2 != null) {
            if (left ? leftConflict(o2, o1) : rightConflict(o1, o2)) {
                o1.tes = JoinableBitSet.union(o1.tes, o2.tes);
            }
            addConflicts(o1, o2.left, left);
            addConflicts(o1, o2.right, left);
        }
    }

    /** Is there a left ordering conflict? */
    protected boolean leftConflict(JoinOperator o2, JoinOperator o1) {
        return JoinableBitSet.overlaps(o1.predicateTables, rightTables(o1, o2)) &&
            operatorConflict(o2.getJoinType(), o1.getJoinType());
    }

    /** Is there a right ordering conflict? */
    protected boolean rightConflict(JoinOperator o1, JoinOperator o2) {
        return JoinableBitSet.overlaps(o1.predicateTables, leftTables(o1, o2)) &&
            operatorConflict(o1.getJoinType(), o2.getJoinType());
    }

    /** Does parent opertor <code>o1</code> conflict with child <code>o2</code>? */
    protected boolean operatorConflict(JoinType o1, JoinType o2) {
        switch (o1) {
        case INNER:
            return (o2 == JoinType.FULL_OUTER);
        case LEFT:
            return (o2 != JoinType.LEFT);
        case FULL_OUTER:
            return (o2 == JoinType.INNER);
        default:
            return true;
        }
    }

    /** All the left tables on the path from <code>o2</code> (inclusive) 
     * to <code>o1</code> (exclusive). */
    protected long leftTables(JoinOperator o1, JoinOperator o2) {
        long result = JoinableBitSet.empty();
        for (JoinOperator o3 = o2; o3 != o1; o3 = o3.parent)
            result = JoinableBitSet.union(result, o3.leftTables);
        if (isCommutative(o2.getJoinType()))
            result = JoinableBitSet.union(result, o2.rightTables);
        return result;
    }

    /** All the right tables on the path from <code>o2</code> (inclusive) 
     * to <code>o1</code> (exclusive). */
    protected long rightTables(JoinOperator o1, JoinOperator o2) {
        long result = JoinableBitSet.empty();
        for (JoinOperator o3 = o2; o3 != o1; o3 = o3.parent)
            result = JoinableBitSet.union(result, o3.rightTables);
        if (isCommutative(o2.getJoinType()))
            result = JoinableBitSet.union(result, o2.leftTables);
        return result;
    }

    /** Does this operator commute? */
    protected boolean isCommutative(JoinType joinType) {
        switch (joinType) {
        case INNER:
        case FULL_OUTER:
            return true;
        default:
            return false;
        }
    }

    /** Is this operator and everything below it inner joined?
     * In that case a null-tolerant predicate doesn't interfere with
     * reordering.
     */
    protected boolean allInnerJoins(JoinOperator op) {
        return ((op.getJoinType() == JoinType.INNER) &&
                ((op.left == null) || allInnerJoins(op.left)) &&
                ((op.right == null) || allInnerJoins(op.right)));
    }

    protected void addWhereConditions(ConditionList whereConditions, 
                                      ExpressionTables visitor) {
        for (ConditionExpression condition : whereConditions) {
            // TODO: When optimizer supports more predicates
            // interestingly, can recognize them, including
            // generalized hypergraph triples.
            if (condition instanceof ComparisonCondition) {
                ComparisonCondition comp = (ComparisonCondition)condition;
                long columnTables = columnReferenceTable(comp.getLeft());
                if (!JoinableBitSet.isEmpty(columnTables)) {
                    long rhs = visitor.getTables(comp.getRight());
                    if (visitor.wasNullTolerant()) continue;
                    addWhereCondition(condition, columnTables, rhs);
                    continue;
                }
                columnTables = columnReferenceTable(comp.getRight());
                if (!JoinableBitSet.isEmpty(columnTables)) {
                    long lhs = visitor.getTables(comp.getLeft());
                    if (visitor.wasNullTolerant()) continue;
                    addWhereCondition(condition, columnTables, lhs);
                    continue;
                }
            }
        }
    }

    protected long columnReferenceTable(ExpressionNode node) {
        if (node instanceof ColumnExpression) {
            int idx = tables.indexOf(((ColumnExpression)node).getTable());
            if (idx >= 0) {
                return JoinableBitSet.of(idx);
            }
        }
        return 0;
    }

    protected void addWhereCondition(ConditionExpression condition,
                                     long columnTables, long comparisonTables) {
        if (!JoinableBitSet.isEmpty(comparisonTables) &&
            !JoinableBitSet.overlaps(columnTables, comparisonTables)) {
            JoinOperator op = new JoinOperator(condition);
            op.leftTables = columnTables;
            op.rightTables = comparisonTables;
            int o = operators.size();
            operators.add(op);
            edges[o*2] = columnTables;
            edges[o*2+1] = comparisonTables;
        }
    }

    /** Compute tables used in join predicate. */
    // TODO: Also need to record whether null-tolerant and prevent more ordering if so.
    static class ExpressionTables implements ExpressionVisitor, PlanVisitor {
        List<Joinable> tables;
        long result;
        boolean nullTolerant;

        public ExpressionTables(List<Joinable> tables) {
            this.tables = tables;
        }

        public long getTables(ConditionList conditions) {
            result = JoinableBitSet.empty();
            nullTolerant = false;
            if (conditions != null) {
                for (ConditionExpression cond : conditions) {
                    cond.accept(this);
                }
            }
            return result;
        }

        public long getTables(ExpressionNode node) {
            result = JoinableBitSet.empty();
            nullTolerant = false;
            node.accept(this);
            return result;
        }

        public boolean wasNullTolerant() {
            return nullTolerant;
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
            if (n instanceof ColumnExpression) {
                int idx = tables.indexOf(((ColumnExpression)n).getTable());
                if (idx >= 0) {
                    result = JoinableBitSet.union(result, JoinableBitSet.of(idx));
                }
            }
            else if (!nullTolerant && (n instanceof FunctionExpression)) {
                String fname = ((FunctionExpression)n).getFunction();
                if ("isNull".equals(fname) || "isUnknown".equals(fname) ||
                    "isTrue".equals(fname) || "isFalse".equals(fname) ||
                    "COALESCE".equals(fname) || "ifnull".equals(fname))
                    nullTolerant = true;
            }
            return true;
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
            return true;
        }
    }
    
}
