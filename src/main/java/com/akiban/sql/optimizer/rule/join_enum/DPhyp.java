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

import com.akiban.server.error.UnsupportedSQLException;

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
    private static final boolean TRACE = false;

    // The leaves of the join tree: tables, derived tables, and
    // possibly joins handled atomically wrt this phase.
    private List<Joinable> tables;
    // The join operators, from JOINs and WHERE clause.
    private List<JoinOperator> operators, evaluateOperators;
    // The hypergraph: since these are unordered, traversal pattern is
    // to go through in order pairing with adjacent (complement bit 1).
    private long[] edges;
    private int noperators, nedges;
    
    // Indexes for leaves and their constituent tables.
    private Map<Joinable,Long> tableBitSets;

    // The "plan class" is the set of retained plans for the given tables.
    private Object[] plans;
    
    @SuppressWarnings("unchecked")
    private P getPlan(long s) {
        return (P)plans[(int)s];
    }
    private void setPlan(long s, P plan) {
        if (TRACE)
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
        assert (ntables < 31);
        plans = new Object[1 << ntables];
        for (int pass = 1; pass <= 2; pass++) {
            // Start with single tables.
            for (int i = 0; i < ntables; i++) {
                long bitset = JoinableBitSet.of(i);
                setPlan(bitset, evaluateTable(bitset, tables.get(i)));
            }
            for (int i = ntables - 1; i >= 0; i--) {
                long ts = JoinableBitSet.of(i);
                emitCsg(ts);
                enumerateCsgRec(ts, JoinableBitSet.through(i));
            }
            P plan = getPlan(plans.length-1); // One that does all the joins together.
            if (plan != null)
                return plan;
            if (TRACE) {
                System.out.println("Plan not complete");
                for (int i = 1; i < plans.length-1; i++) {
                    System.out.println(Integer.toString(i, 2) + ": " + plans[i]);
                }
            }
            assert (pass == 1) : "Additional edges did not connect graph";
            addExtraEdges();
            Arrays.fill(plans, null);
        }
        return null;
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
        JoinType join12 = JoinType.INNER, join21 = JoinType.INNER;
        evaluateOperators.clear();
        for (int e = 0; e < nedges; e++) {
            if (JoinableBitSet.isSubset(edges[e], s1) &&
                JoinableBitSet.isSubset(edges[e^1], s2)) {
                // The one that produced this edge.
                JoinOperator operator = operators.get(e/2);
                JoinType joinType = operator.getJoinType();
                if (joinType != JoinType.INNER) {
                    join12 = joinType;
                    join21 = commuteJoinType(joinType);
                    if ((e & 1) != 0) {
                        join12 = join21;
                        join21 = joinType;
                    }
                }
                evaluateOperators.add(operator);
            }
        }
        P plan = getPlan(s);
        if (join12 != null)
            plan = evaluateJoin(s1, p1, s2, p2, s, plan, join12, evaluateOperators);
        if (join21 != null)
            plan = evaluateJoin(s2, p2, s1, p1, s, plan, join21, evaluateOperators);
        setPlan(s, plan);
    }

    /** Return the best plan for the one-table initial state. */
    public abstract P evaluateTable(long bitset, Joinable table);

    /** Adjust best plan <code>existing</code> for the join of
     * <code>p1</code> and <code>p2</code> with given type and
     * conditions taken from the given joins.
     */
    public abstract P evaluateJoin(long bitset1, P p1, long bitset2, P p2, long bitsetJoined, P existing,
                                   JoinType joinType, Collection<JoinOperator> joins);

    /** Return a leaf of the tree. */
    public Joinable getTable(int index) {
        return tables.get(index);
    }

    /** Initialize state from the given join tree. */
    // TODO: Need to do something about disconnected overall. The
    // cross-product isn't going to do very well no matter what. Maybe
    // just break up the graph _before_ calling all this?
    public void init(Joinable root, ConditionList whereConditions) {
        tables = new ArrayList<Joinable>();
        addTables(root);
        int ntables = tables.size();
        if (ntables > 30)
            // TODO: Need to select some simpler algorithm that scales better.
            throw new UnsupportedSQLException("Too many tables in query: " + ntables, 
                                              null);
        tableBitSets = new HashMap<Joinable,Long>(ntables);
        for (int i = 0; i < ntables; i++) {
            Joinable table = tables.get(i);
            Long bitset = JoinableBitSet.of(i);
            tableBitSets.put(table, bitset);
            if (table instanceof TableJoins) {
                for (TableSource joinedTable : ((TableJoins)table).getTables()) {
                    tableBitSets.put(joinedTable, bitset);
                }
            }
            else if (table instanceof TableGroupJoinTree) {
                for (TableGroupJoinTree.TableGroupJoinNode node : (TableGroupJoinTree)table) {
                    tableBitSets.put(node.getTable(), bitset);
                }
            }
        }
        ExpressionTables visitor = new ExpressionTables(tableBitSets);
        noperators = 0;
        JoinOperator rootOp = initSES(root, visitor);
        if (whereConditions != null)
            noperators += whereConditions.size(); // Maximum possible addition.
        operators = new ArrayList<JoinOperator>(noperators);
        nedges = noperators * 2;
        edges = new long[nedges];
        calcTES(rootOp);
        if (whereConditions != null)
            addWhereConditions(whereConditions, visitor);
        noperators = operators.size();
        nedges = noperators * 2;
        if (TRACE) {
            for (int e = 0; e < nedges; e += 2) {
                System.out.println(JoinableBitSet.toString(edges[e], tables) + " <-> " +
                                   JoinableBitSet.toString(edges[e^1], tables));
            }
        }
        evaluateOperators = new ArrayList<JoinOperator>(noperators);
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

    public static class JoinOperator {
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

        public JoinOperator() {
            joinConditions = new ConditionList(0);
        }

        public long getTables() {
            return JoinableBitSet.union(leftTables, rightTables);
        }

        public JoinNode getJoin() {
            return join;
        }
        public ConditionList getJoinConditions() {
            return joinConditions;
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
                op.leftTables = tableBitSets.get(left);
            }
            Joinable right = join.getRight();
            JoinOperator rightOp = initSES(right, visitor);
            if (rightOp != null) {
                rightOp.parent = op;
                op.right = rightOp;
                op.rightTables = rightOp.getTables();
            }
            else {
                op.rightTables = tableBitSets.get(right);
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
        case RIGHT:
            assert false;       // Should not see right join at this point.
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
        return (commuteJoinType(joinType) != null);
    }

    protected JoinType commuteJoinType(JoinType joinType) {
        switch (joinType) {
        case INNER:
        case FULL_OUTER:
            return joinType;
        case SEMI_INNER_ALREADY_DISTINCT:
            return JoinType.INNER;
        case SEMI_INNER_IF_DISTINCT:
            return JoinType.INNER_NEED_DISTINCT;
        default:
            return null;
        }
    }

    /** Is this operator and everything below it inner joined?
     * In that case a null-tolerant predicate doesn't interfere with
     * reordering since none of the nulls are induced by joins.
     */
    protected boolean allInnerJoins(JoinOperator op) {
        return ((op.getJoinType() == JoinType.INNER) &&
                ((op.left == null) || allInnerJoins(op.left)) &&
                ((op.right == null) || allInnerJoins(op.right)));
    }

    /** Get join conditions from top-level WHERE predicates. */
    protected void addWhereConditions(ConditionList whereConditions, 
                                      ExpressionTables visitor) {
        Iterator<ConditionExpression> iter = whereConditions.iterator();
        while (iter.hasNext()) {
            ConditionExpression condition = iter.next();
            // TODO: When optimizer supports more predicates
            // interestingly, can recognize them, including
            // generalized hypergraph triples.
            if (condition instanceof ComparisonCondition) {
                ComparisonCondition comp = (ComparisonCondition)condition;
                long columnTables = columnReferenceTable(comp.getLeft());
                if (!JoinableBitSet.isEmpty(columnTables)) {
                    long rhs = visitor.getTables(comp.getRight());
                    if (visitor.wasNullTolerant()) continue;
                    if (addWhereCondition(condition, columnTables, rhs)) {
                        iter.remove();
                        continue;
                    }
                }
                columnTables = columnReferenceTable(comp.getRight());
                if (!JoinableBitSet.isEmpty(columnTables)) {
                    long lhs = visitor.getTables(comp.getLeft());
                    if (visitor.wasNullTolerant()) continue;
                    if (addWhereCondition(condition, columnTables, lhs)) {
                        iter.remove();
                        continue;
                    }
                }
            }
        }
    }

    /** Is this a single column in a known table? */
    protected long columnReferenceTable(ExpressionNode node) {
        if (node instanceof ColumnExpression) {
            Long bitset = tableBitSets.get(((ColumnExpression)node).getTable());
            if (bitset != null) {
                return bitset;
            }
        }
        return JoinableBitSet.empty();
    }

    /** Add an edge for the tables in this simple condition. 
     * There are no extra reordering constraints, since this is just a
     * WHERE condition. Furthermore, its tables ought to be in INNER
     * joins, since such a null-intolerent condition implies that they
     * aren't missing.
     */
    protected boolean addWhereCondition(ConditionExpression condition,
                                        long columnTables, long comparisonTables) {
        if (!JoinableBitSet.isEmpty(comparisonTables) &&
            !JoinableBitSet.overlaps(columnTables, comparisonTables)) {
            JoinOperator op = new JoinOperator(condition);
            op.leftTables = columnTables;
            op.rightTables = comparisonTables;
            op.predicateTables = op.getTables();
            int o = operators.size();
            operators.add(op);
            edges[o*2] = columnTables;
            edges[o*2+1] = comparisonTables;
            return true;
        }
        return false;
    }

    /** Compute tables used in join predicate. */
    static class ExpressionTables implements ExpressionVisitor, PlanVisitor {
        Map<Joinable,Long> tableBitSets;
        long result;
        boolean nullTolerant;

        public ExpressionTables(Map<Joinable,Long> tableBitSets) {
            this.tableBitSets = tableBitSets;
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
                Long bitset = tableBitSets.get(((ColumnExpression)n).getTable());
                if (bitset != null) {
                    result = JoinableBitSet.union(result, bitset);
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
    
    /** Add edges to make the hypergraph connected, using the stuck state. */
    protected void addExtraEdges() {
        // Get all the hypernodes that are not subsets of some filled hypernode.
        BitSet maximal = new BitSet(plans.length);
        for (int i = 0; i < plans.length; i++) {
            if (plans[i] != null) {
                maximal.set(i);
            }
        }
        for (int i = plans.length - 1; i > 2; i--) { // (3 is the smallest with subsets)
            if (maximal.get(i)) {
                long subset = JoinableBitSet.empty();
                while (true) {
                    subset = JoinableBitSet.nextSubset(subset, i);
                    if (JoinableBitSet.equals(subset, i)) break;
                    maximal.clear((int)subset);
                }
            }
        }
        int count = maximal.cardinality();
        assert (count > 1) : "Found less than 2 unconnected subgraphs";
        noperators += count - 1;
        nedges = noperators * 2;
        long[] newEdges = new long[nedges];
        System.arraycopy(edges, 0, newEdges, 0, edges.length);
        edges = newEdges;
        // Just connect them all up. This is by no means an optimal
        // set of new edges, but the plan is trouble to begin with
        // since it involved a cross product. Would need something
        // like a min cut hypergraph partition.
        long left = JoinableBitSet.empty();
        int i = -1;
        while (true) {
            i = maximal.nextSetBit(i+1);
            if (i < 0) break;
            if (JoinableBitSet.isEmpty(left)) {
                left = i;
            }
            else {
                addExtraEdge(left, i);
                left = JoinableBitSet.union(left, i);
            }
        }
        assert (noperators == operators.size());
    }

    protected void addExtraEdge(long left, long right) {
        if (TRACE)
            System.out.println("Extra: " + JoinableBitSet.toString(left, tables) +
                               " <->  " + JoinableBitSet.toString(right, tables));
        JoinOperator op = new JoinOperator();
        op.leftTables = left;
        op.rightTables = right;
        int o = operators.size();
        operators.add(op);
        edges[o*2] = left;
        edges[o*2+1] = right;
    }

}
