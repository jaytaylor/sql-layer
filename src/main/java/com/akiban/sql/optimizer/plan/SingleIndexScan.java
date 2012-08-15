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

package com.akiban.sql.optimizer.plan;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.UserTable;
import com.akiban.server.expression.std.Comparison;
import com.akiban.sql.optimizer.plan.ConditionsCount.HowMany;
import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;
import com.akiban.sql.optimizer.rule.range.ColumnRanges;

import java.util.ArrayList;
import java.util.List;

public final class SingleIndexScan extends IndexScan {

    private Index index;
    private ColumnRanges conditionRange;
    // First equalities in the order of the index.
    private List<ExpressionNode> equalityComparands;

    // This is how the indexed result will be ordered from using this index.
    private List<OrderByExpression> ordering;

    private OrderEffectiveness orderEffectiveness;

    // Conditions subsumed by this index.
    // TODO: any cases where a condition is only partially handled and
    // still needs to be checked with Select?
    private List<ConditionExpression> conditions;
    
    // Columns in order, should the index be used as covering.
    private List<ExpressionNode> columns;

    // Followed by an optional inequality.
    private ExpressionNode lowComparand, highComparand;
    // TODO: This doesn't work for merging: consider x < ? AND x <= ?.
    // May need building of index keys in the expressions subsystem.
    private boolean lowInclusive, highInclusive;

    public SingleIndexScan(Index index, TableSource table)
    {
        super(table);
        this.index = index;
    }
    
    public SingleIndexScan(Index index, TableSource rootMost, TableSource leafMost) {
        this(index, rootMost, rootMost, leafMost, leafMost);
    }

    public SingleIndexScan(Index index,
                     TableSource rootMostTable,
                     TableSource rootMostInnerTable,
                     TableSource leafMostInnerTable,
                     TableSource leafMostTable)
    {
        super(rootMostTable, rootMostInnerTable, leafMostInnerTable, leafMostTable);
        this.index = index;
    }

    public Index getIndex() {
        return index;
    }

    @Override
    public List<ExpressionNode> getColumns() {
        return columns;
    }

    public void setColumns(List<ExpressionNode> columns) {
        this.columns = columns;
    }

    public ColumnRanges getConditionRange() {
        return conditionRange;
    }

    public void addRangeCondition(ColumnRanges range) {
        assert conditionRange == null : conditionRange;
        conditionRange = range;
        internalGetConditions().addAll(range.getConditions());
    }

    public List<ExpressionNode> getEqualityComparands() {
        return equalityComparands;
    }

    public void addEqualityCondition(ConditionExpression condition,
                                     ExpressionNode comparand) {
        if (equalityComparands == null)
            equalityComparands = new ArrayList<ExpressionNode>();
        equalityComparands.add(comparand);
        internalGetConditions().add(condition);
    }

    public void addInequalityCondition(ConditionExpression condition,
                                       Comparison comparison,
                                       ExpressionNode comparand) {
        if ((comparison == Comparison.GT) || (comparison == Comparison.GE)) {
            if (lowComparand == null) {
                lowComparand = comparand;
                lowInclusive = (comparison == Comparison.GE);
            }
            else if (lowInclusive == (comparison == Comparison.GE)) {
                List<ExpressionNode> operands = new ArrayList<ExpressionNode>(2);
                operands.add(lowComparand);
                operands.add(comparand);
                lowComparand = new FunctionExpression("_max",
                        operands,
                        lowComparand.getSQLtype(),
                        null);
            }
            else
                // TODO: Could do the MAX anyway and test the conditions later.
                // Might take some refactoring to know which
                // conditions are already there.
                return;
        }
        else if ((comparison == Comparison.LT) || (comparison == Comparison.LE)) {
            if (highComparand == null) {
                highComparand = comparand;
                highInclusive = (comparison == Comparison.LE);
            }
            else if (highInclusive == (comparison == Comparison.LE)) {
                List<ExpressionNode> operands = new ArrayList<ExpressionNode>(2);
                operands.add(highComparand);
                operands.add(comparand);
                highComparand = new FunctionExpression("_min",
                        operands,
                        highComparand.getSQLtype(),
                        null);
            }
            else
                // Not really an inequality.
                return;
        }
        else {
            return;
        }

        internalGetConditions().add(condition);
    }

    @Override
    public ExpressionNode getLowComparand() {
        return lowComparand;
    }

    @Override
    public boolean isLowInclusive() {
        return lowInclusive;
    }

    @Override
    public ExpressionNode getHighComparand() {
        return highComparand;
    }

    @Override
    public boolean isHighInclusive() {
        return highInclusive;
    }

    public void setLowComparand(ExpressionNode comparand, boolean inclusive) {
        lowComparand = comparand;
        lowInclusive = inclusive;
    }

    public void setHighComparand(ExpressionNode comparand, boolean inclusive) {
        highComparand = comparand;
        highInclusive = inclusive;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        if (lowComparand != null)
            lowComparand = (ConditionExpression)lowComparand.duplicate(map);
        if (highComparand != null)
            highComparand = (ConditionExpression)highComparand.duplicate(map);
        equalityComparands = duplicateList(equalityComparands, map);
        ordering = duplicateList(ordering, map);
    }

    @Override
    public List<OrderByExpression> getOrdering() {
        return ordering;
    }

    public void setOrdering(List<OrderByExpression> ordering) {
        this.ordering = ordering;
    }

    public List<ConditionExpression> getConditions() {
        return conditions;
    }

    public boolean hasConditions() {
        return ((conditions != null) && !conditions.isEmpty());
    }

    @Override
    public OrderEffectiveness getOrderEffectiveness() {
        return orderEffectiveness;
    }

    public void setOrderEffectiveness(OrderEffectiveness orderEffectiveness) {
        this.orderEffectiveness = orderEffectiveness;
    }

    @Override
    public List<IndexColumn> getIndexColumns() {
        return index.getAllColumns();
    }

    @Override
    protected String summarizeIndex(int indentation) {
        return String.valueOf(index);
    }

    @Override
    public boolean isAscendingAt(int i) {
        return index.getAllColumns().get(i).isAscending();
    }

    @Override
    public boolean isRecoverableAt(int i) {
        if (index.isSpatial()) return false;
        return index.getAllColumns().get(i).isRecoverable();
    }

    @Override
    public UserTable getLeafMostUTable() {
        return (UserTable) index.leafMostTable();
    }

    @Override
    public List<IndexColumn> getAllColumns() {
        return index.getAllColumns();
    }

    @Override
    public int getNEquality() {
        int nequals = 0;
        if (equalityComparands != null)
            nequals = equalityComparands.size();
        if ((conditionRange != null) && conditionRange.isAllSingle())
            nequals++;
        return nequals;
    }

    @Override
    public void incrementConditionsCounter(ConditionsCounter<? super ConditionExpression> counter) {
        for (ConditionExpression cond : getConditions())
            counter.increment(cond);
    }

    @Override
    public boolean isUseful(ConditionsCount<? super ConditionExpression> count) {
        for (ConditionExpression cond : getConditions()) {
            if (count.getCount(cond) == HowMany.ONE)
                return true;
        }
        return false;
    }

    @Override
    public UserTable findCommonAncestor(IndexScan otherScan) {
        TableSource myTable = getLeafMostTable();
        TableSource otherTable = otherScan.getLeafMostTable();
        int myDepth = myTable.getTable().getDepth();
        int otherDepth = otherTable.getTable().getDepth();
        while (myDepth > otherDepth) {
            myTable = myTable.getParentTable();
            myDepth = myTable.getTable().getDepth();
        }
        while (otherDepth > myDepth) {
            otherTable = otherTable.getParentTable();
            otherDepth = otherTable.getTable().getDepth();
        }
        while (myTable != otherTable) {
            myTable = myTable.getParentTable();
            otherTable = otherTable.getParentTable();
        }
        return myTable.getTable().getTable();
    }

    @Override
    protected void describeConditionRange(StringBuilder output) {
        if (conditionRange != null) {
            output.append(", UNIONs of ");
            output.append(conditionRange.describeRanges());
        }
    }

    @Override
    protected void describeEqualityComparands(StringBuilder output) {
        if (equalityComparands != null) {
            for (ExpressionNode expression : equalityComparands) {
                output.append(", =");
                output.append(expression);
            }
        }
    }

    private List<ConditionExpression> internalGetConditions() {
        if (conditions == null)
            conditions = new ArrayList<ConditionExpression>();
        return conditions;
    }

    @Override
    public void visitComparands(ExpressionRewriteVisitor v) {
        if (equalityComparands != null) {
            for (int i = 0; i < equalityComparands.size(); i++) {
                if (equalityComparands.get(i) != null)
                    equalityComparands.set(i, equalityComparands.get(i).accept(v));
            }
        }
        if (lowComparand != null)
            lowComparand = lowComparand.accept(v);
        if (highComparand != null)
            highComparand = highComparand.accept(v);
    }

    @Override
    public void visitComparands(ExpressionVisitor v) {
        if (equalityComparands != null) {
            for (ExpressionNode comparand : equalityComparands) {
                if (comparand != null)
                    comparand.accept(v);
            }
        }
        if (lowComparand != null)
            lowComparand.accept(v);
        if (highComparand != null)
            highComparand.accept(v);
    }

}
