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

package com.akiban.sql.optimizer.rule.range;

import com.akiban.server.expression.std.Comparison;
import com.akiban.sql.optimizer.plan.ColumnExpression;
import com.akiban.sql.optimizer.plan.ComparisonCondition;
import com.akiban.sql.optimizer.plan.ConditionExpression;
import com.akiban.sql.optimizer.plan.ConstantExpression;
import com.akiban.sql.optimizer.plan.ExpressionNode;
import com.akiban.sql.optimizer.plan.FunctionCondition;
import com.akiban.sql.optimizer.plan.InListCondition;
import com.akiban.sql.optimizer.plan.LogicalFunctionCondition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class ColumnRanges {

    public static ColumnRanges rangeAtNode(ConditionExpression node) {
        if (node instanceof ComparisonCondition) {
            ComparisonCondition comparisonCondition = (ComparisonCondition) node;
            return comparisonToRange(comparisonCondition);
        }
        else if (node instanceof LogicalFunctionCondition) {
            LogicalFunctionCondition condition = (LogicalFunctionCondition) node;
            if (condition.getOperands().size() != 2)
                return null;
            ColumnRanges leftRange = rangeAtNode(condition.getLeft());
            ColumnRanges rightRange = rangeAtNode(condition.getRight());
            if (leftRange != null && rightRange != null) {
                List<RangeSegment> combinedSegments = combineBool(leftRange, rightRange, condition.getFunction());
                if (combinedSegments != null) {
                    return new ColumnRanges(leftRange.getColumnExpression(), condition, combinedSegments);
                }
            }
        }
        else if (node instanceof FunctionCondition) {
            FunctionCondition condition = (FunctionCondition) node;
            if ("isNull".equals(condition.getFunction())) {
                if (condition.getOperands().size() == 1) {
                    ExpressionNode operand = condition.getOperands().get(0);
                    if (operand instanceof ColumnExpression) {
                        ColumnExpression operandColumn = (ColumnExpression) operand;
                        return new ColumnRanges(operandColumn, condition, Collections.singletonList(RangeSegment.ONLY_NULL));
                    }
                }
            }
        }
        else if (node instanceof InListCondition) {
            InListCondition inListCondition = (InListCondition) node;
            return inListToRange(inListCondition);
        }
        return null;
    }

    public static ColumnRanges andRanges(ColumnRanges left, ColumnRanges right) {
        List<RangeSegment> combinedSegments = combineBool(left, right, true);
        if (combinedSegments == null)
            return null;
        Set<ConditionExpression> combinedConditions = new HashSet<ConditionExpression>(left.getConditions());
        combinedConditions.addAll(right.getConditions());
        return new ColumnRanges(left.getColumnExpression(), combinedConditions, combinedSegments);
    }

    private static List<RangeSegment> combineBool(ColumnRanges leftRange, ColumnRanges rightRange, boolean isAnd) {
        if (!leftRange.getColumnExpression().equals(rightRange.getColumnExpression()))
            return null;
        List<RangeSegment> leftSegments = leftRange.getSegments();
        List<RangeSegment> rightSegments = rightRange.getSegments();
        List<RangeSegment> result;
        if (isAnd)
            result = RangeSegment.andRanges(leftSegments, rightSegments);
        else
            result = RangeSegment.orRanges(leftSegments, rightSegments);
        if (result != null)
            result = RangeSegment.sortAndCombine(result);
        return result;
    }
    
    private static List<RangeSegment> combineBool(ColumnRanges leftRange, ColumnRanges rightRange, String logicOp) {
        logicOp = logicOp.toLowerCase();
        if ("and".endsWith(logicOp))
            return combineBool(leftRange, rightRange, true);
        else if ("or".equals(logicOp))
            return combineBool(leftRange, rightRange, false);
        else
            return null;
    }

    public Collection<? extends ConditionExpression> getConditions() {
        return rootConditions;
    }

    public List<RangeSegment> getSegments() {
        return segments;
    }

    public ColumnExpression getColumnExpression() {
        return columnExpression;
    }

    public String describeRanges() {
        return segments.toString();
    }

    public boolean isAllSingle() {
        boolean all = true;
        for (RangeSegment segment : segments) {
            if (!segment.isSingle()) {
                return false;
            }
        }
        return all;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnRanges that = (ColumnRanges) o;

        return columnExpression.equals(that.columnExpression)
                && rootConditions.equals(that.rootConditions)
                && segments.equals(that.segments);

    }

    @Override
    public int hashCode() {
        int result = columnExpression.hashCode();
        result = 31 * result + rootConditions.hashCode();
        result = 31 * result + segments.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Range " + columnExpression + ' ' + segments;
    }

    public ColumnRanges(ColumnExpression columnExpression, Set<? extends ConditionExpression> rootConditions,
                        List<RangeSegment> segments)
    {
        this.columnExpression = columnExpression;
        this.rootConditions = rootConditions;
        this.segments = segments;
    }

    public ColumnRanges(ColumnExpression columnExpression, ConditionExpression rootCondition,
                        List<RangeSegment> segments) {
        this(columnExpression, Collections.singleton(rootCondition), segments);
    }

    private static ColumnRanges comparisonToRange(ComparisonCondition comparisonCondition) {
        final ColumnExpression columnExpression;
        final ExpressionNode other;
        final boolean columnIsRight;
        if (comparisonCondition.getLeft() instanceof ColumnExpression) {
            columnExpression = (ColumnExpression) comparisonCondition.getLeft();
            other = comparisonCondition.getRight();
            columnIsRight = false;
        }
        else if (comparisonCondition.getRight() instanceof ColumnExpression) {
            columnExpression = (ColumnExpression) comparisonCondition.getRight();
            other = comparisonCondition.getLeft();
            columnIsRight = true;
        }
        else {
            return null;
        }
        if (other instanceof ConstantExpression) {
            ConstantExpression constant = (ConstantExpression) other;
            Comparison op = comparisonCondition.getOperation();
            if (columnIsRight) {
                op = flip(op);
            }
            List<RangeSegment> rangeSegments = RangeSegment.fromComparison(op, constant);
            return new ColumnRanges(columnExpression, comparisonCondition, rangeSegments);
        }
        else {
            return null;
        }
    }

    private static Comparison flip(Comparison op) {
        switch (op) {
            case LT:    return Comparison.GT;
            case LE:    return Comparison.GE;
            case GT:    return Comparison.LT;
            case GE:    return Comparison.LE;
            case EQ:
            case NE:    return op;
            default:    throw new AssertionError(op.name());
        }
    }

    private static ColumnRanges inListToRange (InListCondition inListCondition) {
        final ColumnExpression columnExpression;
        if (inListCondition.getOperand() instanceof ColumnExpression) {
            columnExpression = (ColumnExpression)inListCondition.getOperand();
        }
        else {
            return null;
        }
        List<ExpressionNode> expressions = inListCondition.getExpressions();
        List<RangeSegment> rangeSegments = new ArrayList<RangeSegment>(expressions.size());
        for (ExpressionNode expr : expressions) {
            if (expr instanceof ConstantExpression) {
                rangeSegments.addAll(RangeSegment.fromComparison(Comparison.EQ, 
                                                                 (ConstantExpression)expr));
            }
            else {
                return null;
            }
        }
        return new ColumnRanges(columnExpression, inListCondition, rangeSegments);
    }

    private ColumnExpression columnExpression;
    private Set<? extends ConditionExpression> rootConditions;
    private List<RangeSegment> segments;
}
