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

package com.akiban.sql.optimizer.rule;

import com.akiban.server.expression.std.Comparison;
import com.akiban.sql.optimizer.plan.ColumnExpression;
import com.akiban.sql.optimizer.plan.ComparisonCondition;
import com.akiban.sql.optimizer.plan.ConditionExpression;
import com.akiban.sql.optimizer.plan.ConstantExpression;
import com.akiban.sql.optimizer.plan.ExpressionNode;
import com.akiban.sql.optimizer.plan.FunctionCondition;
import com.akiban.sql.optimizer.plan.LogicalFunctionCondition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public final class Range {

    public static Range rangeAtNode(ConditionExpression node) {
        if (node instanceof ComparisonCondition) {
            ComparisonCondition comparisonCondition = (ComparisonCondition) node;
            return comparisonToRange(comparisonCondition);
        }
        else if (node instanceof LogicalFunctionCondition) {
            LogicalFunctionCondition condition = (LogicalFunctionCondition) node;
            Range leftRange = rangeAtNode(condition.getLeft());
            Range rightRange = rangeAtNode(condition.getRight());
            if (leftRange != null && rightRange != null) {
                List<RangeSegment> combinedSegments = combineBool(leftRange, rightRange, condition.getFunction());
                if (combinedSegments != null) {
                    return new Range(leftRange.getColumnExpression(), condition, combinedSegments);
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
                        return new Range(operandColumn, condition, Collections.singletonList(RangeSegment.ONLY_NULL));
                    }
                }
            }
        }
        return null;
    }

    public static Range andRanges(Range left, Range right) {
        List<RangeSegment> combinedSegments = combineBool(left, right, true);
        if (combinedSegments == null)
            return null;
        List<ConditionExpression> combinedConditions = new ArrayList<ConditionExpression>(left.getConditions());
        combinedConditions.addAll(right.getConditions());
        return new Range(left.getColumnExpression(), combinedConditions, combinedSegments);
    }

    private static List<RangeSegment> combineBool(Range leftRange, Range rightRange, boolean isAnd) {
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
    
    private static List<RangeSegment> combineBool(Range leftRange, Range rightRange, String logicOp) {
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

    @Override
    public String toString() {
        return "Range " + columnExpression + ' ' + segments;
    }

    public Range(ColumnExpression columnExpression, Collection<? extends ConditionExpression> rootConditions, List<RangeSegment> segments) {
        this.columnExpression = columnExpression;
        this.rootConditions = rootConditions;
        this.segments = segments;
    }

    public Range(ColumnExpression columnExpression, ConditionExpression rootCondition, List<RangeSegment> segments) {
        this(columnExpression, Collections.singletonList(rootCondition), segments);
    }

    private static Range comparisonToRange(ComparisonCondition comparisonCondition) {
        final ColumnExpression columnExpression;
        final ExpressionNode other;
        if (comparisonCondition.getLeft() instanceof ColumnExpression) {
            columnExpression = (ColumnExpression) comparisonCondition.getLeft();
            other = comparisonCondition.getRight();
        }
        else if (comparisonCondition.getRight() instanceof ColumnExpression) {
            columnExpression = (ColumnExpression) comparisonCondition.getRight();
            other = comparisonCondition.getLeft();
        }
        else {
            return null;
        }
        if (other instanceof ConstantExpression) {
            ConstantExpression constant = (ConstantExpression) other;
            Comparison op = comparisonCondition.getOperation();
            List<RangeSegment> rangeSegments = RangeSegment.fromComparison(op, constant);
            return new Range(columnExpression, comparisonCondition, rangeSegments);
        }
        else {
            return null;
        }
    }

    private ColumnExpression columnExpression;
    private Collection<? extends ConditionExpression> rootConditions;
    private List<RangeSegment> segments;
}
