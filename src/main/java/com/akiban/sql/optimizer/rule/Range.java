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
import com.akiban.sql.optimizer.plan.ConstantExpression;
import com.akiban.sql.optimizer.plan.ExpressionNode;
import com.akiban.sql.optimizer.plan.FunctionCondition;
import com.akiban.sql.optimizer.plan.LogicalFunctionCondition;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Range {

    public static Map<ExpressionNode,Range> rangesRootedAt(ExpressionNode node) {
        Map<ExpressionNode,Range> results = new HashMap<ExpressionNode, Range>();
        Range topRange = nodeRange(node, results);
        insertIntoMap(node, topRange, results);
        return results;
    }

    private static Range nodeRange(ExpressionNode node, Map<ExpressionNode,Range> resultsMap) {
        assert !resultsMap.containsKey(node) : "possible circular reference with " + node + " in " + resultsMap;
        if (node instanceof ComparisonCondition) {
            ComparisonCondition comparisonCondition = (ComparisonCondition) node;
            return comparisonToRange(comparisonCondition);
        }
        else if (node instanceof LogicalFunctionCondition) {
            LogicalFunctionCondition condition = (LogicalFunctionCondition) node;
            Range leftRange = nodeRange(condition.getLeft(), resultsMap);
            Range rightRange = nodeRange(condition.getRight(), resultsMap);
            if (leftRange != null)
                insertIntoMap(condition.getLeft(), leftRange, resultsMap);
            if (rightRange != null)
                insertIntoMap(condition.getRight(), rightRange, resultsMap);
            if (leftRange != null && rightRange != null) {
                List<RangeSegment> combinedSegments = combineBool(leftRange, rightRange, condition.getFunction());
                if (combinedSegments != null)
                    return new Range(leftRange.getColumnExpression(), combinedSegments);
            }
        }
        else if(node instanceof FunctionCondition) {
            FunctionCondition condition = (FunctionCondition) node;
            if ("isNull".equals(condition.getFunction())) {
                if (condition.getOperands().size() == 1) {
                    ExpressionNode operand = condition.getOperands().get(0);
                    if (operand instanceof ColumnExpression) {
                        ColumnExpression operandColumn = (ColumnExpression) operand;
                        RangeSegment segment = new RangeSegment(RangeEndpoint.NULL_INCLUSIVE, RangeEndpoint.NULL_INCLUSIVE);
                        return new Range(operandColumn, Collections.singletonList(segment));
                    }
                }
            }
        }
        return null;
    }

    static List<RangeSegment> combineBool(Range leftRange, Range rightRange, String logicOp) {
        if (!leftRange.getColumnExpression().equals(rightRange.getColumnExpression()))
            return null;
        logicOp = logicOp.toLowerCase();
        List<RangeSegment> leftSegments = leftRange.getSegments();
        List<RangeSegment> rightSegments = rightRange.getSegments();
        List<RangeSegment> result;
        if ("and".endsWith(logicOp))
            result = RangeSegment.andRanges(leftSegments, rightSegments);
        else if ("or".equals(logicOp))
            result = RangeSegment.orRanges(leftSegments, rightSegments);
        else
            result = null;
        if (result != null)
            result = RangeSegment.sortAndCombine(result);
        return result;
    }

    private static void insertIntoMap(ExpressionNode node, Range range, Map<ExpressionNode, Range> results) {
        Range old = results.put(node, range);
        assert old == null : old;
    }

    public List<RangeSegment> getSegments() {
        return segments;
    }

    public ColumnExpression getColumnExpression() {
        return columnExpression;
    }

    @Override
    public String toString() {
        return String.format("Range %s: %s", columnExpression, segments);
    }

    public Range(ColumnExpression columnExpression, List<RangeSegment> segments) {
        this.columnExpression = columnExpression;
        this.segments = segments;
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
            return new Range(columnExpression, rangeSegments);
        }
        else {
            return null;
        }
    }

    private ColumnExpression columnExpression;
    private List<RangeSegment> segments;
}
