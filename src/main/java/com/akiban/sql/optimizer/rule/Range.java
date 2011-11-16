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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public final class Range {

    private static final Logger log = LoggerFactory.getLogger(Range.class);

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

    private static void insertIntoMap(ExpressionNode node, Range range, Map<ExpressionNode, Range> results) {
        Range old = results.put(node, range);
        assert old == null : old;
    }

    private static List<RangeSegment> combineBool(Range leftRange, Range rightRange, String logicOp) {
        if (!leftRange.getColumnExpression().equals(rightRange.getColumnExpression()))
            return null;
        logicOp = logicOp.toLowerCase();
        List<RangeSegment> leftSegments = leftRange.getSegments();
        List<RangeSegment> rightSegments = rightRange.getSegments();
        List<RangeSegment> result;
        if ("and".endsWith(logicOp))
            result = andRanges(leftSegments, rightSegments);
        else if ("or".equals(logicOp))
            result = orRanges(leftSegments, rightSegments);
        else
            result = null;
        if (result != null)
            result = sortAndCombine(result);
        return result;
    }

    static List<RangeSegment> sortAndCombine(List<RangeSegment> segments) {
        try {
            Collections.sort(segments, RANGE_SEGMENTS_BY_START);
        } catch (IllegalComparisonException e) {
            log.warn("illegal comparison in sorting/combining range segments", e);
            return null;
        }
        // General algorithm:
        // - iterate over each RangeSegment.
        // - if this is the first RangeSegment, let it be. Otherwise...
        // -- if its start is <= the end of the previous one...
        // --- if its end is <= the end of the previous one, simply remove it
        // --- otherwise, remove both and replace them with a RangeSegment whose start is the previous one and whose end
        // is the new one; this is now the new previous
        // -- else, if start > the end of the previous one, this is the new current
        //
        // When comparing starts, WILD <= anything; and when comparing ends, WILD >= anything.
        RangeSegment previous = null;
        
        for (ListIterator<RangeSegment> iterator = segments.listIterator(); iterator.hasNext(); ) {
            RangeSegment currentSegment = iterator.next();
            final RangeSegment nextPrevious;
            if (previous == null) {
                nextPrevious = currentSegment;
            }
            else {
                ComparisonResult comparison = compareEndpoints(previous.getEnd(), currentSegment.getStart(), WILD_IS_LOW);
                if (comparison == ComparisonResult.LT || comparison == ComparisonResult.GT) {
                    ComparisonResult endsComparison = compareEndpoints(previous.getEnd(), currentSegment.getEnd(), WILD_IS_HIGH);
                    if (endsComparison == ComparisonResult.LT || endsComparison == ComparisonResult.GT) {
                        iterator.remove();
                        nextPrevious = previous;
                    }
                    else {
                        RangeSegment prev2 = iterator.previous();
                        assert prev2 == previous : prev2 + " != " + previous;
                        iterator.remove();
                        RangeSegment curr2 = iterator.next();
                        assert curr2 == currentSegment : curr2 + " != " + currentSegment;
                        nextPrevious = new RangeSegment(previous.getStart(), currentSegment.getEnd());
                        iterator.set(nextPrevious);
                    }
                }
                else {
                    nextPrevious = currentSegment;
                }
            }
            previous = nextPrevious;
        }
        return segments;
    }

    private static List<RangeSegment> orRanges(List<RangeSegment> leftRanges, List<RangeSegment> rightRanges) {
        List<RangeSegment> bothSegments = new ArrayList<RangeSegment>(leftRanges);
        bothSegments.addAll(rightRanges);
        return bothSegments;
    }

    private static List<RangeSegment> andRanges(List<RangeSegment> leftRanges, List<RangeSegment> rightRanges) {
        List<RangeSegment> results = new ArrayList<RangeSegment>();
        for (RangeSegment leftSegment : leftRanges) {
            for (RangeSegment rightSegment : rightRanges) {
                RangeSegment result = andRangeSegment(leftSegment, rightSegment);
                if (result != null)
                    results.add(result);
            }
        }
        return results;
    }

    private static RangeSegment andRangeSegment(RangeSegment left, RangeSegment right) {
        RangeEndpoint start = rangeEndpoint(left.getStart(), right.getStart(), RangePointComparison.MAX);
        RangeEndpoint end = rangeEndpoint(left.getEnd(), right.getEnd(), RangePointComparison.MIN);
        if (
                start == null
                || end == null
                || (start.hasValue()
                    && end.hasValue()
                    && (
                            RangePointComparison.MIN.get(
                                    start.asValueEndpoint().getValue(),
                                    end.asValueEndpoint().getValue()
                            ) != start.asValueEndpoint().getValue())
                    )
            )
            return null;
        return new RangeSegment(start, end);
    }

    private static RangeEndpoint rangeEndpoint(RangeEndpoint one, RangeEndpoint two, RangePointComparison comparison) {
        if (one.isWild())
            return two;
        if (two.isWild())
            return one;
        RangeEndpoint.ValueEndpoint oneValue = one.asValueEndpoint();
        RangeEndpoint.ValueEndpoint twoValue = two.asValueEndpoint();
        Object resultValue = comparison.get(oneValue.getValue(), twoValue.getValue());
        if (resultValue == null)
            return null;
        boolean resultInclusive = oneValue.isInclusive() && twoValue.isInclusive();
        return RangeEndpoint.of(resultValue, resultInclusive);
    }

    private enum RangePointComparison {
        MIN {
            @Override
            protected Object select(Object one, Object two, int comparison) {
                return comparison < 0 ? one : two;
            }
        },
        MAX {
            @Override
            protected Object select(Object one, Object two, int comparison) {
                return comparison > 0 ? one : two;
            }
        }
        ;

        protected abstract Object select(Object one, Object two, int comparison);

        public Object get(Object one, Object two) {
            ComparisonResult compareResult = compareObjects(one, two);
            switch (compareResult) {
            case EQ:
            case LT:
                return one;
            case GT:
                return two;
            case INVALID:
                return null;
            default:
                throw new AssertionError(compareResult.name());
            }
        }
    }

    private static ComparisonResult compareObjects(Object one, Object two) {
        // if both are null, they're equal. Otherwise, at most one can be null; if either is null, we know the
        // answer. Otherwise, we know neither is null, and we can test their classes
        if (one == null && two == null)
            return ComparisonResult.EQ;
        if (one == null)
            return ComparisonResult.LT;
        if (two == null)
            return ComparisonResult.GT;
        if (!one.getClass().equals(two.getClass()) || !Comparable.class.isInstance(one))
            return ComparisonResult.INVALID;
        Comparable oneT = (Comparable) one;
        Comparable twoT = (Comparable) two;
        @SuppressWarnings("unchecked") // we know that oneT and twoT are both Comparables of the same class
        int compareResult = (oneT).compareTo(twoT);
        if (compareResult < 0)
            return ComparisonResult.LT;
        else if (compareResult > 0)
            return ComparisonResult.GT;
        else
            return ComparisonResult.EQ;
    }

    private enum ComparisonResult {
        LT, GT, EQ, INVALID
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

    private static ComparisonResult compareEndpoints(RangeEndpoint point1, RangeEndpoint point2, boolean wildIsHigh) {
        boolean point1Wild = point1.isWild();
        boolean point2Wild = point2.isWild();
        if (point1Wild && point2Wild)
            return ComparisonResult.EQ;
        if (point1Wild) // point 2 isn't
            return wildIsHigh ? ComparisonResult.GT : ComparisonResult.LT;
        if (point2Wild) // point 1 isn't
            return wildIsHigh ? ComparisonResult.LT : ComparisonResult.GT;
        // neither point is wild
        RangeEndpoint.ValueEndpoint value1 = point1.asValueEndpoint();
        RangeEndpoint.ValueEndpoint value2 = point2.asValueEndpoint();
        return compareObjects(value1.getValue(), value2.getValue());
    }

    private static boolean WILD_IS_HIGH = true;
    private static boolean WILD_IS_LOW = false;

    private ColumnExpression columnExpression;
    private List<RangeSegment> segments;

    private static final Comparator<? super RangeSegment> RANGE_SEGMENTS_BY_START = new Comparator<RangeSegment>() {
        @Override
        public int compare(RangeSegment segment1, RangeSegment segment2) {
            RangeEndpoint start1 = segment1.getStart();
            RangeEndpoint start2 = segment2.getStart();
            ComparisonResult comparisonResult = compareEndpoints(start1, start2, WILD_IS_LOW);
            switch (comparisonResult) {
            case EQ: return 0;
            case LT: return -1;
            case GT: return 1;
            default: throw new IllegalComparisonException(segment1, segment2);
            }
        }
    };

    private static class IllegalComparisonException extends RuntimeException {
        public IllegalComparisonException(Object one, Object two) {
            super(String.format("couldn't sort objects <%s> and <%s>",
                    one,
                    two
            ));
        }
    }
}
