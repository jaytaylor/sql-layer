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
import com.akiban.sql.optimizer.plan.ConstantExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;

public final class RangeSegment {

    private static final Logger log = LoggerFactory.getLogger(RangeSegment.class);

    public static List<RangeSegment> fromComparison(Comparison op, ConstantExpression constantExpression) {
        final RangeEndpoint startPoint;
        final RangeEndpoint endPoint;
        Object constantValue = constantExpression.getValue();
        switch (op) {
        case EQ:
            startPoint = endPoint = RangeEndpoint.inclusive(constantValue);
            break;
        case LT:
            startPoint = RangeEndpoint.WILD;
            endPoint = RangeEndpoint.exclusive(constantValue);
            break;
        case LE:
            startPoint = RangeEndpoint.WILD;
            endPoint = RangeEndpoint.inclusive(constantValue);
            break;
        case GT:
            startPoint = RangeEndpoint.exclusive(constantValue);
            endPoint = RangeEndpoint.WILD;
            break;
        case GE:
            startPoint = RangeEndpoint.inclusive(constantValue);
            endPoint = RangeEndpoint.WILD;
            break;
        case NE:
            List<RangeSegment> result = new ArrayList<RangeSegment>(2);
            result.add(fromComparison(Comparison.LT, constantExpression).get(0));
            result.add(fromComparison(Comparison.GT, constantExpression).get(0));
            return result;
        default:
            throw new AssertionError(op.name());
        }
        RangeSegment result = new RangeSegment(startPoint, endPoint);
        return Collections.singletonList(result);
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
                RangeEndpoint previousEnd = previous.getEnd();
                RangeEndpoint currentStart = currentSegment.getStart();
                // "start" and "end" are relative to the previous. So, startsOverlap specifies whether
                // the current's end is less than the previous start; and endsOverlap specifies whether the current's
                // end is less than the previous end
                boolean startsOverlap = findOverlap(previousEnd, currentStart, WILD_IS_LOW);
                boolean endsOverlap = findOverlap(previousEnd, currentSegment.getEnd(), WILD_IS_HIGH);
                if (startsOverlap || endsOverlap) {
                    if (endsOverlap) {
                        iterator.remove();
                        nextPrevious = previous;
                    }
                    // previous end is < current end; extend by taking previous start and current end
                    else {
                        nextPrevious = new RangeSegment(previous.getStart(), currentSegment.getEnd());
                        // now, replace the previous two with this one
                        iterator.set(nextPrevious);
                        // go back one; now looking at what we just set
                        RangeSegment prev2 = iterator.previous();
                        assert prev2 == nextPrevious : prev2 + " != " + nextPrevious;
                        // go back again; now looking at the previous iteration's RangeSegment
                        RangeSegment prev3 = iterator.previous();
                        assert prev3 == previous : prev3 + " != " + previous;
                        iterator.remove();
                        // go forward one; now back to looking at the one we just created
                        RangeSegment curr2 = iterator.next();
                        assert curr2 == nextPrevious : curr2 + " != " + nextPrevious;
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

    /**
     * Compares two RangePoints for overlap. The two overlap if high < low, or if the two are equal and at least
     * one of them is inclusive or wild.
     * @param low the RangePoint which should be lower, if the two are not to overlap
     * @param high the RangePoint which should be higher, if the two are not to overlap
     * @param wildFlag whether WILD is considered high or low
     * @return whether the two points overlap
     */
    private static boolean findOverlap(RangeEndpoint low, RangeEndpoint high, boolean wildFlag) {
        ComparisonResult comparison = compareEndpoints(low, high, wildFlag);
        final boolean haveOverlap;
        if (comparison == ComparisonResult.GT) {
            haveOverlap = true; // previous end is >= current start; we have overlap
        }
        else if (comparison == ComparisonResult.EQ) {
            // only have overlap in certain situations...
            if (high.isWild() || low.isWild()) {
                haveOverlap = true;
            }
            else {
                RangeEndpoint.ValueEndpoint previousEndValuePoint = low.asValueEndpoint();
                RangeEndpoint.ValueEndpoint currentStartValuePoint = high.asValueEndpoint();
                haveOverlap = previousEndValuePoint.isInclusive() || currentStartValuePoint.isInclusive();
            }
        }
        else {
            haveOverlap = false;
        }
        return haveOverlap;
    }

    static List<RangeSegment> orRanges(List<RangeSegment> leftRanges, List<RangeSegment> rightRanges) {
        List<RangeSegment> bothSegments = new ArrayList<RangeSegment>(leftRanges);
        bothSegments.addAll(rightRanges);
        return bothSegments;
    }

    static List<RangeSegment> andRanges(List<RangeSegment> leftRanges, List<RangeSegment> rightRanges) {
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

    static RangeSegment andRangeSegment(RangeSegment left, RangeSegment right) {
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

    static RangeEndpoint rangeEndpoint(RangeEndpoint one, RangeEndpoint two, RangePointComparison comparison) {
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

    static ComparisonResult compareObjects(Object one, Object two) {
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

    static ComparisonResult compareEndpoints(RangeEndpoint point1, RangeEndpoint point2, boolean wildIsHigh) {
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

    public RangeEndpoint getStart() {
        return start;
    }

    public RangeEndpoint getEnd() {
        return end;
    }

    @Override
    public String toString() {
        return start + " to " + end;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RangeSegment that = (RangeSegment) o;
        return end.equals(that.end) && start.equals(that.start);
    }

    @Override
    public int hashCode() {
        int result = start.hashCode();
        result = 31 * result + end.hashCode();
        return result;
    }

    public RangeSegment(RangeEndpoint start, RangeEndpoint end) {
        this.start = start;
        this.end = end;
    }

    private RangeEndpoint start;
    private RangeEndpoint end;

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
            ComparisonResult compareResult = RangeSegment.compareObjects(one, two);
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

    private enum ComparisonResult {
        LT, GT, EQ, INVALID
    }

    private static final Comparator<? super RangeSegment> RANGE_SEGMENTS_BY_START = new Comparator<RangeSegment>() {
        @Override
        public int compare(RangeSegment segment1, RangeSegment segment2) {
            RangeEndpoint start1 = segment1.getStart();
            RangeEndpoint start2 = segment2.getStart();
            ComparisonResult comparisonResult = RangeSegment.compareEndpoints(start1, start2, WILD_IS_LOW);
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

    private static boolean WILD_IS_HIGH = true;
    private static boolean WILD_IS_LOW = false;
}
