/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.optimizer.rule.range;

import com.foundationdb.server.types.texpressions.Comparison;
import com.foundationdb.sql.optimizer.plan.ConstantExpression;
import com.foundationdb.util.ArgumentValidation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;

public final class RangeSegment {

    private static final Logger log = LoggerFactory.getLogger(RangeSegment.class);

    public static final RangeSegment ONLY_NULL = new RangeSegment(RangeEndpoint.NULL_INCLUSIVE, RangeEndpoint.NULL_INCLUSIVE);

    public static List<RangeSegment> fromComparison(Comparison op, ConstantExpression constantExpression) {
        final RangeEndpoint startPoint;
        final RangeEndpoint endPoint;
        switch (op) {
        case EQ:
            startPoint = endPoint = RangeEndpoint.inclusive(constantExpression);
            break;
        case LT:
            startPoint = RangeEndpoint.NULL_EXCLUSIVE;
            endPoint = RangeEndpoint.exclusive(constantExpression);
            break;
        case LE:
            startPoint = RangeEndpoint.NULL_EXCLUSIVE;
            endPoint = RangeEndpoint.inclusive(constantExpression);
            break;
        case GT:
            startPoint = RangeEndpoint.exclusive(constantExpression);
            endPoint = RangeEndpoint.UPPER_WILD;
            break;
        case GE:
            startPoint = RangeEndpoint.inclusive(constantExpression);
            endPoint = RangeEndpoint.UPPER_WILD;
            break;
        case NE:
            List<RangeSegment> result = new ArrayList<>(2);
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
        } catch (RangeEndpoint.IllegalComparisonException e) {
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
                Boolean startsOverlap = findOverlap(previousEnd, currentStart, true);
                if (startsOverlap == null)
                    return null;
                if (startsOverlap) {
                    Boolean endsOverlap = findOverlap(previousEnd, currentSegment.getEnd(), false);
                    if (endsOverlap == null)
                        return null;
                    if (endsOverlap) {
                        iterator.remove();
                        nextPrevious = previous;
                    }
                    // previous end is < current end; extend by taking previous start and current end
                    else {
                        nextPrevious = new RangeSegment(previous.getStart(), currentSegment.getEnd());
                        replacePreviousTwo(iterator, previous, nextPrevious);
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

    private static void replacePreviousTwo(ListIterator<RangeSegment> iterator, RangeSegment previous,
                                           RangeSegment newValue) {
        // replace the previous iterator's value with this one
        iterator.set(newValue);
        // go back one; now looking at what we just set
        RangeSegment oneBack = iterator.previous();
        assert oneBack == newValue : oneBack + " != " + newValue;
        // go back again; now looking at the previous iteration's RangeSegment
        RangeSegment twoBack = iterator.previous();
        assert twoBack == previous : twoBack + " != " + previous;
        iterator.remove();
        // go forward one; now back to looking at the one we just created
        RangeSegment nowAt = iterator.next();
        assert nowAt == newValue : nowAt + " != " + newValue;
    }

    /**
     * Compares two RangePoints for overlap. The two overlap if high < low, or if the two are equal and at least
     * one of them is inclusive or wild.
     *
     * @param low the RangePoint which should be lower, if the two are not to overlap
     * @param high the RangePoint which should be higher, if the two are not to overlap
     * @param loose if true, a GT_BARELY comparison counts as an overlap; we want this when comparing ends,
     * but not starts
     * @return whether the two points overlap
     */
    private static Boolean findOverlap(RangeEndpoint low, RangeEndpoint high, boolean loose) {
        ComparisonResult comparison = low.comparePreciselyTo(high);
        switch (comparison) {
        case GT_BARELY: return loose;             // low > high only because of inclusiveness. Use the looseness.
        case EQ:        return low.isInclusive(); // if they're (both) exclusive, it's a discontinuity
        case LT_BARELY: // fall through           // low < high only because of inclusiveness. For starts, this is
                                                  // an overlap, and for ends, it can't happen (due to sorting)
        case GT:        return true;              // low > high, this is always an overlap
        case LT:        return false;             // low < high, this is never an overlap
        case INVALID:   return null;              // the two weren't comparable
        default: throw new AssertionError(comparison.name());
        }
    }

    static List<RangeSegment> orRanges(List<RangeSegment> leftRanges, List<RangeSegment> rightRanges) {
        List<RangeSegment> bothSegments = new ArrayList<>(leftRanges);
        bothSegments.addAll(rightRanges);
        return bothSegments;
    }

    static List<RangeSegment> andRanges(List<RangeSegment> leftRanges, List<RangeSegment> rightRanges) {
        List<RangeSegment> results = new ArrayList<>();
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
        RangeEndpoint start = rangeEndpoint(left.getStart(), right.getStart(), RangeEndpoint.RangePointComparison.MAX);
        RangeEndpoint end = rangeEndpoint(left.getEnd(), right.getEnd(), RangeEndpoint.RangePointComparison.MIN);
        // if either null, a comparison failed and we should bail
        // otherwise, if start > end, this is an empty range and we should bail; another iteration of the loop
        // will give us the correct order
        // about the inclusivity factor in compareEndpoints: this only kicks in if both points have equal value
        // but different inclusivity. In this case, we want to reject the segment either way. So we'll nudge this
        // to GT
        if (start == null || end == null)
            return null;
        ComparisonResult comparison = start.comparePreciselyTo(end);
        switch (comparison) {
        case GT:
        case GT_BARELY:
        case LT_BARELY:
            return null;
        }
        return new RangeSegment(start, end);
    }

    static RangeEndpoint rangeEndpoint(RangeEndpoint one, RangeEndpoint two, RangeEndpoint.RangePointComparison comparison) {
        if (one.isUpperWild())
            return comparison == RangeEndpoint.RangePointComparison.MAX ? one : two;
        if (two.isUpperWild())
            return comparison == RangeEndpoint.RangePointComparison.MIN ? one : two;

        Object resultValue = comparison.get(one.getValue(), two.getValue());
        if (resultValue == RangeEndpoint.RangePointComparison.INVALID_COMPARISON)
            return null;
        boolean resultInclusive = one.isInclusive() || two.isInclusive();
        ConstantExpression resultExpression;
        if (resultValue == one.getValue())
            resultExpression = one.getValueExpression();
        else if (resultValue == two.getValue())
            resultExpression = two.getValueExpression();
        else
            throw new AssertionError(String.valueOf(resultValue));
        return RangeEndpoint.of(
                resultExpression,
                resultInclusive
        );
    }

    public RangeEndpoint getStart() {
        return start;
    }

    public RangeEndpoint getEnd() {
        return end;
    }

    public boolean isSingle() {
        return start.equals(end);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (isSingle()) {
            sb.append("% = ");
            sb.append(start.describeValue());
        }
        else {
            sb.append(start.describeValue());
            sb.append(start.isInclusive() ? " <= % " : " < % ");
            if (!end.isUpperWild()) {
                sb.append(end.isInclusive() ? "<= " : "< ");
                sb.append(end.describeValue());
            }
        }
        return sb.toString();
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
        ArgumentValidation.notNull("start", start);
        ArgumentValidation.notNull("end", end);
        this.start = start;
        this.end = end;
    }

    private RangeEndpoint start;
    private RangeEndpoint end;

    private static final Comparator<? super RangeSegment> RANGE_SEGMENTS_BY_START = new Comparator<RangeSegment>() {
        @Override
        public int compare(RangeSegment segment1, RangeSegment segment2) {
            return segment1.getStart().compareTo(segment2.getStart());
        }
    };
}
