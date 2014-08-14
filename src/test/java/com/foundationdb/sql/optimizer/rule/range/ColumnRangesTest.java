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
import com.foundationdb.sql.optimizer.plan.ColumnExpression;
import com.foundationdb.sql.optimizer.plan.ConditionExpression;
import com.foundationdb.sql.optimizer.plan.ConstantExpression;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.foundationdb.sql.optimizer.rule.range.TUtils.*;
import static org.junit.Assert.assertEquals;

public final class ColumnRangesTest {

    @Test
    public void colLtValue() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(firstName, Comparison.LT, value);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                segment(RangeEndpoint.NULL_EXCLUSIVE, exclusive("joe"))
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void valueLtCol() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(value, Comparison.LT, firstName);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                segment(exclusive("joe"), RangeEndpoint.UPPER_WILD)
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void colLeValue() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(firstName, Comparison.LE, value);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                segment(RangeEndpoint.NULL_EXCLUSIVE, inclusive("joe"))
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void valueLeCol() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(value, Comparison.LE, firstName);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                segment(inclusive("joe"), RangeEndpoint.UPPER_WILD)
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void colGtValue() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(firstName, Comparison.GT, value);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                segment(exclusive("joe"), RangeEndpoint.UPPER_WILD)
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void valueGtCol() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(value, Comparison.GT, firstName);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                segment(RangeEndpoint.NULL_EXCLUSIVE, exclusive("joe"))
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void colGeValue() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(firstName, Comparison.GE, value);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                segment(inclusive("joe"), RangeEndpoint.UPPER_WILD)
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void valueGeCol() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(value, Comparison.GE, firstName);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                segment(RangeEndpoint.NULL_EXCLUSIVE, inclusive("joe"))
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void colEqValue() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(firstName, Comparison.EQ, value);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                segment(inclusive("joe"), inclusive("joe"))
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void valueEqCol() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(value, Comparison.EQ, firstName);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                segment(inclusive("joe"), inclusive("joe"))
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void colNeValue() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(firstName, Comparison.NE, value);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                    segment(RangeEndpoint.NULL_EXCLUSIVE, exclusive("joe")),
                    segment(exclusive("joe"), RangeEndpoint.UPPER_WILD)
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void valueNeCol() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(value, Comparison.NE, firstName);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                segment(RangeEndpoint.NULL_EXCLUSIVE, exclusive("joe")),
                segment(exclusive("joe"), RangeEndpoint.UPPER_WILD)
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }
    
    @Test
    public void notColLtValue() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = not(compare(value, Comparison.LT, firstName));
        ColumnRanges expected = null;
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void columnIsNull() {
        ConditionExpression isNull = isNull(firstName);
        ColumnRanges expected = columnRanges(
                firstName,
                isNull,
                segment(RangeEndpoint.NULL_INCLUSIVE, RangeEndpoint.NULL_INCLUSIVE)
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(isNull));
    }

    @Test
    public void differentColumns() {
        ConditionExpression firstNameLtJoe = compare(firstName, Comparison.LT, constant("joe"));
        ConditionExpression lastNameLtSmith = compare(lastName, Comparison.LT, constant("smith"));
        ConditionExpression either = or(firstNameLtJoe, lastNameLtSmith);
        ColumnRanges expected = null;
        assertEquals(expected, ColumnRanges.rangeAtNode(either));
    }

    // the and/or tests are pretty sparse, since RangeSegmentTest is more exhaustive about
    // the overlaps and permutations.

    @Test
    public void orNoOverlap() {
        ConditionExpression nameLtAbe = compare(firstName, Comparison.LT, constant("abe"));
        ConditionExpression nameGeJoe = compare(firstName, Comparison.GE, constant("joe"));
        ConditionExpression either = or(nameLtAbe, nameGeJoe);
        ColumnRanges expected = columnRanges(
                firstName,
                either,
                segment(RangeEndpoint.NULL_EXCLUSIVE, exclusive("abe")),
                segment(inclusive("joe"), RangeEndpoint.UPPER_WILD)
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(either));
    }

    @Test
    public void orWithOverlap() {
        ConditionExpression nameLtAbe = compare(firstName, Comparison.LT, constant("joe"));
        ConditionExpression nameGeJoe = compare(firstName, Comparison.GE, constant("abe"));
        ConditionExpression either = or(nameLtAbe, nameGeJoe);
        ColumnRanges expected = columnRanges(
                firstName,
                either,
                segment(RangeEndpoint.NULL_EXCLUSIVE, RangeEndpoint.UPPER_WILD)
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(either));
    }

    @Test
    public void andNoOverlap() {
        ConditionExpression nameLtAbe = compare(firstName, Comparison.LT, constant("abe"));
        ConditionExpression nameGeJoe = compare(firstName, Comparison.GE, constant("joe"));
        ConditionExpression both = and(nameLtAbe, nameGeJoe);
        ColumnRanges expected = columnRanges(
                firstName,
                both
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(both));
    }

    @Test
    public void andWithOverlap() {
        ConditionExpression nameLtAbe = compare(firstName, Comparison.LT, constant("joe"));
        ConditionExpression nameGeJoe = compare(firstName, Comparison.GE, constant("abe"));
        ConditionExpression both = and(nameLtAbe, nameGeJoe);
        ColumnRanges expected = columnRanges(
                firstName,
                both,
                segment(inclusive("abe"), exclusive("joe"))
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(both));
    }

    @Test
    public void explicitAnd() {
        ConditionExpression nameLtAbe = compare(firstName, Comparison.LT, constant("joe"));
        ConditionExpression nameGeJoe = compare(firstName, Comparison.GE, constant("abe"));
        ColumnRanges nameLtAbeRanges = ColumnRanges.rangeAtNode(nameLtAbe);
        ColumnRanges nameGeJoeRanges = ColumnRanges.rangeAtNode(nameGeJoe);
        ColumnRanges expected = columnRanges(
                firstName,
                set(nameLtAbe, nameGeJoe),
                segment(inclusive("abe"), exclusive("joe"))
        );
        assertEquals(expected, ColumnRanges.andRanges(nameLtAbeRanges, nameGeJoeRanges));
        assertEquals(expected, ColumnRanges.andRanges(nameGeJoeRanges, nameLtAbeRanges));
    }

    @Test
    public void sinOfColumn() {
        ConditionExpression isNull = sin(firstName);
        ColumnRanges expected = null;
        assertEquals(expected, ColumnRanges.rangeAtNode(isNull));
    }
    
    private ColumnRanges columnRanges(ColumnExpression col, ConditionExpression condition, RangeSegment... segments) {
        return new ColumnRanges(
                col,
                Collections.singleton(condition),
                Arrays.asList(segments)
        );
    }

    private ColumnRanges columnRanges(ColumnExpression col, Set<ConditionExpression> conditions, RangeSegment... segs) {
        return new ColumnRanges(
                col,
                conditions,
                Arrays.asList(segs)
        );
    }

    private Set<ConditionExpression> set(ConditionExpression... args) {
        Set<ConditionExpression> result = new HashSet<>();
        for (ConditionExpression object : args) {
            result.add(object);
        }
        return result;
    }
}
