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
import com.akiban.sql.optimizer.plan.ConditionExpression;
import com.akiban.sql.optimizer.plan.ConstantExpression;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.akiban.sql.optimizer.rule.range.TUtils.*;
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

    private <T> Set<T> set(T... args) {
        Set<T> result = new HashSet<T>();
        Collections.addAll(result, args);
        return result;
    }
}
