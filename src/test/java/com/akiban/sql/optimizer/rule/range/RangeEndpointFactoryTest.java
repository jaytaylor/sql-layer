
package com.akiban.sql.optimizer.rule.range;

import com.akiban.server.expression.std.Comparison;
import com.akiban.sql.optimizer.plan.ConstantExpression;
import org.junit.Test;

import java.util.Arrays;

import static com.akiban.sql.optimizer.rule.range.TUtils.constant;
import static com.akiban.sql.optimizer.rule.range.TUtils.exclusive;
import static com.akiban.sql.optimizer.rule.range.TUtils.inclusive;
import static com.akiban.sql.optimizer.rule.range.TUtils.segment;
import static org.junit.Assert.assertEquals;

public final class RangeEndpointFactoryTest {
    @Test
    public void nameLtJoe() {
        check(  Comparison.LT,
                constant("Joe"),
                segment(RangeEndpoint.NULL_EXCLUSIVE, exclusive("Joe"))
        );
    }

    @Test
    public void nameLeJoe() {
        check(  Comparison.LE,
                constant("Joe"),
                segment(RangeEndpoint.NULL_EXCLUSIVE, inclusive("Joe"))
        );
    }

    @Test
    public void nameGtJoe() {
        check(  Comparison.GT,
                constant("Joe"),
                segment(exclusive("Joe"), RangeEndpoint.UPPER_WILD)
        );
    }

    @Test
    public void nameGeJoe() {
        check(  Comparison.GE,
                constant("Joe"),
                segment(inclusive("Joe"), RangeEndpoint.UPPER_WILD)
        );
    }

    @Test
    public void nameEqJoe() {
        check(  Comparison.EQ,
                constant("Joe"),
                segment(inclusive("Joe"), inclusive("Joe"))
        );
    }

    @Test
    public void nameNeJoe() {
        check(  Comparison.NE,
                constant("Joe"),
                segment(RangeEndpoint.NULL_EXCLUSIVE, exclusive("Joe")),
                segment(exclusive("Joe"), RangeEndpoint.UPPER_WILD)
        );
    }

    private void check(Comparison comparison, ConstantExpression value, RangeSegment... expected) {
        assertEquals(Arrays.asList(expected), RangeSegment.fromComparison(comparison, value));
    }
}
