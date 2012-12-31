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
