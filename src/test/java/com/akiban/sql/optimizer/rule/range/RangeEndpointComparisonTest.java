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

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;

import static com.akiban.sql.optimizer.rule.range.ComparisonResult.*;
import static com.akiban.sql.optimizer.rule.range.TUtils.exclusive;
import static com.akiban.sql.optimizer.rule.range.TUtils.inclusive;
import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class RangeEndpointComparisonTest {
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder pb = new ParameterizationBuilder();

        // nulls vs nulls
        param(pb,  RangeEndpoint.NULL_INCLUSIVE, EQ,  RangeEndpoint.NULL_INCLUSIVE);
        param(pb,  RangeEndpoint.NULL_INCLUSIVE, LT_BARELY,  RangeEndpoint.NULL_EXCLUSIVE);
        param(pb,  RangeEndpoint.NULL_EXCLUSIVE, EQ,  RangeEndpoint.NULL_EXCLUSIVE);

        // nulls vs "normal" values
        param(pb,  RangeEndpoint.NULL_INCLUSIVE, LT, inclusive(AARDVARK));
        param(pb,  RangeEndpoint.NULL_INCLUSIVE, LT, exclusive(AARDVARK));
        param(pb,  RangeEndpoint.NULL_EXCLUSIVE, LT, inclusive(AARDVARK));
        param(pb,  RangeEndpoint.NULL_EXCLUSIVE, LT, exclusive(AARDVARK));

        // nulls vs wild
        param(pb,  RangeEndpoint.NULL_INCLUSIVE, LT, RangeEndpoint.UPPER_WILD);
        param(pb,  RangeEndpoint.NULL_EXCLUSIVE, LT, RangeEndpoint.UPPER_WILD);

        // normal values vs same values
        param(pb, inclusive(AARDVARK), EQ, inclusive(AARDVARK));
        param(pb, inclusive(AARDVARK), LT_BARELY, exclusive(AARDVARK));

        // normal values vs comparable values
        param(pb, inclusive(AARDVARK), LT, inclusive(CAT));
        param(pb, inclusive(AARDVARK), LT, exclusive(CAT));
        param(pb, exclusive(AARDVARK), LT, inclusive(CAT));
        param(pb, exclusive(AARDVARK), LT, exclusive(CAT));

        // normal values vs wild
        param(pb, inclusive(AARDVARK), LT, RangeEndpoint.UPPER_WILD);
        param(pb, exclusive(AARDVARK), LT, RangeEndpoint.UPPER_WILD);

        // wild vs wild
        param(pb, RangeEndpoint.UPPER_WILD, EQ, RangeEndpoint.UPPER_WILD);

        // incomparable types
        param(pb, inclusive(TWO), INVALID, inclusive(AARDVARK));
        param(pb, inclusive(TWO), INVALID, exclusive(AARDVARK));
        param(pb, exclusive(TWO), INVALID, inclusive(AARDVARK));
        param(pb, exclusive(TWO), INVALID, exclusive(AARDVARK));

        return pb.asList();
    }

    private static void param(ParameterizationBuilder pb,
                              RangeEndpoint one, ComparisonResult expected, RangeEndpoint two)
    {
        String name = one + " " + expected.describe() + " " + two;
        pb.add(name, one, two, expected);
        // test reflectivity
        final ComparisonResult flippedExpected;
        switch (expected) {
        case LT:        flippedExpected = GT;           break;
        case LT_BARELY: flippedExpected = GT_BARELY;    break;
        case GT:        flippedExpected = LT;           break;
        case GT_BARELY: flippedExpected = LT_BARELY;    break;
        default:        flippedExpected = expected;     break;
        }
        String flippedName = two + " " + flippedExpected.describe() + " " + one;
        if (!flippedName.equals(name)) { // e.g. we don't need to reflect inclusive("A") == inclusive("A")
            pb.add(flippedName, two, one, flippedExpected);
        }
    }

    private static String AARDVARK = "aardvark";
    private static String CAT = "cat";
    private static long TWO = 2;

    @Test
    public void compare() {
        assertEquals(expected, one.comparePreciselyTo(two));
    }

    public RangeEndpointComparisonTest(RangeEndpoint one, RangeEndpoint two, ComparisonResult expected) {
        this.one = one;
        this.two = two;
        this.expected = expected;
    }

    private final RangeEndpoint one;
    private final RangeEndpoint two;
    private final ComparisonResult expected;
}
