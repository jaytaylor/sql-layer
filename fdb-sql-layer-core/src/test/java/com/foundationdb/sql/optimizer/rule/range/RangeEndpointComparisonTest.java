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

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;

import static com.foundationdb.sql.optimizer.rule.range.ComparisonResult.*;
import static com.foundationdb.sql.optimizer.rule.range.TUtils.exclusive;
import static com.foundationdb.sql.optimizer.rule.range.TUtils.inclusive;
import static com.foundationdb.sql.optimizer.rule.range.TUtils.nullExclusive;
import static com.foundationdb.sql.optimizer.rule.range.TUtils.nullInclusive;
import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class RangeEndpointComparisonTest {
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder pb = new ParameterizationBuilder();

        // nulls vs nulls
        param(pb,  nullInclusive(AARDVARK), EQ,  nullInclusive(AARDVARK));
        param(pb,  nullInclusive(AARDVARK), LT_BARELY,  nullExclusive(AARDVARK));
        param(pb,  nullExclusive(AARDVARK), EQ,  nullExclusive(AARDVARK));

        // nulls vs "normal" values
        param(pb,  nullInclusive(AARDVARK), LT, inclusive(AARDVARK));
        param(pb,  nullInclusive(AARDVARK), LT, exclusive(AARDVARK));
        param(pb,  nullExclusive(AARDVARK), LT, inclusive(AARDVARK));
        param(pb,  nullExclusive(AARDVARK), LT, exclusive(AARDVARK));

        // nulls vs wild
        param(pb,  nullInclusive(AARDVARK), LT, RangeEndpoint.UPPER_WILD);
        param(pb,  nullExclusive(AARDVARK), LT, RangeEndpoint.UPPER_WILD);

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
