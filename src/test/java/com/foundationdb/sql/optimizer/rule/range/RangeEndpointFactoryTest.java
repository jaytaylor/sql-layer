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
import org.junit.Test;

import java.util.Arrays;

import static com.foundationdb.sql.optimizer.rule.range.TUtils.constant;
import static com.foundationdb.sql.optimizer.rule.range.TUtils.exclusive;
import static com.foundationdb.sql.optimizer.rule.range.TUtils.inclusive;
import static com.foundationdb.sql.optimizer.rule.range.TUtils.nullExclusive;
import static com.foundationdb.sql.optimizer.rule.range.TUtils.segment;
import static org.junit.Assert.assertEquals;

public final class RangeEndpointFactoryTest {
    @Test
    public void nameLtJoe() {
        check(  Comparison.LT,
                constant("Joe"),
                segment(nullExclusive("Joe"), exclusive("Joe"))
        );
    }

    @Test
    public void nameLeJoe() {
        check(  Comparison.LE,
                constant("Joe"),
                segment(nullExclusive("Joe"), inclusive("Joe"))
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
                segment(nullExclusive("Joe"), exclusive("Joe")),
                segment(exclusive("Joe"), RangeEndpoint.UPPER_WILD)
        );
    }

    private void check(Comparison comparison, ConstantExpression value, RangeSegment... expected) {
        assertEquals(Arrays.asList(expected), RangeSegment.fromComparison(comparison, value));
    }
}
