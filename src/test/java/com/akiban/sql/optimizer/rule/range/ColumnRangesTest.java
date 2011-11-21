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

package com.akiban.sql.optimizer.rule.range;

import com.akiban.server.expression.std.Comparison;
import com.akiban.sql.optimizer.plan.ConditionExpression;
import com.akiban.sql.optimizer.plan.ConstantExpression;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.akiban.sql.optimizer.rule.range.TUtils.compare;
import static com.akiban.sql.optimizer.rule.range.TUtils.constant;
import static com.akiban.sql.optimizer.rule.range.TUtils.exclusive;
import static com.akiban.sql.optimizer.rule.range.TUtils.firstName;
import static com.akiban.sql.optimizer.rule.range.TUtils.isNull;
import static com.akiban.sql.optimizer.rule.range.TUtils.segment;
import static com.akiban.sql.optimizer.rule.range.TUtils.sin;
import static org.junit.Assert.assertEquals;

public final class ColumnRangesTest {

    @Test
    public void columnComparison() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(firstName, Comparison.LT, value);
        ColumnRanges expected = new ColumnRanges(
                firstName,
                Collections.singleton(compare),
                Arrays.asList(segment(RangeEndpoint.NULL_EXCLUSIVE, exclusive("joe")))
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void columnIsNull() {
        ConditionExpression isNull = isNull(firstName);
        ColumnRanges expected = new ColumnRanges(
                firstName,
                Collections.singleton(isNull),
                Arrays.asList(segment(RangeEndpoint.NULL_INCLUSIVE, RangeEndpoint.NULL_INCLUSIVE))
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(isNull));
    }
    @Test
    public void sinOfColumn() {
        ConditionExpression isNull = sin(firstName);
        ColumnRanges expected = null;
        assertEquals(expected, ColumnRanges.rangeAtNode(isNull));
    }

    @Test
    public void tbd() {
        throw new RuntimeException("need testing!");
    }
}
