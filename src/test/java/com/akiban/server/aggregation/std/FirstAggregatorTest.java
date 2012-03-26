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

package com.akiban.server.aggregation.std;

import com.akiban.server.types.AkType;
import com.akiban.server.types.util.ValueHolder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class FirstAggregatorTest {

    @Test
    public void test() {
        FirstAggregator aggregator = new FirstAggregator(AkType.LONG);
        check(aggregator, 1, 1, 2, 3);
    }

    private void check(FirstAggregator aggregator, long expected, long... inputs) {
        assertEquals(expected, aggregate(aggregator, AkType.LONG, inputs).getLong());
    }

    private ValueHolder aggregate(FirstAggregator aggregator, AkType outType, long... inputs) {
        ValueHolder holder = new ValueHolder();
        for (long input : inputs) {
            holder.putLong(input);
            aggregator.input(holder);
        }
        holder.expectType(outType);
        aggregator.output(holder);
        return holder;
    }
}
