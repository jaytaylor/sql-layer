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

package com.foundationdb.server.aggregation.std;

import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.util.ValueHolder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class CountAggregatorTest {
    @Test
    public void countStarWithNulls() {
        CountAggregator aggregator = new CountAggregator(true);
        check(aggregator, 4, "alpha", "bravo", null, "charlie");
        check(aggregator, 2, "delta", null);
    }
    
    @Test
    public void countNotNullWithNulls() {
        CountAggregator aggregator = new CountAggregator(false);
        check(aggregator, 3, "alpha", "bravo", null, "charlie");
        check(aggregator, 1, "delta", null);
    }

    @Test
    public void outputConversion() {
        assertEquals("3", aggregate(new CountAggregator(false), AkType.VARCHAR, "a", "b", "c").getString());
    }

    private void check(CountAggregator aggregator, long expected, String... inputs) {
        assertEquals(expected, aggregate(aggregator, AkType.LONG, inputs).getLong());
    }

    private ValueHolder aggregate(CountAggregator aggregator, AkType outType, String... inputs) {
        ValueHolder holder = new ValueHolder();
        for (String input : inputs) {
            holder.putString(input);
            aggregator.input(holder);
        }
        holder.expectType(outType);
        aggregator.output(holder);
        return holder;
    }
}
