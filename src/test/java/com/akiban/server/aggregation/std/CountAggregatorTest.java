
package com.akiban.server.aggregation.std;

import com.akiban.server.types.AkType;
import com.akiban.server.types.util.ValueHolder;
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
