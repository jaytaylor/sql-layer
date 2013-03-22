
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
