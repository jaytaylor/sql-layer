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

package com.akiban.server.aggregation.std;

import com.akiban.server.aggregation.Aggregator;
import com.akiban.server.aggregation.AggregatorFactory;
import com.akiban.server.service.functions.Aggregate;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.util.ValueHolder;

public final class CountAggregator implements Aggregator {

    @Aggregate("count")
    public static AggregatorFactory count(final String name, final AkType type) {
        return new InnerFactory(name, false);
    }

    @Aggregate("count(*)")
    public static AggregatorFactory countStar(final String name, final AkType type) {
        return new InnerFactory(name, false);
    }

    // Aggregator interface

    @Override
    public void input(ValueSource input) {
        if (countStar || (!input.isNull())) {
            ++ count;
        }
    }

    @Override
    public void output(ValueTarget output) {
        holder.putLong(count);
        count = 0;
        Converters.convert(holder, output);
    }

    @Override
    public ValueSource emptyValue() {
        return EMPTY_VALUE;
    }


    // use in this package

    CountAggregator(boolean countStar) {
        this.countStar = countStar;
        this.holder = new ValueHolder();
    }

    // object state

    private final boolean countStar;
    private final ValueHolder holder;
    private long count;

    // class state

    private static final ValueSource EMPTY_VALUE = new ValueHolder(AkType.LONG, 0);

    // nested class

    private static final class InnerFactory implements AggregatorFactory {
        
        @Override
        public Aggregator get(Object obj)
        {
            return get();
        }

        @Override
        public Aggregator get() {
            return new CountAggregator(countStar);
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public AkType outputType() {
            return AkType.LONG;
        }

        @Override
        public String toString() {
            return name;
        }

        private InnerFactory(String name, boolean countStar) {
            this.countStar = countStar;
            this.name = name;
        }

        private final boolean countStar;
        private final String name;
    }
}
