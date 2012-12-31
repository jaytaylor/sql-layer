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
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.util.ValueHolder;

public final class FirstAggregator implements Aggregator {

    @Aggregate("first")
    public static AggregatorFactory count(String name, AkType type) {
        return new InnerFactory(name, type);
    }

    // Aggregator interface

    @Override
    public void input(ValueSource input) {
        if (first) {
            holder.expectType(type);
            Converters.convert(input, holder);
            first = false;
        }
    }

    @Override
    public void output(ValueTarget output) {
        Converters.convert(holder, output);
        first = true;
    }

    @Override
    public ValueSource emptyValue() {
        return NullValueSource.only();
    }


    // use in this package

    FirstAggregator(AkType type) {
        this.type = type;
        holder = new ValueHolder();
        first = true;
    }

    // object state

    private final AkType type;
    private final ValueHolder holder;
    private boolean first;

    // nested class

    private static final class InnerFactory implements AggregatorFactory {
        
        @Override
        public Aggregator get(Object obj)
        {
            return get();
        }

        @Override
        public Aggregator get() {
            return new FirstAggregator(type);
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public AkType outputType() {
            return type;
        }

        @Override
        public String toString() {
            return name;
        }

        private InnerFactory(String name, AkType type) {
            this.name = name;
            this.type = type;
        }

        private final String name;
        private final AkType type;
    }
}
