/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

import com.foundationdb.server.aggregation.Aggregator;
import com.foundationdb.server.aggregation.AggregatorFactory;
import com.foundationdb.server.service.functions.Aggregate;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.ValueTarget;
import com.foundationdb.server.types.conversion.Converters;
import com.foundationdb.server.types.util.ValueHolder;

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
