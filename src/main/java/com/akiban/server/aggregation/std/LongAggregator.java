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

import com.akiban.server.aggregation.Aggregator;
import com.akiban.server.aggregation.AggregatorFactory;
import com.akiban.server.service.functions.Aggregate;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.LongExtractor;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.util.ArgumentValidation;

@SuppressWarnings("unused")
public final class LongAggregator implements Aggregator {

    @Aggregate("min")
    public static AggregatorFactory mins(final String name, final AkType type) {
        return new AggregatorFactory() {
            @Override
            public Aggregator get() {
                return new LongAggregator(minProcessor, type);
            }

            @Override
            public AkType outputType() {
                return type;
            }

            @Override
            public String toString() {
                return name;
            }
        };
    }

    @Aggregate("max")
    public static AggregatorFactory maxes(final String name, final AkType type) {
        return new AggregatorFactory() {
            @Override
            public Aggregator get() {
                return new LongAggregator(maxProcessor, type);
            }

            @Override
            public AkType outputType() {
                return type;
            }

            @Override
            public String toString() {
                return name;
            }
        };
    }

    @Override
    public void input(ValueSource input) {
        value =  processor.process(value, extractor.getLong(input));
        sawAny = true;
    }

    @Override
    public void output(ValueTarget output) {
        assert sawAny : "didn't see any input rows!";
        Converters.convert(new ValueHolder(type, value), output);
        this.value = processor.initialValue();
    }

    @Override
    public ValueSource emptyValue() {
        return NullValueSource.only();
    }

    protected LongAggregator(LongProcessor processor, AkType type) {
        this.type = type;
        if (type.underlyingType() != AkType.LONG.underlyingType()) {
            throw new IllegalArgumentException("type must have an underlying long: " + type);
        }
        this.extractor = Extractors.getLongExtractor(this.type);
        this.processor = processor;
        this.value = this.processor.initialValue();
        ArgumentValidation.notNull("processor", this.processor);
    }

    private final AkType type;
    private final LongExtractor extractor;
    private final LongProcessor processor;
    private long value;
    private boolean sawAny = false;

    // class state

    private static LongProcessor minProcessor = new LongProcessor() {
        @Override
        public long process(long oldState, long input) {
            return Math.min(oldState, input);
        }

        @Override
        public long initialValue() {
            return Long.MAX_VALUE;
        }
    };

    private static LongProcessor maxProcessor = new LongProcessor() {
        @Override
        public long process(long oldState, long input) {
            return Math.max(oldState, input);
        }

        @Override
        public long initialValue() {
            return Long.MIN_VALUE;
        }
    };


    // subclasses
    private interface LongProcessor {
        long initialValue();
        long process(long oldState, long input);
    }
}
