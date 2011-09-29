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
import com.akiban.server.error.NoSuchFunctionException;
import com.akiban.server.types.AkType;

import java.util.EnumMap;
import java.util.Map;

final class LongAggregators {

    public static AggregatorFactory min(AkType type) {
        return get(readOnlyMins, type);
    }

    public static AggregatorFactory max(AkType type) {
        return get(readOnlyMaxes, type);
    }

    private LongAggregators() {}

    private static AggregatorFactory get(Map<AkType,AggregatorFactory> factories, AkType type) {
        AggregatorFactory result = factories.get(type);
        if (result == null)
            throw new NoSuchFunctionException("min");
        return result;
    }

    private static final Map<AkType,AggregatorFactory> readOnlyMins = createLongAggregator(
            new LongAggregator.LongProcessor() {
                @Override
                public long process(long oldState, long input) {
                    return Math.min(oldState, input);
                }

                @Override
                public String name() {
                    return "MIN";
                }
            }
    );

    private static final Map<AkType,AggregatorFactory> readOnlyMaxes = createLongAggregator(
            new LongAggregator.LongProcessor() {
                @Override
                public long process(long oldState, long input) {
                    return Math.max(oldState, input);
                }

                @Override
                public String name() {
                    return "MAX";
                }
            }
    );

    private static Map<AkType,AggregatorFactory> createLongAggregator(final LongAggregator.LongProcessor processor) {
        Map<AkType,AggregatorFactory> result = new EnumMap<AkType, AggregatorFactory>(AkType.class);
        for (final AkType type : AkType.values()) {
            if (type.underlyingTypeOrNull() == AkType.UnderlyingType.LONG_AKTYPE) {
                result.put(type, new AggregatorFactory() {
                    @Override
                    public Aggregator get() {
                        return new LongAggregator(processor, type);
                    }

                    @Override
                    public String toString() {
                        return processor.name();
                    }
                });
            }
        }
        return result;
    }
}
