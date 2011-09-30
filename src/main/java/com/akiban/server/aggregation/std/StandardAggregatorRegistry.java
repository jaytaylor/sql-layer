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
import com.akiban.server.aggregation.AggregatorRegistry;
import com.akiban.server.error.NoSuchFunctionException;
import com.akiban.server.types.AkType;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

public final class StandardAggregatorRegistry implements AggregatorRegistry {
    @Override
    public AggregatorFactory get(String name, AkType type) {
        Map<AkType,AggregatorFactory> byType = readOnlyFactories.get(name.toUpperCase());
        if (byType == null)
            throw new NoSuchFunctionException(name);
        AggregatorFactory result = byType.get(type);
        if (result == null)
            throw new NoSuchFunctionException(name);
        return result;
    }

    private final Map<String,Map<AkType,AggregatorFactory>> readOnlyFactories = createFactories();

    private static Map<String,Map<AkType,AggregatorFactory>> createFactories() {
        Map<String,Map<AkType,AggregatorFactory>> result = new HashMap<String, Map<AkType, AggregatorFactory>>();

        result.put("MIN", createLongAggregator("MIN", new LongAggregator.LongProcessor() {
            @Override
            public long process(long oldState, long input) {
                return Math.min(oldState, input);
            }
        }));

        result.put("MAX", createLongAggregator("MAX", new LongAggregator.LongProcessor() {
            @Override
            public long process(long oldState, long input) {
                return Math.min(oldState, input);
            }
        }));

        return result;
    }

    private static Map<AkType,AggregatorFactory> createLongAggregator(
            final String name,
            final LongAggregator.LongProcessor processor)
    {
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
                        return name;
                    }
                });
            }
        }
        return result;
    }
}
