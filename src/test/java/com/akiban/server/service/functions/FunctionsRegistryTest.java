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

package com.akiban.server.service.functions;

import com.akiban.server.aggregation.Aggregator;
import com.akiban.server.aggregation.AggregatorFactory;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public final class FunctionsRegistryTest {
    
    @Test
    public void findAggregatorFactory() {
        FunctionsRegistry registry = registry(Good.class);
        assertEquals(Good.expectedAggregatorFactories(), registry.getAllAggregators());
    }

    @Test
    public void findExpressionComposer() {
        FunctionsRegistry registry = registry(Good.class);
        assertEquals(Good.expectedExpressionFactories(), registry.getAllComposers());
    }

    // use in this class

    private static FunctionsRegistry registry(Class<?>... classes) {
        return new FunctionsRegistry(new InternalClassFinder(classes));
    }

    // nested classes

    private static class InternalClassFinder implements FunctionsClassFinder {
        private InternalClassFinder(Class<?>... classes) {
            this.classes = Arrays.asList(classes);
        }

        @Override
        public List<Class<?>> findClasses() {
            return classes;
        }

        private final List<Class<?>> classes;
    }

    public static class Good {

        @Aggregate("foo")
        public static AggregatorFactory get(AkType type) {
            return type == AkType.LONG
                    ? AGGREGATOR_FACTORY
                    : null;
        }

        @Scalar("foo")
        public static final ExpressionComposer EXPRESSION_COMPOSER = new ExpressionComposer() {
            @Override
            public Expression compose(List<? extends Expression> arguments) {
                throw new UnsupportedOperationException();
            }
        };

        static Map<String, Map<AkType, AggregatorFactory>> expectedAggregatorFactories() {
            Map<String,Map<AkType,AggregatorFactory>> expected = new HashMap<String, Map<AkType, AggregatorFactory>>();
            Map<AkType,AggregatorFactory> expectedInner = new EnumMap<AkType, AggregatorFactory>(AkType.class);
            expectedInner.put(AkType.LONG, AGGREGATOR_FACTORY);
            expected.put("foo", expectedInner);
            return expected;
        }

        static Map<String,ExpressionComposer> expectedExpressionFactories() {
            return Collections.singletonMap("foo", EXPRESSION_COMPOSER);
        }

        private static final AggregatorFactory AGGREGATOR_FACTORY = new AggregatorFactory() {
            @Override
            public Aggregator get() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
