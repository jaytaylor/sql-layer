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

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public final class FunctionsRegistryTest {
    
    @Test
    public void findAggregatorFactory() {
        FunctionsRegistry registry = registry(MockFinderTargets.GoodForAggregators.class);
        assertEquals(MockFinderTargets.GoodForAggregators.expectedAggregatorFactories(), registry.getAllAggregators());
    }

    @Test
    public void findExpressionComposer() {
        FunctionsRegistry registry = registry(MockFinderTargets.GoodForAggregators.class);
        assertEquals(MockFinderTargets.GoodForAggregators.expectedExpressionFactories(), registry.getAllComposers());
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
}
