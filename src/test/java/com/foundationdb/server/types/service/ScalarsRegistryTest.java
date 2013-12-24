/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.server.types.service;

import com.foundationdb.server.types.*;
import com.foundationdb.server.types.TypesTestClass;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.texpressions.TValidatedScalar;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class ScalarsRegistryTest {

    @Test
    public void singleOverload() {
        TInputSet a = test.createOverloadWithPriority(1);
        test.expectInputSets(a);
        test.run();
    }

    @Test
    public void twoOverloadsSamePriority() {
        TInputSet a = test.createOverloadWithPriority(1);
        TInputSet b = test.createOverloadWithPriority(1);
        test.expectInputSets(a, b);
        test.run();
    }

    @Test
    public void twoOverloadsSparsePriorities() {
        TInputSet a = test.createOverloadWithPriority(-9812374);
        TInputSet b = test.createOverloadWithPriority(1928734);
        test.expectInputSets(a);
        test.expectInputSets(b);
        test.run();
    }

    @Test
    public void overloadHasMultiplePriorities() {
        TInputSet a = test.createOverloadWithPriority(1, 2);
        TInputSet b = test.createOverloadWithPriority(1);
        TInputSet c = test.createOverloadWithPriority(2);
        test.expectInputSets(a, b);
        test.expectInputSets(a, c);
        test.run();
    }

    @Test
    public void noOverloads() {
        TypesRegistryServiceImpl registry = new TypesRegistryServiceImpl();
        registry.start(new InstanceFinderBuilder());
        List<TPreptimeValue> args = Collections.emptyList();
        assertEquals("lookup for FOO", null, registry.getScalarsResolver().getRegistry().get("foo"));
        test.noRunNeeded();
    }

    @After
    public void checkTester() {
        assertTrue("Tester wasn't used", test.checked);
    }

    private final Tester test = new Tester();

    private static class Tester {

        TInputSet createOverloadWithPriority(int priority, int... priorities) {
            priorities = Ints.concat(new int[] { priority }, priorities);
            TScalar result = new DummyScalar(FUNC_NAME, priorities);
            instanceFinder.put(TScalar.class, result);
            return onlyInputSet(result);
        }

        void expectInputSets(TInputSet priorityGroupInput, TInputSet... priorityGroupInputs) {
            priorityGroupInputs = ObjectArrays.concat(priorityGroupInputs, priorityGroupInput);
            Set<TInputSet> expectedInputs = Sets.newHashSet(priorityGroupInputs);
            inputSetsByPriority.add(expectedInputs);
        }

        void noRunNeeded() {
            assert inputSetsByPriority.isEmpty() : inputSetsByPriority;
            checked = true;
        }

        void run() {
            checked = true;
            TypesRegistryServiceImpl registry = new TypesRegistryServiceImpl();
            registry.start(instanceFinder);

            Iterable<? extends ScalarsGroup<TValidatedScalar>> scalarsByPriority
                    = registry.getScalarsResolver().getRegistry().get(FUNC_NAME);
            List<Set<TInputSet>> actuals = new ArrayList<>();
            for (ScalarsGroup<TValidatedScalar> scalarsGroup : scalarsByPriority) {
                Set<TInputSet> actualInputs = new HashSet<>();
                for (TScalar scalar : scalarsGroup.getOverloads()) {
                    TInputSet overloadInput = onlyInputSet(scalar);
                    actualInputs.add(overloadInput);
                }
                actuals.add(actualInputs);
            }

            assertEquals("input sets not equal by identity", inputSetsByPriority, actuals);
        }

        TInputSet onlyInputSet(TScalar result) {
            TInputSet onlyInputSet = result.inputSets().get(0);
            assertEquals("input sets should have size 1", Arrays.asList(onlyInputSet), result.inputSets());
            return onlyInputSet;
        }

        private final InstanceFinderBuilder instanceFinder = new InstanceFinderBuilder();
        private final List<Set<TInputSet>> inputSetsByPriority = new ArrayList<>();
        private boolean checked = false;

        private static final String FUNC_NAME = "foo";
    }

    private static class DummyScalar extends TScalarBase {

        @Override
        public List<TInputSet> inputSets() {
            if (inputSets == null)
                inputSets = super.inputSets();
            return inputSets;
        }

        @Override
        protected void buildInputSets(TInputSetBuilder builder) {
            builder.covers(null, 0);
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs,
                                  ValueTarget output) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String displayName() {
            return name;
        }

        @Override
        public TOverloadResult resultType() {
            return TOverloadResult.fixed(testClass);
        }

        @Override
        public int[] getPriorities() {
            return priorities;
        }

        private DummyScalar(String name, int[] priorities) {
            this.name = name;
            this.priorities = priorities;
        }

        private final String name;
        private final int[] priorities;
        private List<TInputSet> inputSets; // base class will recreate this each time, which we don't want
    }

    private static final TClass testClass = new TypesTestClass("A");
}
