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

package com.akiban.server.t3expressions;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.T3TestClass;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInputSet;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public final class T3ScalarsRegistryTest {

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
        T3RegistryServiceImpl registry = new T3RegistryServiceImpl();
        registry.start(new InstanceFinderBuilder());
        assertEquals("lookup for FOO", null, registry.getOverloads("foo"));
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
            TOverload result = new DummyOverload(FUNC_NAME, priorities);
            instanceFinder.put(TOverload.class, result);
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
            T3RegistryServiceImpl registry = new T3RegistryServiceImpl();
            registry.start(instanceFinder);

            Iterable<? extends ScalarsGroup> scalarsByPriority = registry.getOverloads(FUNC_NAME);
            List<Set<TInputSet>> actuals = new ArrayList<Set<TInputSet>>();
            for (ScalarsGroup scalarsGroup : scalarsByPriority) {
                Set<TInputSet> actualInputs = new HashSet<TInputSet>();
                for (TOverload overload : scalarsGroup.getOverloads()) {
                    TInputSet overloadInput = onlyInputSet(overload);
                    actualInputs.add(overloadInput);
                }
                actuals.add(actualInputs);
            }

            assertEquals("input sets not equal by identity", inputSetsByPriority, actuals);
        }

        TInputSet onlyInputSet(TOverload result) {
            TInputSet onlyInputSet = result.inputSets().get(0);
            assertEquals("input sets should have size 1", Arrays.asList(onlyInputSet), result.inputSets());
            return onlyInputSet;
        }

        private final InstanceFinderBuilder instanceFinder = new InstanceFinderBuilder();
        private final List<Set<TInputSet>> inputSetsByPriority = new ArrayList<Set<TInputSet>>();
        private boolean checked = false;

        private static final String FUNC_NAME = "foo";
    }

    private static class DummyOverload extends TOverloadBase {

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
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs,
                                  PValueTarget output) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String displayName() {
            return name;
        }

        @Override
        public TOverloadResult resultType() {
            return TOverloadResult.fixed(testClass.instance());
        }

        @Override
        public int[] getPriorities() {
            return priorities;
        }

        private DummyOverload(String name, int[] priorities) {
            this.name = name;
            this.priorities = priorities;
        }

        private final String name;
        private final int[] priorities;
        private List<TInputSet> inputSets; // base class will recreate this each time, which we don't want
    }

    private static final TClass testClass = new T3TestClass("A");
}
