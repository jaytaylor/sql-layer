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
import com.akiban.server.types3.texpressions.TValidatedOverload;
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

            Iterable<? extends ScalarsGroup<TValidatedOverload>> scalarsByPriority = registry.getOverloads(FUNC_NAME);
            List<Set<TInputSet>> actuals = new ArrayList<Set<TInputSet>>();
            for (ScalarsGroup<TValidatedOverload> scalarsGroup : scalarsByPriority) {
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
