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

import com.akiban.server.error.NoSuchFunctionException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.t3expressions.OverloadResolver.OverloadException;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TAggregatorBase;
import com.akiban.server.types3.TBundleID;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TCastBase;
import com.akiban.server.types3.TCastIdentifier;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.common.types.NoAttrTClass;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.Constantness;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import com.akiban.server.types3.texpressions.TValidatedOverload;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class OverloadResolverTest {
    private static class SimpleRegistry implements T3RegistryService {
        private final Map<String,List<TValidatedOverload>> validatedMap = new HashMap<String, List<TValidatedOverload>>();
        private final Map<TOverload,TValidatedOverload> originalMap = new HashMap<TOverload, TValidatedOverload>();
        private final Map<TClass, Map<TClass, TCast>> castMap = new HashMap<TClass, Map<TClass, TCast>>();
        private final Set<TCastIdentifier> strongCasts = new HashSet<TCastIdentifier>();


        public SimpleRegistry(TOverload... overloads) {
            for(TOverload overload : overloads) {
                TValidatedOverload validated = new TValidatedOverload(overload);
                originalMap.put(overload, validated);
                List<TValidatedOverload> list = validatedMap.get(overload.displayName());
                if(list == null) {
                    list = new ArrayList<TValidatedOverload>();
                    validatedMap.put(overload.displayName(), list);
                }
                list.add(validated);
            }
        }

        public void setCasts(TCast... casts) {
            castMap.clear();
            strongCasts.clear();
            for(TCast cast : casts) {
                Map<TClass,TCast> map = castMap.get(cast.sourceClass());
                if(map == null) {
                    map = new HashMap<TClass, TCast>();
                    castMap.put(cast.sourceClass(), map);
                }
                map.put(cast.targetClass(), cast);
                if (cast instanceof TestCastBase) {
                    TestCastBase tcb = (TestCastBase) cast;
                    if (tcb.isStrong)
                        strongCasts.add(new TCastIdentifier(cast));
                }
            }
        }

        @Override
        public List<TValidatedOverload> getOverloads(String name) {
            return validatedMap.get(name.toLowerCase());
        }

        @Override
        public TCast cast(TClass source, TClass target) {
            Map<TClass,TCast> map = castMap.get(source);
            if(map != null) {
                return map.get(target);
            }
            return null;
        }

        @Override
        public Set<TClass> stronglyCastableTo(TClass tClass) {
            Map<TClass, TCast> map = T3RegistryServiceImpl.createStrongCastsMap(castMap, strongCasts).get(tClass);
            Set<TClass> results = (map == null)
                    ? new HashSet<TClass>(1)
                    : new HashSet<TClass>(map.keySet());
            results.add(tClass);
            return results;
        }

        @Override
        public boolean isStrong(TCast cast) {
            TClass source = cast.sourceClass();
            TClass target = cast.targetClass();
            return stronglyCastableTo(target).contains(source);
        }

        @Override
        public Collection<? extends TAggregatorBase> getAggregates(String name) {
            throw new UnsupportedOperationException();
        }

        public TValidatedOverload validated(TOverload overload) {
            return originalMap.get(overload);
        }
    }

    private static class TestClassBase extends NoAttrTClass {
        private static final TBundleID TEST_BUNDLE_ID = new TBundleID("test", new UUID(0,0));

        public TestClassBase(String name, PUnderlying pUnderlying) {
            super(TEST_BUNDLE_ID, name, null, null, 1, 1, 1, pUnderlying, null, null);
        }
    }

    private static class TestCastBase extends TCastBase {

        private final boolean isStrong;

        public TestCastBase(TClass sourceAndTarget) {
            this(sourceAndTarget, sourceAndTarget, true);
        }

        public TestCastBase(TClass source, TClass target, boolean isAutomatic) {
            super(source, target, Constantness.UNKNOWN);
            this.isStrong = isAutomatic;
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            throw new UnsupportedOperationException();
        }
    }


    private static final String MUL_NAME = "*";
    private static class TestMulBase extends TOverloadBase {
        private final TClass tLeft;
        private final TClass tRight;
        private final TClass tTarget;

        public TestMulBase(TClass tClass) {
            this(tClass, tClass, tClass);
        }

        public TestMulBase(TClass tLeft, TClass tRight, TClass tTarget) {
            this.tLeft = tLeft;
            this.tRight = tRight;
            this.tTarget = tTarget;
        }

        @Override
        protected void buildInputSets(TInputSetBuilder builder) {
            if (tLeft == tRight) {
                builder.covers(tLeft, 0, 1);
            } else {
                builder.covers(tLeft, 0);
                builder.covers(tRight, 1);
            }
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String displayName() {
            return MUL_NAME;
        }

        @Override
        public TOverloadResult resultType() {
            return TOverloadResult.fixed(tTarget.instance());
        }
    }

    private static class TestGetBase extends TOverloadBase {
        private final String name;
        private final TClass tResult;
        private final TInputSetBuilder builder = new TInputSetBuilder();

        public TestGetBase(String name, TClass tResult) {
            this.name = name;
            this.tResult = tResult;
        }

        public TInputSetBuilder builder() {
            return builder;
        }

        @Override
        protected void buildInputSets(TInputSetBuilder builder) {
            builder.reset(this.builder);
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String displayName() {
            return name;
        }

        @Override
        public TOverloadResult resultType() {
            return (tResult == null) ? TOverloadResult.picking() : TOverloadResult.fixed(tResult.instance());
        }
    }

    private final static TClass TINT = new TestClassBase("int", PUnderlying.INT_32);
    private final static TClass TBIGINT = new TestClassBase("bigint", PUnderlying.INT_64);
    private final static TClass TDATE = new TestClassBase("date",  PUnderlying.DOUBLE);
    private final static TClass TVARCHAR = new TestClassBase("varchar",  PUnderlying.BYTES);

    private final static TCast C_INT_INT = new TestCastBase(TINT);
    private final static TCast C_INT_BIGINT = new TestCastBase(TINT, TBIGINT, true);
    private final static TCast C_BIGINT_BIGINT = new TestCastBase(TBIGINT);
    private final static TCast C_BIGINT_INT = new TestCastBase(TBIGINT, TINT, false);
    private final static TCast C_DATE_DATE = new TestCastBase(TDATE, TDATE, true);

    private final static TestMulBase MUL_INTS = new TestMulBase(TINT);
    private final static TestMulBase MUL_BIGINTS = new TestMulBase(TBIGINT);
    private final static TestMulBase MUL_DATE_INT = new TestMulBase(TDATE, TINT, TDATE);


    private SimpleRegistry registry;
    private OverloadResolver resolver;

    private void init(TOverload... overloads) {
        registry = new SimpleRegistry(overloads);
        resolver = new OverloadResolver(registry);
    }


    private static TPreptimeValue prepVal(TClass tClass) {
        return (tClass != null) ? new TPreptimeValue(tClass.instance()) : new TPreptimeValue();
    }

    private static List<TPreptimeValue> prepVals(TClass... tClasses) {
        TPreptimeValue[] prepVals = new TPreptimeValue[tClasses.length];
        for(int i = 0; i < tClasses.length; ++i) {
            prepVals[i] = prepVal(tClasses[i]);
        }
        return Arrays.asList(prepVals);
    }

    private void checkResolved(String msg, TOverload expected, String overloadName, List<TPreptimeValue> prepValues) {
        TValidatedOverload validated = expected != null ? registry.validated(expected) : null;
        // result.getPickingClass() not checked, SimpleRegistry doesn't implement commonTypes()
        OverloadResolver.OverloadResult result = resolver.get(overloadName, prepValues);
        assertSame(msg, validated, result != null ? result.getOverload() : null);
    }


    @Test(expected=NoSuchFunctionException.class)
    public void noSuchOverload() {
        init();
        resolver.get("foo", Arrays.asList(prepVal(TINT)));
    }

    @Test(expected=WrongExpressionArityException.class)
    public void knownOverloadTooFewParams() {
        init(MUL_INTS);
        resolver.get(MUL_NAME, prepVals(TINT));
    }

    @Test(expected=WrongExpressionArityException.class)
    public void knownOverloadTooManyParams() {
        init(MUL_INTS);
        resolver.get(MUL_NAME, prepVals(TINT, TINT, TINT));
    }

    // default resolution, exact match
    @Test
    public void mulIntWithInts() {
        init(MUL_INTS);
        checkResolved("INT*INT", MUL_INTS, MUL_NAME, prepVals(TINT, TINT));
    }

    // default resolution, types don't match (no registered casts, int -> bigint)
    @Test
    public void mulBigIntWithInts() {
        init(MUL_BIGINTS);
        checkResolved("INT*INT", MUL_BIGINTS, MUL_NAME, prepVals(TINT, TINT));
    }

    // default resolution, requires weak cast (bigint -> int)
    @Test
    public void mulIntWithBigInts() {
        init(MUL_INTS);
        checkResolved("INT*INT", MUL_INTS, MUL_NAME, prepVals(TINT, TINT));
    }

    // input resolution, no casts
    @Test(expected = OverloadException.class)
    public void mulIntMulBigIntWithIntsNoCasts() {
        init(MUL_INTS, MUL_BIGINTS);
        checkResolved("INT*INT", null, MUL_NAME, prepVals(TINT, TINT));
    }

    // input resolution, type only casts, only one candidate
    @Test
    public void mulIntMulBigIntWithIntsTypeOnlyCasts() {
        init(MUL_INTS, MUL_BIGINTS);
        registry.setCasts(C_INT_INT, C_BIGINT_BIGINT);
        checkResolved("INT*INT", MUL_INTS, MUL_NAME, prepVals(TINT, TINT));
        checkResolved("BIGINT*BIGINT", MUL_BIGINTS, MUL_NAME, prepVals(TBIGINT, TBIGINT));
    }

    // input resolution, more casts, 2 candidates but 1 more specific
    @Test
    public void mulIntMulBigIntWithIntsAndIntBigintStrongAndWeakCasts() {
        init(MUL_INTS, MUL_BIGINTS);
        registry.setCasts(C_INT_INT, C_INT_BIGINT,
                          C_BIGINT_BIGINT, C_BIGINT_INT);
        // 2 candidates, 1 more specific
        checkResolved("INT*INT", MUL_INTS, MUL_NAME, prepVals(TINT, TINT));
    }

    @Test
    public void specMulExampleIntBigIntAndDateCombos() {
        init(MUL_INTS, MUL_BIGINTS, MUL_DATE_INT);
        registry.setCasts(C_INT_INT, C_INT_BIGINT,
                          C_BIGINT_BIGINT, C_BIGINT_INT,
                          C_DATE_DATE);
        // 2 survive filtering, 1 more specific
        checkResolved("INT*INT", MUL_INTS, MUL_NAME, prepVals(TINT, TINT));
        // 1 survives filtering (bigint can't be strongly cast to INT or DATE)
        checkResolved("BIGINT*BIGINT", MUL_BIGINTS, MUL_NAME, prepVals(TBIGINT, TBIGINT));
        // 1 survives filtering (INT strongly cast to BIGINT)
        checkResolved("BIGINT*INT", MUL_BIGINTS, MUL_NAME, prepVals(TBIGINT, TINT));
        // 1 survives filtering
        checkResolved("DATE*INT", MUL_DATE_INT, MUL_NAME, prepVals(TDATE, TINT));
        try {
            // 3 survive filtering, 1 less specific, 2 candidates
            checkResolved("?*INT", null, MUL_NAME, prepVals(null, TINT));
            fail("expected OverloadException");
        } catch (OverloadException e) {
            // expected
        }
    }

    @Test(expected = OverloadException.class)
    public void conflictingOverloads() {
        final String NAME = "foo";
        // Overloads aren't valid and should(?) be rejected by real registry,
        // but make sure resolver doesn't choke
        TestGetBase posPos = new TestGetBase(NAME, TINT);
        posPos.builder().covers(TINT, 0, 1);
        TestGetBase posRem = new TestGetBase(NAME, TINT);
        posRem.builder().covers(TINT, 0).vararg(TINT);

        init(posPos, posRem);
        registry.setCasts(C_INT_INT);

        checkResolved(NAME+"(INT,INT)", null, NAME, prepVals(TINT, TINT));
    }

    @Test
    public void noArg() {
        final String NAME = "foo";
        TestGetBase noArg = new TestGetBase(NAME, TINT);
        init(noArg);
        checkResolved(NAME+"()", noArg, NAME, prepVals());
    }

    @Test
    public void onePosAndRemainingWithPickingSet() {
        final String NAME = "coalesce";
        TestGetBase coalesce = new TestGetBase(NAME, TVARCHAR);
        coalesce.builder().pickingVararg(null, 0);
        init(coalesce);

        try {
            OverloadResolver.OverloadResult result = resolver.get(NAME, prepVals());
            fail("WrongArity expected but got: " + result);
        } catch(WrongExpressionArityException e) {
            // Expected
        }

        checkResolved(NAME+"(INT)", coalesce, NAME, prepVals(TINT));
        registry.setCasts(C_INT_BIGINT);
        checkResolved(NAME+"(null,INT,BIGINT)", coalesce, NAME, prepVals(null, TINT, TBIGINT));
        try {
            checkResolved(NAME+"(null,DATE,INT)", coalesce, NAME, prepVals(null, TDATE, TINT));
            fail("expected overload exception");
        } catch (OverloadException e) {
            // There is no common type between date and int
        }
    }

    @Test
    public void onlyPickingRemaining() {
        final String NAME = "first";
        TestGetBase first = new TestGetBase(NAME, null);
        first.builder.pickingVararg(null, 0);
        init(first);
        checkResolved(NAME+"(INT)", first, NAME, prepVals(TINT));
        registry.setCasts(C_INT_BIGINT);
        checkResolved(NAME+"(BIGINT,INT)", first, NAME, prepVals(TBIGINT,TINT));
        try {
            checkResolved(NAME+"()", first, NAME, prepVals());
            fail("expected overload exception");
        } catch (WrongExpressionArityException e) {
            // can't resolve overload if nargs is wrong
        }
        try {
            checkResolved(NAME+"(null)", first, NAME, Arrays.asList(prepVal(null)));
            fail("expected overload exception");
        } catch (OverloadException e) {
            // can't find picking type for first if it's null
        }
    }
}
