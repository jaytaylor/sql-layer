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

import com.akiban.server.error.NoSuchFunctionException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TBundleID;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TCastBase;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeContext;
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
import sun.security.util.BigInt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertSame;

public class OverloadResolverTest {
    private static class SimpleRegistry implements T3ScalarsRegistry {
        private final Map<String,List<TValidatedOverload>> validatedMap = new HashMap<String, List<TValidatedOverload>>();
        private final Map<TOverload,TValidatedOverload> originalMap = new HashMap<TOverload, TValidatedOverload>();
        private final Map<TClass, Map<TClass, TCast>> castMap = new HashMap<TClass, Map<TClass, TCast>>();


        public SimpleRegistry(TOverload... overloads) {
            for(TOverload overload : overloads) {
                TValidatedOverload validated = new TValidatedOverload(overload);
                originalMap.put(overload, validated);
                List<TValidatedOverload> list = validatedMap.get(overload.overloadName());
                if(list == null) {
                    list = new ArrayList<TValidatedOverload>();
                    validatedMap.put(overload.overloadName(), list);
                }
                list.add(validated);
            }
        }

        public void setCasts(TCast... casts) {
            castMap.clear();
            for(TCast cast : casts) {
                Map<TClass,TCast> map = castMap.get(cast.sourceClass());
                if(map == null) {
                    map = new HashMap<TClass, TCast>();
                    castMap.put(cast.sourceClass(), map);
                }
                map.put(cast.targetClass(), cast);
            }
        }

        @Override
        public List<TValidatedOverload> getOverloads(String name) {
            return validatedMap.get(name);
        }

        @Override
        public OverloadResolutionResult get(String name, List<? extends TClass> inputClasses) {
            throw new UnsupportedOperationException();
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
        public TClassPossibility commonTClass(TClass one, TClass two) {
            throw new UnsupportedOperationException();
        }

        public TValidatedOverload validated(TOverload overload) {
            return originalMap.get(overload);
        }
    }

    private static class TestClassBase extends NoAttrTClass {
        private static final TBundleID TEST_BUNDLE_ID = new TBundleID("test", new UUID(0,0));

        public TestClassBase(String name, PUnderlying pUnderlying) {
            super(TEST_BUNDLE_ID, name, 1, 1, 1, pUnderlying);
        }
    }

    private static class TestCastBase extends TCastBase {
        public TestCastBase(TClass sourceAndTarget) {
            this(sourceAndTarget, sourceAndTarget, true);
        }

        public TestCastBase(TClass source, TClass target, boolean isAutomatic) {
            super(source, target, isAutomatic, Constantness.UNKNOWN);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            throw new UnsupportedOperationException();
        }
    }


    private static final String MUL_NAME = "*";
    private static class TestMulBase extends TOverloadBase {
        private final TClass tClass;

        public TestMulBase(TClass tClass) {
            this.tClass = tClass;
        }

        @Override
        protected void buildInputSets(TInputSetBuilder builder) {
            builder.covers(tClass, 0, 1);
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String overloadName() {
            return MUL_NAME;
        }

        @Override
        public TOverloadResult resultType() {
            return TOverloadResult.fixed(tClass.instance());
        }
    }

    private final static TClass TINT = new TestClassBase("int", PUnderlying.INT_32);
    private final static TClass TBIGINT = new TestClassBase("bigint", PUnderlying.INT_64);
    private final static TClass TINTERVAL = new TestClassBase("interval", PUnderlying.INT_32);
    private final static TClass TDOUBLE = new TestClassBase("double",  PUnderlying.DOUBLE);

    private final static TCast C_INT_INT = new TestCastBase(TINT);
    private final static TCast C_INT_BIGINT = new TestCastBase(TINT, TBIGINT, true);
    private final static TCast C_BIGINT_BIGINT = new TestCastBase(TBIGINT);
    private final static TCast C_BIGINT_INT = new TestCastBase(TBIGINT, TINT, false);

    private final static TestMulBase MUL_INT = new TestMulBase(TINT);
    private final static TestMulBase MUL_BIGINT = new TestMulBase(TBIGINT);
    private final static TestMulBase MUL_DOUBLE = new TestMulBase(TDOUBLE);


    private static TPreptimeValue prepVal(TClass tClass) {
        return new TPreptimeValue(tClass.instance());
    }

    private static List<TPreptimeValue> prepVals(TClass... tClasses) {
        TPreptimeValue[] prepVals = new TPreptimeValue[tClasses.length];
        for(int i = 0; i < tClasses.length; ++i) {
            prepVals[i] = prepVal(tClasses[i]);
        }
        return Arrays.asList(prepVals);
    }

    @Test(expected=NoSuchFunctionException.class)
    public void noSuchOverload() {
        SimpleRegistry registry = new SimpleRegistry();
        OverloadResolver resolver = new OverloadResolver(registry);
        resolver.get("foo", Arrays.asList(prepVal(TINT)));
    }

    @Test(expected=WrongExpressionArityException.class)
    public void knownOverloadTooFewParams() {
        SimpleRegistry registry = new SimpleRegistry(MUL_INT);
        OverloadResolver resolver = new OverloadResolver(registry);
        resolver.get(MUL_NAME, prepVals(TINT));
    }

    @Test(expected=WrongExpressionArityException.class)
    public void knownOverloadTooManyParams() {
        SimpleRegistry registry = new SimpleRegistry(MUL_INT);
        OverloadResolver resolver = new OverloadResolver(registry);
        resolver.get(MUL_NAME, prepVals(TINT, TINT, TINT));
    }

    // default resolution, exact match
    @Test
    public void mulIntWithInts() {
        SimpleRegistry registry = new SimpleRegistry(MUL_INT);
        OverloadResolver resolver = new OverloadResolver(registry);
        assertSame(registry.validated(MUL_INT),
                   resolver.get(MUL_NAME, prepVals(TINT, TINT)).getOverload());
    }

    // default resolution, requires strong cast (int -> bigint)
    @Test
    public void mulBigIntWithInts() {
        SimpleRegistry registry = new SimpleRegistry(MUL_BIGINT);
        OverloadResolver resolver = new OverloadResolver(registry);
        assertSame(registry.validated(MUL_BIGINT),
                   resolver.get(MUL_NAME, prepVals(TINT, TINT)).getOverload());
    }

    // default resolution, requires weak cast (bigint -> int)
    @Test
    public void mulIntWithBigInts() {
        SimpleRegistry registry = new SimpleRegistry(MUL_INT);
        OverloadResolver resolver = new OverloadResolver(registry);
        assertSame(registry.validated(MUL_INT),
                   resolver.get(MUL_NAME, prepVals(TINT, TINT)).getOverload());
    }

    // input resolution, no casts
    @Test
    public void mulIntMulBigIntWithIntsNoCasts() {
        SimpleRegistry registry = new SimpleRegistry(MUL_INT, MUL_BIGINT);
        OverloadResolver resolver = new OverloadResolver(registry);
        assertSame(null,
                   resolver.get(MUL_NAME, prepVals(TINT, TINT)));
    }

    // input resolution, type only casts, only one candidate
    @Test
    public void mulIntMulBigIntWithIntsTypeOnlyCasts() {
        SimpleRegistry registry = new SimpleRegistry(MUL_INT, MUL_BIGINT);
        registry.setCasts(C_INT_INT, C_BIGINT_BIGINT);
        OverloadResolver resolver = new OverloadResolver(registry);
        assertSame(registry.validated(MUL_INT),
                   resolver.get(MUL_NAME, prepVals(TINT, TINT)).getOverload());
        assertSame(registry.validated(MUL_INT),
                   resolver.get(MUL_NAME, prepVals(TINT, TINT)).getOverload());
    }

    // input resolution, more casts, only one candidate for each
    @Test
    public void mulIntMulBigIntWithIntsAndIntBigintStrongAndWeakCasts() {
        SimpleRegistry registry = new SimpleRegistry(MUL_INT, MUL_BIGINT);
        registry.setCasts(C_INT_INT, C_INT_BIGINT, C_BIGINT_BIGINT, C_BIGINT_INT);
        OverloadResolver resolver = new OverloadResolver(registry);
        // 2 candidates, 1 more specific
        assertSame("INT*INT", registry.validated(MUL_INT),
                   resolver.get(MUL_NAME, prepVals(TINT, TINT)).getOverload());
        // 1 candidate
        assertSame("INT*BIGINT", registry.validated(MUL_BIGINT),
                   resolver.get(MUL_NAME, prepVals(TINT, TBIGINT)).getOverload());
        // 1 candidate
        assertSame("BIGINT*BIGINT", registry.validated(MUL_BIGINT),
                   resolver.get(MUL_NAME, prepVals(TBIGINT, TBIGINT)).getOverload());
    }
}
