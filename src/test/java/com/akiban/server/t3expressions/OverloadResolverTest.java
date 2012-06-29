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

import com.akiban.qp.operator.QueryContext;
import com.akiban.server.error.NoSuchFunctionException;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TBundleID;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TFactory;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.common.types.NoAttrTClass;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import com.akiban.server.types3.texpressions.TValidatedOverload;
import org.junit.Test;

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
            throw new UnsupportedOperationException();
        }

        @Override
        public TClassPossibility commonTClass(TClass one, TClass two) {
            throw new UnsupportedOperationException();
        }

        public TValidatedOverload getValidated(TOverload overload) {
            return originalMap.get(overload);
        }
    }

    private static TBundleID TEST_BUNDLE_ID = new TBundleID("test", new UUID(0,0));

    private static class TestClassBase extends NoAttrTClass {
        public TestClassBase(String name, PUnderlying pUnderlying) {
            super(TEST_BUNDLE_ID, name, 1, 1, 1, pUnderlying);
        }
    }

    private static final String MUL_NAME = "*";
    private static class MulOverloadBase extends TOverloadBase {
        private final TClass tClass;

        public MulOverloadBase(TClass tClass) {
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

    private final static TClass TEST_INT = new TestClassBase("int", PUnderlying.INT_32);
    private final static TClass TEST_BIGINT = new TestClassBase("bigint", PUnderlying.INT_64);
    private final static TClass TEST_DOUBLE = new TestClassBase("double",  PUnderlying.DOUBLE);
    private final static MulOverloadBase MUL_INT = new MulOverloadBase(TEST_INT);
    private final static MulOverloadBase MUL_BIGINT = new MulOverloadBase(TEST_BIGINT);
    private final static MulOverloadBase MUL_DOUBLE = new MulOverloadBase(TEST_DOUBLE);

    private static TPreptimeValue prepVal(TClass tClass) {
        return new TPreptimeValue(tClass.instance());
    }

    @Test(expected=NoSuchFunctionException.class)
    public void noSuchOverload() {
        SimpleRegistry registry = new SimpleRegistry();
        OverloadResolver resolver = new OverloadResolver(registry);
        resolver.get("foo", Arrays.asList(prepVal(TEST_INT)));
    }

    // default resolution (single overload), exact match
    @Test
    public void defaultMulIntWithInts() {
        SimpleRegistry registry = new SimpleRegistry(MUL_INT);
        OverloadResolver resolver = new OverloadResolver(registry);
        assertSame(registry.getValidated(MUL_INT),
                   resolver.get(MUL_NAME, Arrays.asList(prepVal(TEST_INT), prepVal(TEST_INT))).getOverload());
    }

    // default resolution (single overload), requires strong cast (int -> bigint)
    @Test
    public void defaultMulBigIntWithInts() {
        SimpleRegistry registry = new SimpleRegistry(MUL_BIGINT);
        OverloadResolver resolver = new OverloadResolver(registry);
        assertSame(registry.getValidated(MUL_BIGINT),
                   resolver.get(MUL_NAME, Arrays.asList(prepVal(TEST_INT), prepVal(TEST_INT))).getOverload());
    }

    // default resolution (single overload), requires weak cast (bigint -> int)
    @Test
    public void defaultMulIntWithBigInts() {
        SimpleRegistry registry = new SimpleRegistry(MUL_INT);
        OverloadResolver resolver = new OverloadResolver(registry);
        assertSame(registry.getValidated(MUL_INT),
                   resolver.get(MUL_NAME, Arrays.asList(prepVal(TEST_INT), prepVal(TEST_INT))).getOverload());
    }
}
