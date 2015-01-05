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

import com.foundationdb.server.error.ArgumentTypeRequiredException;
import com.foundationdb.server.error.NoSuchCastException;
import com.foundationdb.server.error.NoSuchFunctionOverloadException;
import com.foundationdb.server.error.NoSuchFunctionException;
import com.foundationdb.server.error.WrongExpressionArityException;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TBundleID;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TCastBase;
import com.foundationdb.server.types.TCastIdentifier;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.TStrongCasts;
import com.foundationdb.server.types.common.types.NoAttrTClass;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.Constantness;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class OverloadResolverTest {

    private static class TestClassBase extends NoAttrTClass {
        private static final TBundleID TEST_BUNDLE_ID = new TBundleID("test", new UUID(0,0));

        public TestClassBase(String name, UnderlyingType underlyingType) {
            super(TEST_BUNDLE_ID, name, null, null, 1, 1, 1, underlyingType, null, 64, null);
        }

        @Override
        public TCast castToVarchar() {
            return null;
        }

        @Override
        public TCast castFromVarchar() {
            return null;
        }
    }

    private static class TestCastBase extends TCastBase {

        public TestCastBase(TClass source, TClass target, boolean isStrong) {
            super(source, target, Constantness.UNKNOWN);
            this.isStrong = isStrong;
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            throw new UnsupportedOperationException();
        }

        TStrongCasts strongCasts() {
            return isStrong
                    ? TStrongCasts.from(sourceClass()).to(targetClass())
                    : null;
        }

        private boolean isStrong;
    }


    private static final String MUL_NAME = "*";
    private static class TestMulBase extends TScalarBase {
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
        protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String displayName() {
            return MUL_NAME;
        }

        @Override
        public TOverloadResult resultType() {
            return TOverloadResult.fixed(tTarget);
        }
    }

    private static class TestGetBase extends TScalarBase {
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
        protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String displayName() {
            return name;
        }

        @Override
        public TOverloadResult resultType() {
            return (tResult == null) ? TOverloadResult.picking() : TOverloadResult.fixed(tResult);
        }
    }

    private final static TClass TINT = new TestClassBase("int", UnderlyingType.INT_32);
    private final static TClass TBIGINT = new TestClassBase("bigint", UnderlyingType.INT_64);
    private final static TClass TDATE = new TestClassBase("date",  UnderlyingType.DOUBLE);
    private final static TClass TVARCHAR = new TestClassBase("varchar",  UnderlyingType.BYTES);

    private final static TestCastBase C_INT_BIGINT = new TestCastBase(TINT, TBIGINT, true);
    private final static TestCastBase C_BIGINT_INT = new TestCastBase(TBIGINT, TINT, false);

    private final static TestMulBase MUL_INTS = new TestMulBase(TINT);
    private final static TestMulBase MUL_BIGINTS = new TestMulBase(TBIGINT);
    private final static TestMulBase MUL_DATE_INT = new TestMulBase(TDATE, TINT, TDATE);


    private TypesRegistryService registry;

    private class Initializer {
        public Initializer overloads(TScalar... scalars) {
            finder.put(TScalar.class, scalars);
            return this;
        }

        public Initializer types(TClass... classes) {
            finder.put(TClass.class, classes);
            return this;
        }

        public Initializer casts(TCast... casts) {
            for (TCast cast : casts) {
                if (!castIdentifiers.add(new TCastIdentifier(cast)))
                    continue;
                if (cast instanceof TestCastBase) {
                    TStrongCasts strongCasts = ((TestCastBase) cast).strongCasts();
                    if (strongCasts != null)
                        finder.put(TStrongCasts.class, strongCasts);
                }
                finder.put(TCast.class, cast);
                types(cast.sourceClass());
                types(cast.targetClass());
            }
            return this;
        }

        private void init() {
            TypesRegistryServiceImpl registryImpl = new TypesRegistryServiceImpl();
            registryImpl.start(finder);
            registry = registryImpl;
        }

        private InstanceFinderBuilder finder = new InstanceFinderBuilder();
        private Set<TCastIdentifier> castIdentifiers = new HashSet<>();
    }

    private static TPreptimeValue prepVal(TClass tClass) {
        return (tClass != null) ? new TPreptimeValue(tClass.instance(true)) : new TPreptimeValue();
    }

    private static List<TPreptimeValue> prepVals(TClass... tClasses) {
        TPreptimeValue[] prepVals = new TPreptimeValue[tClasses.length];
        for(int i = 0; i < tClasses.length; ++i) {
            prepVals[i] = prepVal(tClasses[i]);
        }
        return Arrays.asList(prepVals);
    }

    private void checkResolved(String msg, TScalar expected, String overloadName, List<TPreptimeValue> prepValues) {
        // result.getPickingClass() not checked, SimpleRegistry doesn't implement commonTypes()
        OverloadResolver.OverloadResult result = registry.getScalarsResolver().get(overloadName, prepValues);
        assertSame(msg, expected, result != null ? result.getOverload().getUnderlying() : null);
    }

    private void checkCommon(TClass a, TClass b, TClass common) {
        TClass actualCommon;
        try {
            actualCommon = registry.getCastsResolver().commonTClass(a, b);
        } catch (NoSuchCastException e) {
            actualCommon = null;
        }
        assertSame("common(" + a + ", " + b +")", common, actualCommon);
    }

    @Test
    public void findCommon_Int_Bigint() {
        new Initializer().casts(C_INT_BIGINT).init();
        checkCommon(TINT, TBIGINT, TBIGINT);
    }

    @Test
    public void findCommon_BigInt_Bigint() {
        new Initializer().types(TINT, TBIGINT).init();
        checkCommon(TBIGINT, TBIGINT, TBIGINT);
    }

    @Test
    public void findCommon_Int_Date() {
        new Initializer().types(TINT, TBIGINT, TDATE).init();
        checkCommon(TINT, TDATE, null);
    }

    @Test(expected=NoSuchFunctionException.class)
    public void noSuchOverload() {
        new Initializer().init();
        registry.getScalarsResolver().get("foo", Arrays.asList(prepVal(TINT)));
    }

    @Test(expected=WrongExpressionArityException.class)
    public void knownOverloadTooFewParams() {
        new Initializer().overloads(MUL_INTS).init();
        registry.getScalarsResolver().get(MUL_NAME, prepVals(TINT));
    }

    @Test(expected=WrongExpressionArityException.class)
    public void knownOverloadTooManyParams() {
        new Initializer().overloads(MUL_INTS).init();
        registry.getScalarsResolver().get(MUL_NAME, prepVals(TINT, TINT, TINT));
    }

    // default resolution, exact match
    @Test
    public void mulIntWithInts() {
        new Initializer().overloads(MUL_INTS).init();
        checkResolved("INT*INT", MUL_INTS, MUL_NAME, prepVals(TINT, TINT));
    }

    // default resolution, types don't match (no registered casts, int -> bigint)
    @Test
    public void mulBigIntWithInts() {
        new Initializer().overloads(MUL_BIGINTS).casts(C_INT_BIGINT).init();
        checkResolved("INT*INT", MUL_BIGINTS, MUL_NAME, prepVals(TINT, TINT));
    }

    // default resolution, requires weak cast (bigint -> int)
    @Test
    public void mulIntWithBigInts() {
        new Initializer().overloads(MUL_INTS).init();
        checkResolved("INT*INT", MUL_INTS, MUL_NAME, prepVals(TINT, TINT));
    }

    // input resolution, no casts
    @Test(expected = NoSuchFunctionOverloadException.class)
    public void mulIntMulBigIntWithIntsNoCasts() {
        new Initializer().types(TINT, TBIGINT, TDATE).overloads(MUL_INTS, MUL_BIGINTS).init();
        checkResolved("INT*INT", null, MUL_NAME, prepVals(TDATE, TDATE));
    }

    // input resolution, type only casts, only one candidate
    @Test
    public void mulIntMulBigIntWithIntsTypeOnlyCasts() {
        new Initializer()
                .types(TINT, TBIGINT)
                .overloads(MUL_INTS, MUL_BIGINTS).init();
        checkResolved("INT*INT", MUL_INTS, MUL_NAME, prepVals(TINT, TINT));
        checkResolved("BIGINT*BIGINT", MUL_BIGINTS, MUL_NAME, prepVals(TBIGINT, TBIGINT));
    }

    // input resolution, more casts, 2 candidates but 1 more specific
    @Test
    public void mulIntMulBigIntWithIntsAndIntBigintStrongAndWeakCasts() {
        new Initializer()
                .overloads(MUL_INTS, MUL_BIGINTS)
                .casts(C_INT_BIGINT, C_BIGINT_INT).init();
        // 2 candidates, 1 more specific
        checkResolved("INT*INT", MUL_INTS, MUL_NAME, prepVals(TINT, TINT));
    }

    @Test
    public void specMulExampleIntBigIntAndDateCombos() {
        new Initializer()
                .overloads(MUL_INTS, MUL_BIGINTS, MUL_DATE_INT)
                .casts(C_INT_BIGINT, C_BIGINT_INT)
                .types(TDATE).init();
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
        } catch (ArgumentTypeRequiredException e) {
            // expected
        }
    }

    @Test(expected = IllegalStateException.class)
    public void conflictingOverloads() {
        final String NAME = "foo";
        // Overloads aren't valid and should(?) be rejected by real registry,
        // but make sure resolver doesn't choke
        TestGetBase posPos = new TestGetBase(NAME, TINT);
        posPos.builder().covers(TINT, 0, 1);
        TestGetBase posRem = new TestGetBase(NAME, TINT);
        posRem.builder().covers(TINT, 0).vararg(TINT);

        new Initializer().overloads(posPos, posRem).init();
        checkResolved(NAME+"(INT,INT)", null, NAME, prepVals(TINT, TINT));
    }

    @Test
    public void noArg() {
        final String NAME = "foo";
        TestGetBase noArg = new TestGetBase(NAME, TINT);
        new Initializer().overloads(noArg).init();
        checkResolved(NAME+"()", noArg, NAME, prepVals());
    }

    @Test
    public void onePosAndRemainingWithPickingSet() {
        final String NAME = "coalesce";
        TestGetBase coalesce = new TestGetBase(NAME, TVARCHAR);
        coalesce.builder().pickingVararg(null, 0);
        new Initializer().overloads(coalesce).init();

        try {
            OverloadResolver.OverloadResult result = registry.getScalarsResolver().get(NAME, prepVals());
            fail("WrongArity expected but got: " + result);
        } catch(WrongExpressionArityException e) {
            // Expected
        }

        checkResolved(NAME + "(INT)", coalesce, NAME, prepVals(TINT));
        new Initializer().overloads(coalesce).casts(C_INT_BIGINT).types(TDATE).init();
        checkResolved(NAME+"(null,INT,BIGINT)", coalesce, NAME, prepVals(null, TINT, TBIGINT));
        try {
            checkResolved(NAME+"(null,DATE,INT)", coalesce, NAME, prepVals(null, TDATE, TINT));
            fail("expected overload exception");
        } catch (NoSuchCastException e) {
            // There is no common type between date and int
        }
    }

    @Test
    public void onlyPickingRemaining() {
        final String NAME = "first";
        TestGetBase first = new TestGetBase(NAME, null);
        first.builder.pickingVararg(null, 0);
        new Initializer().overloads(first).init();
        checkResolved(NAME + "(INT)", first, NAME, prepVals(TINT));
        new Initializer().overloads(first).casts(C_INT_BIGINT).init();
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
        } catch (ArgumentTypeRequiredException e) {
            // can't find picking type for first if it's null
        }
    }
}
