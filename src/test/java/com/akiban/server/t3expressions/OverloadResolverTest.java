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
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TBundleID;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TCastBase;
import com.akiban.server.types3.TCastIdentifier;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.TStrongCasts;
import com.akiban.server.types3.common.types.NoAttrTClass;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.Constantness;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;
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

        public TestClassBase(String name, PUnderlying pUnderlying) {
            super(TEST_BUNDLE_ID, name, null, null, 1, 1, 1, pUnderlying, null, 64, null);
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
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
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
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
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
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
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

    private final static TClass TINT = new TestClassBase("int", PUnderlying.INT_32);
    private final static TClass TBIGINT = new TestClassBase("bigint", PUnderlying.INT_64);
    private final static TClass TDATE = new TestClassBase("date",  PUnderlying.DOUBLE);
    private final static TClass TVARCHAR = new TestClassBase("varchar",  PUnderlying.BYTES);

    private final static TestCastBase C_INT_BIGINT = new TestCastBase(TINT, TBIGINT, true);
    private final static TestCastBase C_BIGINT_INT = new TestCastBase(TBIGINT, TINT, false);

    private final static TestMulBase MUL_INTS = new TestMulBase(TINT);
    private final static TestMulBase MUL_BIGINTS = new TestMulBase(TBIGINT);
    private final static TestMulBase MUL_DATE_INT = new TestMulBase(TDATE, TINT, TDATE);


    private T3RegistryService registry;

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
            T3RegistryServiceImpl registryImpl = new T3RegistryServiceImpl();
            registryImpl.start(finder);
            registry = registryImpl;
        }

        private InstanceFinderBuilder finder = new InstanceFinderBuilder();
        private Set<TCastIdentifier> castIdentifiers = new HashSet<TCastIdentifier>();
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
        } catch (OverloadException e) {
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
    @Test(expected = OverloadException.class)
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
        } catch (OverloadException e) {
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
        } catch (OverloadException e) {
            // can't find picking type for first if it's null
        }
    }
}
