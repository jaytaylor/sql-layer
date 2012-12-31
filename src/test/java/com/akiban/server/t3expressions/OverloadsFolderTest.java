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

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.T3TestClass;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;
import com.akiban.server.types3.texpressions.TValidatedScalar;
import com.google.common.base.Function;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class OverloadsFolderTest {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        Cases test = new Cases();

         // "Sandwiched" overloads:
        test.overloads(
                "A, B...",
                "C, D...",
                "E, B...")
                .differentAt(0).differentAt(1).differentAt(100);
        
        // fixed arity
        test.overloads(
                "A")
                .sameAt(0).undefinedAt(1).undefinedAt(100);

        test.overloads(
                "A",
                "B")
                .differentAt(0).undefinedAt(1).undefinedAt(100);
        test.overloads(
                "A",
                "A")
                .sameAt(0).undefinedAt(1).undefinedAt(100);

        // one vararg
        test.overloads(
                "A...")
                .sameAt(0).sameAt(1).sameAt(100);

        // vararg + fixed arity
        test.overloads(
                "A",
                "B...")
                .differentAt(0).sameAt(1).sameAt(100);

        test.overloads(
                "A, B",
                "A, A...")
                .sameAt(0).differentAt(1).sameAt(2).sameAt(100);

        test.overloads(
                "A",
                "A...")
                .sameAt(0).sameAt(1).sameAt(100);

        test.overloads(
                "B...",
                "B")
                .sameAt(0).sameAt(1).sameAt(100);
        
        test.overloads(
                "A",
                "A...",
                "B...")
                .differentAt(0).differentAt(1).differentAt(100);
        
        test.overloads(
                "A...",
                "B",
                "A, B, A...")
                .differentAt(0).differentAt(1).sameAt(2).sameAt(100);

        // two varargs

        test.overloads( // note: this isn't allowed by other types3 components, but let's check it anyway
                "A...",
                "A...")
                .sameAt(0).sameAt(1).sameAt(100);

        test.overloads(
                "A...",
                "B...")
                .differentAt(0).differentAt(1).differentAt(100);

        test.overloads(
                "A...",
                "B, B...")
                .differentAt(0).differentAt(1).differentAt(100);
        test.overloads(
                "A...",
                "A, A...")
                .sameAt(0).sameAt(1).sameAt(100);

        test.overloads(
                "A, B, A",
                "A...")
                .sameAt(0).differentAt(1).sameAt(2).sameAt(100);
        test.overloads(
                "A, B, A...",
                "A...")
                .sameAt(0).differentAt(1).sameAt(2).sameAt(100);
        test.overloads(
                "A, B, C...",
                "A...")
                .sameAt(0).differentAt(1).differentAt(2).differentAt(100);
        
        // four overloads
        test.overloads(
                "A, B",
                "B...",
                "C, B, D",
                "E")
                .differentAt(0).sameAt(1).differentAt(2).sameAt(3).sameAt(100);

        // overloads take ANY
        test.overloads(
                "?",
                "?")
                .sameAt(0).undefinedAt(1);
        test.overloads(
                "A",
                "?")
                .sameAt(0).undefinedAt(1);
        test.overloads(
                "?",
                "A")
                .sameAt(0).undefinedAt(1);
        test.overloads(
                "?...")
                .sameAt(0).sameAt(1).sameAt(100);

        return test.build();
    }

    @Test
    public void sameTypeAt() {
        assertEquals(sameTypeAtExpected, foldResult.at(pos, null));
    }

    public OverloadsFolderTest(OverloadsFolder.Result<Boolean> foldResult, int pos, Boolean sameTypeAtExpected) {
        this.foldResult = foldResult;
        this.pos = pos;
        this.sameTypeAtExpected = sameTypeAtExpected;
    }

    private final OverloadsFolder.Result<Boolean> foldResult;
    private final int pos;
    private final Boolean sameTypeAtExpected;

    private static final Function<TClass, Boolean> notNull = new Function<TClass, Boolean>() {
        @Override
        public Boolean apply(TClass input) {
            return input != ResolvablesRegistry.differentTargetTypes;
        }
    };

    private static class Cases {
        public Cases overloads(String... overloads) {
            flush();
            this.overloadDefs = overloads;
            this.expectations = new HashMap<Integer, Boolean>();
            return this;
        }

        public Cases sameAt(int pos) {
            expectations.put(pos, Boolean.TRUE);
            return this;
        }

        public Cases differentAt(int pos) {
            expectations.put(pos, Boolean.FALSE);
            return this;
        }

        public Cases undefinedAt(int pos) {
            expectations.put(pos, null);
            return this;
        }

        public Collection<Parameterization> build() {
            flush();
            return pb.asList();
        }

        private void flush() {
            if (overloadDefs == null) {
                assert expectations == null : expectations;
                return; // must have been the first call to start()
            }
            Collection<TValidatedScalar> overloads = new ArrayList<TValidatedScalar>();
            for (String overloadDef : overloadDefs) {
                TScalar scalar = parse(overloadDef);
                TValidatedScalar validated = new TValidatedScalar(scalar);
                overloads.add(validated);
            }
            OverloadsFolder.Result<TClass> foldToClass = ResolvablesRegistry.sameInputSets.fold(overloads);
            OverloadsFolder.Result<Boolean> foldResult = foldToClass.transform(notNull);

            StringBuilder overloadsDescBuilder = new StringBuilder();
            for (int i = 0; i < overloadDefs.length; i++) {
                String overloadDef = overloadDefs[i];
                overloadsDescBuilder.append(FUNC_NAME).append('(').append(overloadDef).append(')');
                if (i + 1 < overloadDefs.length)
                    overloadsDescBuilder.append(", ");
            }
            String overloadsDescr = overloadsDescBuilder.toString();

            for (Map.Entry<Integer, Boolean> expected : expectations.entrySet()) {
                int pos = expected.getKey();
                Boolean sameTypeAt = expected.getValue();
                String caseName = overloadsDescr + " at " + pos;
                pb.add(caseName, foldResult, pos, sameTypeAt);
            }
        }

        private TScalar parse(final String overloadDef) {
            return new TScalarBase() {
                @Override
                protected void buildInputSets(TInputSetBuilder builder) {
                    String[] args = overloadDef.split(", *");
                    for (int pos = 0; pos < args.length; pos++) {
                        String arg = args[pos];
                        boolean isVararg = false;
                        if (arg.endsWith("...")) {
                            isVararg = true;
                            arg = arg.substring(0, arg.length() - 3);
                        }
                        assert arg.length() == 1 : arg;
                        char argChar = arg.charAt(0);
                        TClass tClass;
                        if (argChar == '?') {
                            tClass = null;
                        }
                        else {
                            assert Character.isLetter(argChar) : arg;
                            tClass = tClasses.get(arg);
                            if (tClass == null) {
                                tClass = new T3TestClass(arg);
                                tClasses.put(arg, tClass);
                            }
                        }
                        if (isVararg)
                            builder.vararg(tClass);
                        else
                            builder.covers(tClass, pos);
                    }
                }

                @Override
                protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs,
                                          PValueTarget output) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public String displayName() {
                    return "FOO";
                }

                @Override
                public TOverloadResult resultType() {
                    return TOverloadResult.fixed(RET_TYPE);
                }
            };

        }

        private final ParameterizationBuilder pb = new ParameterizationBuilder();
        private final Map<String,TClass> tClasses = new HashMap<String, TClass>();
        private String[] overloadDefs;
        private Map<Integer, Boolean> expectations;
    }

    private static final String FUNC_NAME = "FOO";
    private static final TClass RET_TYPE = new T3TestClass("R");
}
