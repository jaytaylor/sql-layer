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

package com.akiban.server.types.typestests;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.util.WrappingByteSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.akiban.server.types.AkType.*;

public final class MismatchedConversionsSuite {

    static ConversionSuite<?> basedOn(LinkedConversion<?> baseConversion) {
        List<AkType> validTypes = new ArrayList<AkType>();
        Collections.addAll(validTypes, AkType.values());
        validTypes.removeAll(invalidTypes);
        validTypes.removeAll(baseConversion.unsupportedTypes());
        validTypes = Collections.unmodifiableList(validTypes); // sanity check

        List<AkType> scratchPad = new ArrayList<AkType>(validTypes);
        ConversionSuite.SuiteBuilder<Switcher> suiteBuilder
                = ConversionSuite.build(new DelegateLinkedConversion(baseConversion));

        final int cap = cap();
        for (AkType expected : validTypes) {
            scratchPad.remove(expected);
            Collections.shuffle(scratchPad);

            for (int i=0; i < scratchPad.size() && i <= cap; ++i) {
                AkType switchTo = scratchPad.get(i);
                TestCase<Switcher> switcherTC = TestCase.derive(
                        gettersAndPutters.get(expected),
                        new Switcher(switchTo, expected)
                );
                suiteBuilder.add(switcherTC);
            }

            scratchPad.add(expected);
        }
        return suiteBuilder.suite();
    }

    private static int cap() {
        String capProp = System.getProperty(MISMATCHED_CONVERSIONS_PROP, DEFAULT_PERMUTATIONS);
        if (capProp.equalsIgnoreCase(ALL)) {
            return AkType.values().length;
        }
        final int cap;
        try {
            cap = Integer.parseInt(capProp);
        } catch (NumberFormatException e) {
            throw new NumberFormatException(
                    MISMATCHED_CONVERSIONS_PROP + " must be an int or \"" + ALL + "\": was " + capProp
            );
        }
        if (cap < 0) {
            throw new RuntimeException(MISMATCHED_CONVERSIONS_PROP + " must be positive: was " + cap);
        }
        return cap;
    }

    private static Map<AkType,TestCase<?>> createGettersAndPutters() {
        EnumMap<AkType,TestCase<?>> map = new EnumMap<AkType, TestCase<?>>(AkType.class);
        
        map.put(DATE, TestCase.forDate(0, NO_STATE));
        map.put(DATETIME, TestCase.forDateTime(0, NO_STATE));
        map.put(DECIMAL, TestCase.forDecimal(BigDecimal.ONE, 1, 0, NO_STATE));
        map.put(DOUBLE, TestCase.forDouble(0, NO_STATE));
        map.put(FLOAT, TestCase.forFloat(0, NO_STATE));
        map.put(INT, TestCase.forInt(0, NO_STATE));
        map.put(LONG, TestCase.forLong(0, NO_STATE));
        map.put(VARCHAR, TestCase.forString("world", 5, "US-ASCII", NO_STATE));
        map.put(TEXT, TestCase.forText("world", 5, "US-ASCII", NO_STATE));
        map.put(TIME, TestCase.forTime(0, NO_STATE));
        map.put(TIMESTAMP, TestCase.forTimestamp(0, NO_STATE));
        map.put(INTERVAL_MILLIS, TestCase.forInterval_Millis(0, NO_STATE));
        map.put(INTERVAL_MONTH, TestCase.forInterval_Month(0, NO_STATE));
        map.put(U_BIGINT, TestCase.forUBigInt(BigInteger.TEN, NO_STATE));
        map.put(U_DOUBLE, TestCase.forUDouble(0, NO_STATE));
        map.put(U_FLOAT, TestCase.forUFloat(0, NO_STATE));
        map.put(U_INT, TestCase.forUInt(0, NO_STATE));
        map.put(VARBINARY, TestCase.forVarBinary(new WrappingByteSource().wrap(new byte[0]), 0, NO_STATE));
        map.put(YEAR, TestCase.forYear(0, NO_STATE));
        map.put(BOOL, TestCase.forBool(false, NO_STATE));

        Set<AkType> allValidAkTypes = EnumSet.allOf(AkType.class);
        allValidAkTypes.removeAll(invalidTypes);
        Set<AkType> mappedTypes = map.keySet();
        if (!allValidAkTypes.equals(mappedTypes)) {
            allValidAkTypes.removeAll(mappedTypes);
            throw new RuntimeException("Found unmapped but valid type(s). This isn't your fault; the error is in "
                    + MismatchedConversionsSuite.class.getSimpleName() + ". Unmapped types: " + allValidAkTypes
            );
        }
        
        return map;
    }

    private MismatchedConversionsSuite() {}

    private static final String MISMATCHED_CONVERSIONS_PROP = "akserver.test.mismatched-conversions";
    private static final String ALL = "ALL";
    private static final String DEFAULT_PERMUTATIONS = "ALL";
    private static final Object NO_STATE = "SWITCH";
    private static final Set<AkType> invalidTypes = EnumSet.of(NULL, UNSUPPORTED, RESULT_SET);
    private static final Map<AkType,TestCase<?>> gettersAndPutters = createGettersAndPutters();

    static class Switcher {

        TestCase<?> switchTo() {
            return switchTo;
        }

        @Override
        public String toString() {
            return "switching to " + switchTo;
        }

        Switcher(AkType switchTo, AkType switchFrom) {
            this.switchTo = TestCase.derive(
                    gettersAndPutters.get(switchTo),
                    "switched from " + switchFrom
            );
        }

        private final TestCase<?> switchTo;
    }

    static class DelegateLinkedConversion implements LinkedConversion<Switcher> {
        @Override
        public ValueSource linkedSource() {
            return delegate.linkedSource();
        }

        @Override
        public ValueTarget linkedTarget() {
            return delegate.linkedTarget();
        }

        @Override
        public void setUp(TestCase<?> testCase) {
            delegate.setUp(testCase);
        }

        @Override
        public void syncConversions() {
            delegate.syncConversions();
        }

        @Override
        public void checkPut(Switcher expected) {
            throw new UnsupportedOperationException("this shouldn't be called!");
        }

        @Override
        public Set<? extends AkType> unsupportedTypes() {
            return delegate.unsupportedTypes();
        }

        private DelegateLinkedConversion(LinkedConversion<?> delegate) {
            this.delegate = delegate;
        }

        private final LinkedConversion<?> delegate;
    }
}
