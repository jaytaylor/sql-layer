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
package com.akiban.server.types.typestests;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ConversionSource;
import com.akiban.server.types.ConversionTarget;
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
        map.put(DECIMAL, TestCase.forDecimal(BigDecimal.ONE, NO_STATE));
        map.put(DOUBLE, TestCase.forDouble(0, NO_STATE));
        map.put(FLOAT, TestCase.forFloat(0, NO_STATE));
        map.put(INT, TestCase.forInt(0, NO_STATE));
        map.put(LONG, TestCase.forLong(0, NO_STATE));
        map.put(VARCHAR, TestCase.forString("world", NO_STATE));
        map.put(TEXT, TestCase.forText("world", NO_STATE));
        map.put(TIME, TestCase.forTime(0, NO_STATE));
        map.put(TIMESTAMP, TestCase.forTimestamp(0, NO_STATE));
        map.put(U_BIGINT, TestCase.forUBigInt(BigInteger.TEN, NO_STATE));
        map.put(U_DOUBLE, TestCase.forUDouble(0, NO_STATE));
        map.put(U_FLOAT, TestCase.forUFloat(0, NO_STATE));
        map.put(U_INT, TestCase.forUInt(0, NO_STATE));
        map.put(VARBINARY, TestCase.forVarBinary(new WrappingByteSource().wrap(new byte[0]), NO_STATE));
        map.put(YEAR, TestCase.forYear(0, NO_STATE));
        
        return map;
    }

    private MismatchedConversionsSuite() {}

    private static final String MISMATCHED_CONVERSIONS_PROP = "akserver.test.mismatched-conversions";
    private static final String ALL = "ALL";
    private static final String DEFAULT_PERMUTATIONS = "1";
    private static final Object NO_STATE = "SWITCH";
    private static final Set<AkType> invalidTypes = EnumSet.of(NULL, UNSUPPORTED);
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
        public ConversionSource linkedSource() {
            return delegate.linkedSource();
        }

        @Override
        public ConversionTarget linkedTarget() {
            return delegate.linkedTarget();
        }

        @Override
        public void setUp(AkType type) {
            delegate.setUp(type);
        }

        @Override
        public void syncConversions() {
            delegate.syncConversions();
        }

        @Override
        public void checkPut(Switcher expected) {
            throw new UnsupportedOperationException("this shouldn't be called!");
        }

        private DelegateLinkedConversion(LinkedConversion<?> delegate) {
            this.delegate = delegate;
        }

        private final LinkedConversion<?> delegate;
    }
}
