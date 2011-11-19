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
import com.akiban.server.types.extract.ConverterTestUtils;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.LongExtractor;
import com.akiban.util.ByteSource;
import com.akiban.util.WrappingByteSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static com.akiban.server.types.typestests.TestCase.NO_STATE;

final class StandardTestCases {
    static Collection<TestCase<?>> get() {
        return onlyList;
    }

    private static Collection<TestCase<?>> make() {
        List<TestCase<?>> list = new ArrayList<TestCase<?>>();
        
        // TODO need to figure out upper and lower limits, test those

        list.add(TestCase.forDate(0, NO_STATE));
        list.add(TestCase.forDate(5119897L, NO_STATE)); // 9999-12-15
        
        list.add(TestCase.forDateTime(0, NO_STATE));
        list.add(TestCase.forDateTime(99991231235959L, NO_STATE));
        
        list.add(TestCase.forDecimal(BigDecimal.ZERO, 1, 0, NO_STATE));
        list.add(TestCase.forDecimal(BigDecimal.ONE, 1, 0, NO_STATE));
        list.add(TestCase.forDecimal(new BigDecimal("10.0"), 3, 1, NO_STATE));
        list.add(TestCase.forDecimal(BigDecimal.valueOf(-10), 10, 0, NO_STATE));
        
        list.add(TestCase.forDouble(0, NO_STATE));
        list.add(TestCase.forDouble(-0, NO_STATE));
        list.add(TestCase.forDouble(1, NO_STATE));
        list.add(TestCase.forDouble(-1, NO_STATE));
        list.add(TestCase.forDouble(Double.MIN_VALUE, NO_STATE));
        list.add(TestCase.forDouble(Double.MAX_VALUE, NO_STATE));
        
        list.add(TestCase.forFloat(0, NO_STATE));
        list.add(TestCase.forFloat(-0, NO_STATE));
        list.add(TestCase.forFloat(1, NO_STATE));
        list.add(TestCase.forFloat(-1, NO_STATE));
        list.add(TestCase.forFloat(Float.MIN_VALUE, NO_STATE));
        list.add(TestCase.forFloat(Float.MAX_VALUE, NO_STATE));

        list.add(TestCase.forInt(-1, NO_STATE));
        list.add(TestCase.forInt(0, NO_STATE));
        list.add(TestCase.forInt(1, NO_STATE));
        list.add(TestCase.forInt(-1, Long.MAX_VALUE));
        list.add(TestCase.forInt(-1, Long.MIN_VALUE));

        list.add(TestCase.forLong(-1, NO_STATE));
        list.add(TestCase.forLong(0, NO_STATE));
        list.add(TestCase.forLong(1, NO_STATE));
        list.add(TestCase.forLong(-1, Long.MAX_VALUE));
        list.add(TestCase.forLong(-1, Long.MIN_VALUE));

        list.add(TestCase.forString("", 32, "UTF-8", NO_STATE));
        list.add(TestCase.forString("word", 32, "UTF-8", NO_STATE));
        list.add(TestCase.forString("☃", 32, "UTF-8", NO_STATE));
        list.add(TestCase.forString("a ☃ is cold", 32, "UTF-8", NO_STATE));

        list.add(TestCase.forText("", 32, "UTF-8", NO_STATE));
        list.add(TestCase.forText("word", 32, "UTF-8", NO_STATE));
        list.add(TestCase.forText("☃", 32, "UTF-8", NO_STATE));
        list.add(TestCase.forText("a ☃ is cold", 32, "UTF-8", NO_STATE));

        list.add(TestCase.forTime(-1, NO_STATE));
        list.add(TestCase.forTime(0, NO_STATE));
        list.add(TestCase.forTime(1, NO_STATE));

        list.add(TestCase.forBool(true, NO_STATE));
        list.add(TestCase.forBool(false, NO_STATE));

        LongExtractor timestampExtractor = Extractors.getLongExtractor(AkType.TIMESTAMP);
        ConverterTestUtils.setGlobalTimezone("UTC");
        list.add(TestCase.forTimestamp(timestampExtractor.getLong("0000-00-00 00:00:00"), NO_STATE));
        list.add(TestCase.forTimestamp(timestampExtractor.getLong("1970-01-01 00:00:01"), NO_STATE));
        list.add(TestCase.forTimestamp(timestampExtractor.getLong("2011-08-18 15:09:00"), NO_STATE));
        list.add(TestCase.forTimestamp(timestampExtractor.getLong("2038-01-19 03:14:06"), NO_STATE));

        list.add(TestCase.forUBigInt(BigInteger.ZERO, NO_STATE));
        list.add(TestCase.forUBigInt(BigInteger.ONE, NO_STATE));
        list.add(TestCase.forUBigInt(BigInteger.TEN, NO_STATE));

        list.add(TestCase.forUDouble(0, NO_STATE));
        list.add(TestCase.forUDouble(-0, NO_STATE));
        list.add(TestCase.forUDouble(1, NO_STATE));
        list.add(TestCase.forUDouble(-1, NO_STATE));
        list.add(TestCase.forUDouble(Double.MIN_VALUE, NO_STATE));
        list.add(TestCase.forUDouble(Double.MAX_VALUE, NO_STATE));

        list.add(TestCase.forUFloat(0, NO_STATE));
        list.add(TestCase.forUFloat(-0, NO_STATE));
        list.add(TestCase.forUFloat(1, NO_STATE));
        list.add(TestCase.forUFloat(-1, NO_STATE));
        list.add(TestCase.forUFloat(Float.MIN_VALUE, NO_STATE));
        list.add(TestCase.forUFloat(Float.MAX_VALUE, NO_STATE));

        list.add(TestCase.forUInt(0, NO_STATE));
        list.add(TestCase.forUInt(1, NO_STATE));
        list.add(TestCase.forUInt(Integer.MAX_VALUE, NO_STATE));
        list.add(TestCase.forUInt(((long)Math.pow(2, 32))-1, NO_STATE));

        list.add(TestCase.forVarBinary(wrap(), 0, NO_STATE));
        list.add(TestCase.forVarBinary(wrap(Byte.MIN_VALUE, Byte.MAX_VALUE, 0), 2, NO_STATE));

        LongExtractor yearConverter = Extractors.getLongExtractor(AkType.YEAR);
        list.add(TestCase.forYear(yearConverter.getLong("0000"), NO_STATE));
        list.add(TestCase.forYear(yearConverter.getLong("1901"), NO_STATE));
        list.add(TestCase.forYear(yearConverter.getLong("1983"), NO_STATE));
        list.add(TestCase.forYear(yearConverter.getLong("2155"), NO_STATE));

        list.add(TestCase.forInterval(0, NO_STATE));
        list.add(TestCase.forInterval(Long.MAX_VALUE, NO_STATE));
        list.add(TestCase.forInterval(Long.MIN_VALUE, NO_STATE));

        verifyAllTypesTested(list);

        return Collections.unmodifiableCollection(list);
    }

    private static void verifyAllTypesTested(Collection<? extends TestCase<?>> testCases) {
        Set<AkType> allTypes = EnumSet.allOf(AkType.class);
        allTypes.removeAll(EnumSet.of(AkType.UNSUPPORTED, AkType.NULL));
        for (TestCase<?> testCase : testCases) {
            allTypes.remove(testCase.type());
        }
        if (!allTypes.isEmpty()) {
            throw new RuntimeException("untested types: " + allTypes);
        }
    }

    private static ByteSource wrap(int... values) {
        byte[] bytes = new byte[values.length];
        for (int i=0; i < bytes.length; ++i) {
            int asInt = values[i];
            if (asInt < Byte.MIN_VALUE || asInt > Byte.MAX_VALUE) {
                throw new AssertionError("invalid byte value " + asInt);
            }
            bytes[i] = (byte)asInt;
        }
        return new WrappingByteSource().wrap(bytes);
    }

    private static Collection<TestCase<?>> onlyList = make();
}
