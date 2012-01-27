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

package com.akiban.server.types.extract;

import com.akiban.server.error.InconvertibleTypesException;
import com.akiban.server.error.OverflowException;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;

import org.junit.Test;
import static org.junit.Assert.*;

import java.lang.Math;
import java.math.BigDecimal;
import java.math.BigInteger;

public class DoubleExtractorTest {
    static final DoubleExtractor EXTRACTOR = Extractors.getDoubleExtractor();
    static final ValueHolder HOLDER = new ValueHolder();

    protected void testExtract(double expected, double delta) {
        assertEquals(expected, EXTRACTOR.getDouble(HOLDER), delta);
    }

    @Test
    public void testBasic() {
        HOLDER.putDouble(Math.PI);
        testExtract(Math.PI, 0.0);

        HOLDER.putLong(1234);
        testExtract(1.234e3, 0.0);
        
        HOLDER.putString("-12.3");
        testExtract(-1.23e1, 0.0);

        HOLDER.putDecimal(new BigDecimal("23.45"));
        testExtract(2.345e1, 0.0);

        HOLDER.putUBigInt(BigInteger.valueOf(1).shiftLeft(128));
        testExtract(Double.longBitsToDouble(0x47F0000000000000L), 0.0);
    }

    
    @Test(expected = InconvertibleTypesException.class)
    public void testDateToDouble() {
        HOLDER.putDate(111);
        EXTRACTOR.getDouble(HOLDER);
    }

    @Test(expected = OverflowException.class)
    public void testDecimalTooLarge() {
        HOLDER.putDecimal(BigDecimal.valueOf(1).scaleByPowerOfTen(309));
        EXTRACTOR.getDouble(HOLDER);
    }    

    @Test(expected = OverflowException.class)
    public void testDecimalTooSmall() {
        HOLDER.putDecimal(BigDecimal.valueOf(-1).scaleByPowerOfTen(309));
        EXTRACTOR.getDouble(HOLDER);
    }    

    @Test(expected = OverflowException.class)
    public void testIntegerTooLarge() {
        HOLDER.putUBigInt(BigInteger.valueOf(1).shiftLeft(1024));
        EXTRACTOR.getDouble(HOLDER);
    }    

    @Test(expected = OverflowException.class)
    public void testIntegerTooSmall() {
        HOLDER.putUBigInt(BigInteger.valueOf(1).shiftLeft(1024).negate());
        EXTRACTOR.getDouble(HOLDER);
    }    

}
