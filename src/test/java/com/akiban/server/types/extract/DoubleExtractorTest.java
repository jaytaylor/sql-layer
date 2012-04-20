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

    
    @Test
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
