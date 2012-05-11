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

package com.akiban.server.types.conversion;

import com.akiban.server.error.InvalidCharToNumException;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.extract.Extractors;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class DoubleConverterTest {
    static private class TestElement {
        private final double dbl;
        private final String str;
        private final long longBits;

        public TestElement(double dbl, long bits) {
            this.dbl = dbl;
            this.str = Double.valueOf(dbl).toString();
            this.longBits = bits;
        }

        @Override
        public String toString() {
            return String.format("(%s, %f, %d)", str, dbl, longBits);
        }
    }

    static private final double EPSILON = 0;

    private final TestElement[] TEST_CASES = {
            new TestElement(                       -0.0d, 0x8000000000000000L),
            new TestElement(                        0.0d, 0x0000000000000000L),
            new TestElement(                       -1.0d, 0xBFF0000000000000L),
            new TestElement(                        1.0d, 0x3FF0000000000000L),
            new TestElement(   839573957392.29575739275d, 0x42686F503D620977L),
            new TestElement(            -0.986730586093d, 0xBFEF934C05A76F64L),
            new TestElement(428732459843.84344482421875d, 0x4258F49C8AD0F5FBL),
            new TestElement(               2.7182818284d, 0x4005BF0A8B12500BL),
            new TestElement(          -9007199250000000d, 0xC33FFFFFFFB7A880L),
            new TestElement(        7385632847582937583d, 0x43D99FC27C6C68D0L)
    };

    @Test
    public void encodeToBits() {
        FromObjectValueSource source = new FromObjectValueSource();
        for(TestElement t : TEST_CASES) {
            final double fromDouble = Extractors.getDoubleExtractor().getDouble(source.setReflectively(t.dbl));
            final double fromString = Extractors.getDoubleExtractor().getDouble(source.setReflectively(t.str));
            assertEquals("float->bits: " + t, t.longBits, Double.doubleToLongBits(fromDouble));
            assertEquals("string->bits: " + t, t.longBits, Double.doubleToLongBits(fromString));
        }
    }

    @Test(expected=InvalidCharToNumException.class)
    public void invalidNumber() {
        Extractors.getDoubleExtractor().getDouble(new FromObjectValueSource().setReflectively("zebra"));
    }
}
