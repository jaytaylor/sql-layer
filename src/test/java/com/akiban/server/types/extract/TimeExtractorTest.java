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

import org.junit.Assert;
import org.junit.Test;

import com.akiban.server.error.InvalidDateFormatException;

public class TimeExtractorTest extends LongExtractorTestBase {
    public TimeExtractorTest() {
        super(ExtractorsForDates.TIME,
              new TestElement[] {
                new TestElement("00:00:00", 0),
                new TestElement("00:00:01", 1),
                new TestElement("-00:00:01", -1),
                new TestElement("838:59:59", 8385959),
                new TestElement("-838:59:59", -8385959),
                new TestElement("14:20:32", 142032),
                new TestElement("-147:21:01", -1472101L)
              });
    }


    @Test
    public void partiallySpecified() {
        Assert.assertEquals("00:00:02", encodeAndDecode("2"));
        Assert.assertEquals("00:00:20", encodeAndDecode("20"));
        Assert.assertEquals("00:03:21", encodeAndDecode("201"));
        Assert.assertEquals("00:33:31", encodeAndDecode("2011"));
        Assert.assertEquals("00:05:42", encodeAndDecode("5:42"));
        Assert.assertEquals("-00:00:42", encodeAndDecode("-42"));
        Assert.assertEquals("-00:10:02", encodeAndDecode("-10:02"));
    }

    @Test(expected=InvalidDateFormatException.class)
    public void invalidNumber() {
        encodeAndDecode("20111zebra");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void noNumbers() {
        encodeAndDecode("zebra");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void tooManyParts() {
        encodeAndDecode("01:02:03:04");
    }
}
