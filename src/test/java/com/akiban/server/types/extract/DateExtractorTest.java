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

public class DateExtractorTest extends LongExtractorTestBase {
    public DateExtractorTest() {
        super(ExtractorsForDates.DATE,
              new TestElement[] {
                new TestElement("0000-00-00", 0),
                new TestElement("0000-00-31", 31),
                new TestElement("0000-01-31", 63),
                new TestElement("0000-12-00", 384),
                new TestElement("1986-10-28", 1017180),
                new TestElement("2011-04-07", 1029767),
                new TestElement("1620-11-21", 829813),
                new TestElement("0320-06-14", 164046),
                new TestElement("9999-12-25", 5119897L)
              });
    }


    @Test
    public void partiallySpecified() {
        Assert.assertEquals("0002-00-00", encodeAndDecode("2"));
        Assert.assertEquals("0020-00-00", encodeAndDecode("20"));
        Assert.assertEquals("0201-00-00", encodeAndDecode("201"));
        Assert.assertEquals("2011-00-00", encodeAndDecode("2011"));
        Assert.assertEquals("2011-04-00", encodeAndDecode("2011-4"));
        Assert.assertEquals("2011-04-08", encodeAndDecode("2011-4-8"));
    }

    @Test(expected=InvalidDateFormatException.class)
    public void tooManyParts() {
        encodeAndDecode("2011-04-04-04");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void invalidNumber() {
        encodeAndDecode("2011-04-zebra");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void noNumbers() {
        encodeAndDecode("a-b-c");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void noHyphens() {
        encodeAndDecode("banana");
    }
}
