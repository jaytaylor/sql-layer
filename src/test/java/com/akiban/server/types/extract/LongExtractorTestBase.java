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

abstract public class LongExtractorTestBase {
    static protected class TestElement {
        private final long longVal;
        private final String string;

        public TestElement(String s, Number n) {
            this.longVal = n.longValue();
            this.string = s;
        }

        @Override
        public String toString() {
            return String.format("(%d, %s)", longVal, string);
        }
    }

    protected final LongExtractor ENCODER;
    protected final TestElement[] TEST_CASES;

    public LongExtractorTestBase(LongExtractor encoder, TestElement[] testElements) {
        this.ENCODER = encoder;
        this.TEST_CASES = testElements;
    }

    protected String encodeAndDecode(String str) {
        final long val = ENCODER.getLong(str);
        return ENCODER.asString(val);
    }

    @Test
    public void decodingToString() {
        int count = 0;
        for(TestElement t : TEST_CASES) {
            final String decoded = ENCODER.asString(t.longVal);
            Assert.assertEquals("test case [" + count + "] long->String: " + t, t.string, decoded);
            ++count;
        }
    }
}
