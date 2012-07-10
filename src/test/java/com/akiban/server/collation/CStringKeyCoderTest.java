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

package com.akiban.server.collation;

import static org.junit.Assert.*;

import org.junit.Test;

import com.google.inject.Exposed;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.exception.ConversionException;
import com.persistit.util.Util;

public class CStringKeyCoderTest {

    @Test
    public void appendKeySegment() throws Exception {
        final CStringKeyCoder coder = new CStringKeyCoder();
        final AkCollator akCollator = AkCollatorFactory.getAkCollator("latin1_swedish_ci");
        final Key key = new Key((Persistit) null);
        try {
            coder.appendKeySegment(key, "Not a CString", null);
            fail("Expected exception");
        } catch (ConversionException e) {
            // expected
        }

        clear(key).append(123);
        final Key copy = new Key(key);
        int initialSize = key.getEncodedSize();
        coder.appendKeySegment(key, new CString("abcde", akCollator.getCollationId()), null);
        assertTrue("Some bytes should have been added", key.getEncodedSize() > initialSize + 2);
        byte[] bytes = key.getEncodedBytes();
        int end = initialSize;
        while (bytes[end] != 0) {
            end++;
        }
        assertTrue("Other bytes should not change", equals(key, copy, 0, initialSize));
        assertTrue("Other bytes should not change", equals(key, copy, end, bytes.length - end));
        assertEquals("Lead byte should encode the collationId", bytes[initialSize], akCollator.getCollationId());
        assertEquals("No nulls in key segment", key.getEncodedSize(), end);
    }

    @Test
    public void decodeKeySegment() throws Exception {
        final CStringKeyCoder coder = new CStringKeyCoder();
        final Key key = new Key((Persistit) null);
        try {
            coder.decodeKeySegment(key, CString.class, null);
            fail("Expected exception");
        } catch (ConversionException e) {
            // expected
        }
        try {
            coder.renderKeySegment(key, new CString(), CString.class, null);
            fail("Expected exception");
        } catch (ConversionException e) {
            // expected
        }
    }

    @Test
    public void displayKeySegment() throws Exception {
        final CStringKeyCoder coder = new CStringKeyCoder();
        final AkCollator akCollator = AkCollatorFactory.getAkCollator("latin1_swedish_ci");
        final Key key = new Key((Persistit) null);
        clear(key);
        coder.appendKeySegment(key, new CString("abcde", akCollator.getCollationId()), null);
        StringBuilder sb = new StringBuilder();
        coder.displayKeySegment(key, sb, CString.class, null);
        assertEquals("Expect decode to upper case", "ABCDE", sb.toString());
    }

    private boolean equals(final Key k1, final Key k2, final int offset, final int length) {
        for (int i = offset; i < offset + length; i++) {
            if (k1.getEncodedBytes()[i] != k2.getEncodedBytes()[i]) {
                return false;
            }
        }
        return true;
    }
    
    
    private Key clear(final Key key) {
        byte[] bytes = key.getEncodedBytes();
        Util.clearBytes(bytes, 0, bytes.length);
        key.clear();
        return key;
    }
}
