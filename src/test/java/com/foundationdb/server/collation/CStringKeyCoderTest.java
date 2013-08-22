/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.collation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.exception.ConversionException;
import com.persistit.util.Util;

public class CStringKeyCoderTest {

    @Test
    public void appendKeySegment() throws Exception {
        final CStringKeyCoder coder = new CStringKeyCoder();
        final AkCollator akCollator = AkCollatorFactory.getAkCollator("latin1_swedish_ci");
        final Key key = new TestKeyCreator().createKey();
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
        final Key key = new TestKeyCreator().createKey();
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
        final Key key = new TestKeyCreator().createKey();
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
