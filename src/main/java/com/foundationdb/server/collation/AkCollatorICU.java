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


import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.ibm.icu.text.Collator;
import com.persistit.Key;
import com.persistit.util.Util;
import java.util.Arrays;

public class AkCollatorICU extends AkCollator {

    ThreadLocal<Collator> collator = new ThreadLocal<Collator>() {
        protected Collator initialValue() {
            return AkCollatorFactory.forScheme(getScheme());
        }
    };

    /**
     * Create an AkCollator which may be used in across multiple threads. Each
     * instance of AkCollator has a ThreadLocal which optionally contains a
     * reference to a thread-private ICU4J Collator.
     * 
     * @param name
     *            Name given to AkCollatorFactory by which to look up the scheme
     * @param scheme
     *            Formatted string containing Locale name, and collation string
     *            strength.
     */
    AkCollatorICU(final String scheme, final int collationId) {
        super(scheme, collationId);
        collator.get(); // force the collator to initialize (to test scheme)
    }

    @Override
    public boolean isRecoverable() {
        return false;
    }

    @Override
    public void append(Key key, String value) {
        if (value == null) {
            key.append(null);
        } else {
            key.append(encodeSortKeyBytes(value));
        }
    }

    @Override
    public String decode(Key key) {
        throw new UnsupportedOperationException("Unable to decode a collator sort key");
    }

    @Override
    public int compare(String source, String target) {
        return collator.get().compare(source, target);
    }

    @Override
    public boolean isCaseSensitive() {
        return collator.get().getStrength() > Collator.SECONDARY;
    }

    /**
     * Construct the sort key bytes for the given String value
     * 
     * @param value
     *            the String
     * @return sort key bytes
     */
    @Override
    byte[] encodeSortKeyBytes(String value) {
        byte[] bytes = collator.get().getCollationKey(value).toByteArray();
        return Arrays.copyOf(bytes, bytes.length - 1); // Remove terminating null.
    }

    /**
     * Decode the value to a string of hex digits. For ICU4J collations that's
     * the best we can do. This method is used by the
     * {@link CStringKeyCoder#displayKeySegment} method.
     * 
     * @param bytes
     *            Bytes to decode
     * @param index
     *            starting index
     * @param length
     *            number of bytes to decode
     */
    @Override
    String decodeSortKeyBytes(byte[] bytes, int index, int length) {
        StringBuilder sb = new StringBuilder();
        Util.bytesToHex(sb, bytes, index, length);
        return sb.toString();
    }

    @Override
    public int hashCode(String string) {
        byte[] bytes = collator.get().getCollationKey(string).toByteArray();
        bytes = Arrays.copyOfRange(bytes, 0, bytes.length-1); // remove null terminating character
        return hashCode(bytes);
    }

    @Override
    public int hashCode(byte[] bytes) {
        return hashFunction.hashBytes(bytes, 0, bytes.length).asInt();
    }

    private final HashFunction hashFunction = Hashing.goodFastHash(32); // Because we're returning ints
}
