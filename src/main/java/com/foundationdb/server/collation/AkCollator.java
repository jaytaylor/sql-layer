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

import com.foundationdb.server.PersistitKeyValueSource;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.WrappingByteSource;
import com.persistit.Key;
import com.google.common.primitives.UnsignedBytes;

public abstract class AkCollator {

    /** Debug only: *cannot* be used for returning, converting, etc */
    public static String getDebugString(ValueSource valueSource, AkCollator collator) {
        if (valueSource.isNull())
            return null;
        Object obj = valueSource.getObject();
        if (obj instanceof String) {
            return (String)obj;
        }
        if (obj instanceof WrappingByteSource) {
            obj = ((WrappingByteSource)obj).byteArray();
        }
        if (obj instanceof byte[]) {
                byte[] bytes = (byte[])obj;
                assert (collator != null) : "encoded as bytes without collator";
                return collator.debugDecodeSortKeyBytes(bytes, 0, bytes.length);
        }
        throw new AssertionError("no value");
    }

    public static int hashValue(ValueSource valueSource, AkCollator collator) {
        if (valueSource.isNull())
            return collator.hashCode((String)null);
        Object obj = valueSource.getObject();
        if (obj instanceof String) {
            return collator.hashCode(valueSource.getString());
        }
        if (obj instanceof WrappingByteSource) {
            obj = ((WrappingByteSource)obj).byteArray();
        }
        if (obj instanceof byte[]) {
            byte[] bytes = (byte[])obj;
            assert (collator != null) : "encoded as bytes without collator";
            return collator.hashCode(bytes);
        }
        throw new AssertionError("no value");
    }

    private final String collatorScheme;

    private final int collationId;

    protected AkCollator(final String collatorScheme, final int collationId) {
        this.collatorScheme = collatorScheme;
        this.collationId = collationId;
    }

    /**
     * @return true if this collator is capable of precisely recovering the key
     *         string from a key segment.
     */
    abstract public boolean isRecoverable();

    /** Append a String to a Key, encoding to bytes if appropriate. */
    abstract public void append(Key key, String value);

    /**
     * Compare two string values: Comparable<ValueSource>
     */
    final public int compare(ValueSource value1, ValueSource value2) {
        boolean persistit1 = value1 instanceof PersistitKeyValueSource;
        boolean persistit2 = value2 instanceof PersistitKeyValueSource;

        if (persistit1 && persistit2) {
            return ((PersistitKeyValueSource) value1).compare((PersistitKeyValueSource) value2);
        }
        if (value1.isNull()) {
            return value2.isNull() ? 0 : -1;
        }
        if (value2.isNull()) {
            return 1;
        }

        // Delicate: If one is a key source the other might not be.
        // Need to normalize to bytes e.g. AkCollatorBinary appends a string directly (not sortKeyBytes)

        Object o1 = value1.getObject();
        Object o2 = value2.getObject();
        if(isByteLike(o1) || isByteLike(o2)) {
            return UnsignedBytes.lexicographicalComparator().compare(getBytes(o1), getBytes(o2));
        } else {
            assert o1 instanceof String : o1;
            assert o2 instanceof String : o2;
            return compare((String)o1, (String)o2);
        }
    }

    private byte[] getBytes(Object obj) {
        assert obj != null;
        if (obj instanceof byte[]) {
            return (byte[]) obj;
        }
        if (obj instanceof WrappingByteSource){
            return ((WrappingByteSource)obj).byteArray();
        }
        if (obj instanceof String) {
            return encodeSortKeyBytes((String)obj);
        }
        throw new AssertionError("Unexpected ValueSource object type: " + obj.getClass().getName());
    }

    private boolean isByteLike(Object obj) {
        return (obj instanceof byte[]) || (obj instanceof WrappingByteSource);
    }

    /**
     * Compare two string objects: Comparable<String>
     */
    abstract public int compare(String string1, String string2);

    /**
     * @return whether the underlying collation scheme is case-sensitive
     */
    abstract public boolean isCaseSensitive();

    /**
     * Compute the hash of a String based on the sort keys produced by the
     * underlying collator. For example, if the collator is case-insensitive
     * then hash("ABC") == hash("abc").
     * 
     * @param string
     *            the String
     * @return the computed hash value
     * @throws NullPointerException
     *             if string is null
     */
    abstract public int hashCode(final String string);

    abstract public int hashCode(final byte[] bytes);

    @Override
    public String toString() {
        return collatorScheme;
    }

    public int getCollationId() {
        return collationId;
    }

    public String getScheme() {
        return collatorScheme;
    }

    /** Construct the sort key bytes for the given String value. */
    public abstract byte[] encodeSortKeyBytes(String value);

    /** Recover a String value which may be approximate. */
    abstract String debugDecodeSortKeyBytes(byte[] bytes, int index, int length);
}
