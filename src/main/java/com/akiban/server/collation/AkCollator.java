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

import com.akiban.server.PersistitKeyPValueSource;
import com.akiban.server.PersistitKeyValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PValueSource;
import com.persistit.Key;

public abstract class AkCollator {

    public static String getString(PValueSource valueSource) {
        if (valueSource.isNull())
            return null;
        else if (valueSource.hasRawValue())
            return valueSource.getString();
        else if (valueSource.hasCacheValue())
            return (String) valueSource.getObject();
        throw new AssertionError("no value");
    }

    private final String collatorName;

    private final String collatorScheme;

    private final int collationId;

    protected AkCollator(final String collatorName, final String collatorScheme, final int collationId) {
        this.collatorName = collatorName;
        this.collatorScheme = collatorScheme;
        this.collationId = collationId;
    }

    /**
     * @return true if this collator is capable of precisely recovering the key
     *         string from a key segment.
     */
    abstract public boolean isRecoverable();

    /**
     * Append a String to a Key
     * 
     * @param key
     * @param value
     */
    abstract public void append(Key key, String value);

    /**
     * Decode a String from a Key segment
     * 
     * @param key
     * @return the decoded String
     * @throws UnsupportedOperationException
     *             for collations in which a precise transformation is
     *             impossible. See {@link #isRecoverable()}.
     */
    abstract public String decode(Key key);

    /**
     * Compare two string values: Comparable<ValueSource>
     */
    final public int compare(ValueSource value1, ValueSource value2) {
        boolean persistit1 = value1 instanceof PersistitKeyValueSource;
        boolean persistit2 = value2 instanceof PersistitKeyValueSource;
        if (persistit1 && persistit2) {
            return ((PersistitKeyValueSource) value1).compare((PersistitKeyValueSource) value2);
        } else if (persistit1) {
            return ((PersistitKeyValueSource) value1).compare(this, value2.getString());
        } else if (persistit2) {
            return -((PersistitKeyValueSource) value2).compare(this, value1.getString());
        } else {
            return compare(value1.getString(), value2.getString());
        }
    }

    /**
     * Compare two string values: Comparable<PValueSource>
     */
    final public int compare(PValueSource value1, PValueSource value2) {
        boolean persistit1 = value1 instanceof PersistitKeyPValueSource;
        boolean persistit2 = value2 instanceof PersistitKeyPValueSource;
        if (persistit1 && persistit2) {
            return ((PersistitKeyPValueSource) value1).compare((PersistitKeyPValueSource) value2);
        } else if (persistit1) {
            return ((PersistitKeyPValueSource) value1).compare(this, getString(value2));
        } else if (persistit2) {
            return -((PersistitKeyPValueSource) value2).compare(this, getString(value1));
        } else {
            return compare(getString(value1), getString(value2));
        }
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

    @Override
    public String toString() {
        return collatorName + "(" + collatorScheme + ")";
    }

    public int getCollationId() {
        return collationId;
    }

    public String getName() {
        return collatorName;
    }

    public String getScheme() {
        return collatorScheme;
    }

    /**
     * Construct the sort key bytes for the given String value. This method is
     * intended for use only by {@link CStringKeyCoder} and is therefore
     * package-private.
     * 
     * @param value
     *            the String
     * @return sort key bytes, last byte only must be zero
     */
    abstract byte[] encodeSortKeyBytes(String value);

    /**
     * Recover a String value which may be approximate. For example The string
     * may be spelled with incorrect case and not correctly represent some
     * characters. This method is intended for use only by
     * {@link CStringKeyCoder} and is therefore package-private.
     * 
     * @param bytes
     *            the sort key bytes
     * @param index
     *            index within array of first sorted key byte
     * @param length
     *            number of sorted key bytes
     * @return the decoded String value
     * @throws UnsupportedOperationException
     *             if unable to decode sort keys
     */
    abstract String decodeSortKeyBytes(byte[] bytes, int index, int length);
}
