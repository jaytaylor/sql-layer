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

import com.ibm.icu.text.CollationKey;
import com.ibm.icu.text.Collator;
import com.persistit.util.Util;

/**
 * TODO: Prototype
 * 
 * Represent a String and an ICU4J Collator. Instances can be presented to the
 * {@link com.persistit.Key#append(Object)} method for encoding because Akiban
 * Server registers a {@link com.persistit.encoding.KeyRenderer} for this class.
 * The actual conversion from String to stored bytes is performed by the
 * Collator.
 * 
 * 
 * @author peter
 * 
 */
public class CString implements Comparable<CString> {

    private final Collator collator;

    private String string;

    private byte[] sortKeyBytes;

    /**
     * Construct an instance containing the original source string. This
     * instance may be used for encoding.
     * 
     * @param string
     * @param collator
     */
    public CString(final String string, final Collator collator) {
        this.string = string;
        this.collator = collator;
    }

    /**
     * Construct an instance that will hold the result of
     * {@link com.persistit.Key#decode(Object)}. This instance will not have a
     * String value, but will have the sort key bytes so that
     * 
     * @param collator
     */
    public CString(final Collator collator) {
        this(null, collator);
    }

    public static int compare(CString a, CString b) {
        return a.compareTo(b);
    }
    
    public static int compare(CString a, String b) {
        return a.compareTo(new CString(b, a.getCollator()));
    }
    
    public static int compare(String a, CString b) {
        return -b.compareTo(new CString(a, b.getCollator()));
    }
    
    public static int compare(String a, String b) {
        return a.compareTo(b);
    }
    /**
     * Construct an instance that can hold a byte array but has no other useful
     * behavior. This instance will be created only when
     * {@link com.persistit.Key#decode()} is used without supplying a CString to
     * be populated. The issue is that when this call is made, the choice of
     * Collator is not known.
     */
    public CString() {
        this(null, null);
    }

    /**
     * Comparison is done differently depending on whether the sort key is known
     * for both CStrings.
     * 
     * @param other
     *            the CString to compare to.
     * @return integer that negative, positive or zero as this CString collates
     *         before, after or equal to the supplied CString
     */
    @Override
    public int compareTo(CString other) {
        if (other == this) {
            return 0;
        }
        if (!collator.equals(other.collator)) {
            throw new IllegalArgumentException("Collator mismatch");
        }
        if (other.sortKeyBytes != null && sortKeyBytes != null) {
            return compareTo(sortKeyBytes, other.sortKeyBytes);
        }
        if (string == null) {
            byte[] a = sortKeyBytes;
            byte[] b = collator.getCollationKey(other.string).toByteArray();
            return compareTo(a, b);
        }
        if (other.string == null) {
            byte[] a = collator.getCollationKey(string).toByteArray();
            byte[] b = other.sortKeyBytes;
            return compareTo(a, b);
        }
        return collator.compare(string, other.string);
    }

    static int compareTo(final byte[] a, final byte[] b) {
        int length = Math.min(a.length, b.length);
        for (int i = 0; i < length; i++) {
            int diff = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (diff != 0) {
                return diff;
            }
        }
        return a.length > length ? 1 : b.length > length ? -1 : 0;
    }

    public Collator getCollator() {
        return collator;
    }

    public String getString() {
        return string;
    }
    
    public void setString(final String string) {
        this.string = string;
    }

    public boolean hasSortKeyBytes() {
        return sortKeyBytes != null;
    }

    public byte[] getSortKeyBytes() {
        if (sortKeyBytes == null) {
            CollationKey key = collator.getCollationKey(string);
            sortKeyBytes = key.toByteArray();
        }
        return sortKeyBytes;
    }

    /**
     * Copies the supplied array to the sortKeyBytes field of this object.
     * Removes the source string if there is one since the bytes do not
     * necessarily represent the sort order of the original string.
     * 
     * @param bytes
     */
    public void putSortKeyBytes(final byte[] bytes) {
        putSortKeyBytes(bytes, 0, bytes.length);
    }

    /**
     * Copies the supplied sub-array to the sortKeyBytes field of this object.
     * Removes the source string if there is one since the bytes do not
     * necessarily represent the sort order of the original string.
     * 
     * @param bytes
     * @param index
     * @param length
     * 
     */
    public void putSortKeyBytes(final byte[] bytes, int index, int length) {
        if (sortKeyBytes == null || sortKeyBytes.length != length) {
            sortKeyBytes = new byte[length];
        }
        System.arraycopy(bytes, index, sortKeyBytes, 0, length);
        string = null;
    }

    void removeSortKeyBytes() {
        sortKeyBytes = null;
    }
    
    /**
     * Simple toString to make contents visible while debugging
     */
    @Override
    public String toString() {
        if (string != null) {
            return string;
        }
        if (sortKeyBytes != null) {
            StringBuilder sb = new StringBuilder("CString[");
            Util.bytesToHex(sb, sortKeyBytes, 0, sortKeyBytes.length);
            sb.append("]");
            return sb.toString();
        }
        return "CString[null]";
    }
}
