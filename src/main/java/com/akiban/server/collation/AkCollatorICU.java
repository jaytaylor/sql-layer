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

import java.util.Arrays;

import com.akiban.server.types.ValueSource;
import com.ibm.icu.text.Collator;
import com.persistit.Key;
import com.persistit.util.Util;

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
    AkCollatorICU(final String name, final String scheme, final int collationId) {
        super(name, scheme, collationId);
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
            key.append(new CString(value, getCollationId()));
        }
    }

   
    @Override
    public String decode(Key key) {
        throw new UnsupportedOperationException("Unable to decode a collator sort key");
    }

    @Override
    public int compare(ValueSource value1, ValueSource value2) {
        return compare(value1.getString(), value2.getString());
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
     * @return sort key bytes, last byte only is zero
     */
    @Override
    byte[] encodeSortKeyBytes(String value) {
        return collator.get().getCollationKey(value).toByteArray();
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
        return AkCollator.hashCode(bytes, 0, bytes.length);
    }

    @Override
    public int hashCode(Key key) {
        return CStringKeyCoder.hashCode(getCollationId(), key);
    }


}
