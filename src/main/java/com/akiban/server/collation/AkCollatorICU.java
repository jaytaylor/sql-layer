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

import com.akiban.server.types.ValueSource;
import com.ibm.icu.text.Collator;
import com.persistit.Key;

public class AkCollatorICU implements AkCollator {

    Collator collator;

    AkCollatorICU(final Collator collator) {
        this.collator = collator;
    }

    @Override
    public boolean isRecoverable() {
        return false;
    }

    @Override
    public void append(Key key, String value) {
        byte[] sortBytes = collator.getCollationKey(value).toByteArray();
        byte[] keyBytes = key.getEncodedBytes();
        int size = key.getEncodedSize();
        if (size + sortBytes.length > key.getMaximumSize()) {
            throw new IllegalArgumentException("Too long: " + size + sortBytes.length);
        }
        assert verifySortByteZeroes(sortBytes) : "ICU4J is expected to return a zero-terminated sort key";
        System.arraycopy(sortBytes, 0, keyBytes, size, sortBytes.length);
        key.setEncodedSize(size + sortBytes.length);
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
        return collator.compare(source, target);
    }

    @Override
    public Collator getCollator() {
        return collator;
    }
    

    @Override
    public boolean isCaseSensitive() {
        return collator.getStrength() > Collator.SECONDARY;
    }

    private boolean verifySortByteZeroes(final byte[] a) {
        for (int index = 0; index < a.length - 1; index++) {
            if (a[index] == 0) {
                return false;
            }
        }
        if (a[a.length - 1] != 0) {
            return false;
        }
        return true;
    }
}
