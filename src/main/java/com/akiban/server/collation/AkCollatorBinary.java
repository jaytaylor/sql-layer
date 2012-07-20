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
import com.persistit.Key;

public class AkCollatorBinary extends AkCollator {

    public AkCollatorBinary() {
        super(AkCollatorFactory.UCS_BINARY, AkCollatorFactory.UCS_BINARY, 0);
    }
    
    @Override
    public boolean isRecoverable() {
        return true;
    }

    @Override
    public void append(Key key, String value) {
        key.append(value);
    }

    @Override
    public String decode(Key key) {
        return key.decodeString();
    }

    /**
     * Append the given value to the given key.
     */
    public byte[] encodeSortKeyBytes(String value) {
        throw new UnsupportedOperationException("No sort key encoding for binary collation");
    }

    /**
     * Recover the value or throw an unsupported exception.
     */
    public String decodeSortKeyBytes(byte[] bytes, int index, int length) {
        throw new UnsupportedOperationException("No sort key encoding for binary collation");
    }


    @Override
    public int compare(ValueSource value1, ValueSource value2) {
        return compare(value1.getString(), value2.getString());
    }

    @Override
    public int compare(String string1, String string2) {
        return string1.compareTo(string2);
    }

    @Override
    public boolean isCaseSensitive() {
        return true;
    }

    @Override
    public String toString() {
        return getName();
    }

    @Override
    public int hashCode(String string) {
        return string.hashCode();
    }

    @Override
    public int hashCode(Key key) {
        return ((String)key.decode()).hashCode();
    }
}
