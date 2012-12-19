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
package com.akiban.server;

import com.akiban.server.collation.AkCollator;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.persistit.Value;

public final class PersistitValuePValueTarget implements PValueTarget {
    
    // PersistitValuePValueTarget interface
    
    public void attach(Value value) {
        this.value = value;
    }
    
    // PValueTarget interface
    
    @Override
    public boolean supportsCachedObjects() {
        return false;
    }

    @Override
    public void putObject(Object object) {
        value.put(object);
    }

    @Override
    public TClass getUnderlyingType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putNull() {
        value.putNull();
    }

    @Override
    public void putBool(boolean value) {
        this.value.put(value);
    }

    @Override
    public void putInt8(byte value) {
        this.value.put(value);
    }

    @Override
    public void putInt16(short value) {
        this.value.put(value);
    }

    @Override
    public void putUInt16(char value) {
        this.value.put(value);
    }

    @Override
    public void putInt32(int value) {
        this.value.put(value);
    }

    @Override
    public void putInt64(long value) {
        this.value.put(value);
    }

    @Override
    public void putFloat(float value) {
        this.value.put(value);
    }

    @Override
    public void putDouble(double value) {
        this.value.put(value);
    }

    @Override
    public void putBytes(byte[] value) {
        this.value.put(value);
    }

    @Override
    public void putString(String value, AkCollator collator) {
        this.value.put(value);
    }
    
    private Value value;
}
