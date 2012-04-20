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

import com.akiban.ais.model.Column;
import com.akiban.qp.operator.Cursor;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSourceHelper;
import com.akiban.server.types.ValueTarget;
import com.akiban.util.ByteSource;
import com.persistit.Key;
import com.persistit.Value;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class PersistitValueValueTarget implements ValueTarget {

    // PersistitKeyValueTarget interface

    public void attach(Value value) {
        this.value = value;
    }

    public PersistitValueValueTarget expectingType(AkType type) {
        this.type = type;
        return this;
    }

    public PersistitValueValueTarget expectingType(Column column) {
        return expectingType(column.getType().akType());
    }
    
    // ValueTarget interface

    @Override
    public void putNull() {
        checkState(AkType.NULL);
        value.putNull();
        invalidate();
    }

    @Override
    public void putDate(long value) {
        checkState(AkType.DATE);
        this.value.put(value);
        invalidate();
    }

    @Override
    public void putDateTime(long value) {
        checkState(AkType.DATETIME);
        this.value.put(value);
        invalidate();
    }

    @Override
    public void putDecimal(BigDecimal value) {
        checkState(AkType.DECIMAL);
        this.value.put(value);
        invalidate();
    }

    @Override
    public void putDouble(double value) {
        checkState(AkType.DOUBLE);
        this.value.put(value);
        invalidate();
    }

    @Override
    public void putFloat(float value) {
        checkState(AkType.FLOAT);
        this.value.put(value);
        invalidate();
    }

    @Override
    public void putInt(long value) {
        checkState(AkType.INT);
        this.value.put(value);
        invalidate();
    }

    @Override
    public void putLong(long value) {
        checkState(AkType.LONG);
        this.value.put(value);
        invalidate();
    }

    @Override
    public void putString(String value) {
        checkState(AkType.VARCHAR);
        this.value.put(value);
        invalidate();
    }

    @Override
    public void putText(String value) {
        checkState(AkType.TEXT);
        this.value.put(value);
        invalidate();
    }

    @Override
    public void putTime(long value) {
        checkState(AkType.TIME);
        this.value.put(value);
        invalidate();
    }

    @Override
    public void putTimestamp(long value) {
        checkState(AkType.TIMESTAMP);
        this.value.put(value);
        invalidate();
    }
    
    @Override
    public void putInterval_Millis(long value) {
        throw new UnsupportedOperationException("interval not supported yet");
    }

    @Override
    public void putInterval_Month (long value) {
        throw new UnsupportedOperationException ("interval not supported yet");
    }
    
    @Override
    public void putUBigInt(BigInteger value) {
        checkState(AkType.U_BIGINT);
        this.value.put(value);
        invalidate();
    }

    @Override
    public void putUDouble(double value) {
        checkState(AkType.U_DOUBLE);
        this.value.put(value);
        invalidate();
    }

    @Override
    public void putUFloat(float value) {
        checkState(AkType.U_FLOAT);
        this.value.put(value);
        invalidate();
    }

    @Override
    public void putUInt(long value) {
        checkState(AkType.U_INT);
        this.value.put(value);
        invalidate();
    }

    @Override
    public void putVarBinary(ByteSource value) {
        checkState(AkType.VARBINARY);
        this.value.putByteArray(value.byteArray(), value.byteArrayOffset(), value.byteArrayLength());
        invalidate();
    }

    @Override
    public void putYear(long value) {
        checkState(AkType.YEAR);
        this.value.put(value);
        invalidate();
    }

    @Override
    public void putBool(boolean value) {
        checkState(AkType.BOOL);
        this.value.put(value);
        invalidate();
    }

    @Override
    public void putResultSet(Cursor value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AkType getConversionType() {
        return type;
    }

    // object interface

    @Override
    public String toString() {
        return value.toString();
    }

    // private methods

    private void checkState(AkType type) {
        ValueSourceHelper.checkType(this.type, type);
    }

    private void invalidate() {
        type = AkType.UNSUPPORTED;
    }

    // object state

    private Value value;
    private AkType type = AkType.UNSUPPORTED;
}
