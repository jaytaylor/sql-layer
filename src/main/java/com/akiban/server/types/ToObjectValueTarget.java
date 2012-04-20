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

package com.akiban.server.types;

import com.akiban.qp.operator.Cursor;
import com.akiban.server.types.conversion.Converters;
import com.akiban.util.ByteSource;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class ToObjectValueTarget implements ValueTarget {
    
    // ToObjectValueTarget interface

    /**
     * Convenience method for extracting an Object from a conversion source.
     * @param source the incoming source
     * @return the converted Object
     */
    public Object convertFromSource(ValueSource source) {
        expectType(source.getConversionType());
        return Converters.convert(source, this).lastConvertedValue();
    }

    public ToObjectValueTarget expectType(AkType type) {
        this.akType = type;
        putPending = true;
        return this;
    }
    
    public Object lastConvertedValue() {
        if (putPending) {
            throw new IllegalStateException("put is pending for type " + akType);
        }
        return result;
    }
    
    // ValueTarget interface

    @Override
    public void putNull() {
        internalPut(null, AkType.NULL);
    }

    @Override
    public void putDate(long value) {
        internalPut(value, AkType.DATE);
    }

    @Override
    public void putDateTime(long value) {
        internalPut(value, AkType.DATETIME);
    }

    @Override
    public void putDecimal(BigDecimal value) {
        internalPut(value, AkType.DECIMAL);
    }

    @Override
    public void putDouble(double value) {
        internalPut(value, AkType.DOUBLE);
    }

    @Override
    public void putFloat(float value) {
        internalPut(value, AkType.FLOAT);
    }

    @Override
    public void putInt(long value) {
        internalPut(value, AkType.INT);
    }

    @Override
    public void putLong(long value) {
        internalPut(value, AkType.LONG);
    }

    @Override
    public void putString(String value) {
        internalPut(value, AkType.VARCHAR);
    }

    @Override
    public void putText(String value) {
        internalPut(value, AkType.TEXT);
    }

    @Override
    public void putTime(long value) {
        internalPut(value, AkType.TIME);
    }

    @Override
    public void putTimestamp(long value) {
        internalPut(value, AkType.TIMESTAMP);
    }
    
    @Override
    public void putInterval_Millis(long value){
        internalPut(value, AkType.INTERVAL_MILLIS);
    }

    @Override
    public void putInterval_Month(long value) {
        internalPut(value, AkType.INTERVAL_MONTH);
    }
    
    @Override
    public void putUBigInt(BigInteger value) {
        internalPut(value, AkType.U_BIGINT);
    }

    @Override
    public void putUDouble(double value) {
        internalPut(value, AkType.U_DOUBLE);
    }

    @Override
    public void putUFloat(float value) {
        internalPut(value, AkType.U_FLOAT);
    }

    @Override
    public void putUInt(long value) {
        internalPut(value, AkType.U_INT);
    }

    @Override
    public void putVarBinary(ByteSource value) {
        internalPut(value, AkType.VARBINARY);
    }

    @Override
    public void putYear(long value) {
        internalPut(value, AkType.YEAR);
    }

    @Override
    public void putBool(boolean value) {
        internalPut(value, AkType.BOOL);
    }

    @Override
    public void putResultSet(Cursor value) {
        internalPut(value, AkType.RESULT_SET);
    }

    @Override
    public AkType getConversionType() {
        return akType;
    }

    // Object interface

    @Override
    public String toString() {
        return String.format("Converted(%s %s)",
                akType,
                putPending ? "<put pending>" : result
        );
    }

    // for use in this class
    
    private void internalPut(Object value, AkType type) {
        ValueSourceHelper.checkType(akType, type);
        if (!putPending) {
            throw new IllegalStateException("no put pending: " + toString());
        }
        result = value;
        putPending = false;
    }
    
    // object state
    
    private Object result;
    private AkType akType = AkType.UNSUPPORTED;
    private boolean putPending = true;
}
