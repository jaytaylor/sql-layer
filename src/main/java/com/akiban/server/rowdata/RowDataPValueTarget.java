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

package com.akiban.server.rowdata;

import com.akiban.server.AkServerUtil;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.util.ArgumentValidation;
import java.math.BigInteger;

public final class RowDataPValueTarget implements PValueTarget {

    public void bind(FieldDef fieldDef, byte[] backingBytes, int offset) {
        clear();
        ArgumentValidation.notNull("fieldDef", fieldDef);
        ArgumentValidation.withinArray("backing bytes", backingBytes, "offset", offset);
        this.fieldDef = fieldDef;
        this.bytes = backingBytes;
        this.offset = offset;
    }

    public int lastEncodedLength() {
        if (lastEncodedLength < 0) {
            throw new IllegalStateException("no last recorded length available");
        }
        return lastEncodedLength;
    }

    public RowDataPValueTarget() {
        clear();
    }

    // ValueTarget interface

    @Override
    public void putNull() {
        setNullBit();
        recordEncoded(0);
    }

    @Override
    public void putDouble(double value) {
        recordEncoded(encodeLong(Double.doubleToLongBits(value)));
    }

    @Override
    public void putFloat(float value) {
        recordEncoded(encodeInt(Float.floatToIntBits(value)));
    }
    
    @Override
    public PUnderlying getUnderlyingType() {
        return fieldDef.column().tInstance().typeClass().underlyingType();
    }

    @Override
    public void putValueSource(PValueSource source) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putBool(boolean value) {
        recordEncoded(encodeLong(value ? 1 : 0));
    }

    @Override
    public void putInt8(byte value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putInt16(short value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putUInt16(char value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putInt32(int value) {
        recordEncoded(encodeInt(value));
    }

    @Override
    public void putInt64(long value) {
        recordEncoded(encodeLong(value));
    }

    @Override
    public void putBytes(byte[] value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putObject(Object object) {
        throw new UnsupportedOperationException();
    }

    // private methods
    
    private void recordEncoded(int encodedLength) {
        clear();
        lastEncodedLength = encodedLength;
    }

    private void clear() {
        lastEncodedLength = -1;
        bytes = null;
        offset = -1;
    }

    private int encodeInt(int value) {
        assert INT_STORAGE_SIZE == fieldDef.getMaxStorageSize() : fieldDef.getMaxStorageSize();
        return AkServerUtil.putIntegerByWidth(bytes, offset, INT_STORAGE_SIZE, value);
    }

    private int encodeLong(long value) {
        int width = fieldDef.getMaxStorageSize();
        return AkServerUtil.putIntegerByWidth(bytes, offset, width, value);
    }

    private boolean encodableAsLong(BigInteger value) {
        return value.compareTo(MAX_BIGINT) <= 0;
    }

    private void setNullBit() {
        // TODO unloop this
        int target = fieldDef.getFieldIndex();
        int fieldCount = fieldDef.getRowDef().getFieldCount();
        int offsetWithinMap = offset;
        for (int index = 0; index < fieldCount; index += 8) {
            for (int j = index; j < index + 8 && j < fieldCount; j++) {
                if (j == target) {
                    bytes[offsetWithinMap] |= (1 << j - index);
                    return;
                }
            }
            ++offsetWithinMap;
        }
        throw new AssertionError("field not found! " + fieldDef);
    }

    // object state

    private FieldDef fieldDef;
    private int lastEncodedLength;
    private byte bytes[];
    private int offset;

    // consts

    private static final int INT_STORAGE_SIZE = 4;

    /**
     * We want to encode BigInteger as long, so we require it to be smaller than (2^64) + 1
     */
    private static final BigInteger MAX_BIGINT = BigInteger.valueOf(Long.MAX_VALUE);
}
