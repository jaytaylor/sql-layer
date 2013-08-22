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

package com.foundationdb.server;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.foundationdb.ais.model.CharsetAndCollation;
import com.foundationdb.ais.model.Column;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.ValueSourceHelper;
import com.foundationdb.server.types.ValueTarget;
import com.foundationdb.server.types.conversion.Converters;
import com.foundationdb.util.ByteSource;
import com.persistit.Key;

public final class PersistitKeyValueTarget implements ValueTarget {

    private AkCollator collator = null;
    
    // PersistitKeyValueTarget interface

    public void attach(Key key) {
        this.key = key;
    }
    
    public PersistitKeyValueTarget expectingType(AkType type, AkCollator collator) {
        if (type == AkType.INTERVAL_MILLIS || type == AkType.INTERVAL_MONTH)
            throw new UnsupportedOperationException();
        this.type = type;
        this.collator = collator;
        return this;
    }

    public PersistitKeyValueTarget expectingType(Column column) {
        return expectingType(column.getType().akType(), column.getCollator());
    }

    public void append(ValueSource valueSource, AkType akType, AkCollator collator) {
        expectingType(akType, this.collator);
        if (collator == null) {
            Converters.convert(valueSource, this);
        } else {
            if (valueSource instanceof PersistitKeyValueSource) {
                PersistitKeyValueSource persistitKeyValueSource = (PersistitKeyValueSource) valueSource;
                Key sourceKey = persistitKeyValueSource.key();
                int sourceDepth = persistitKeyValueSource.depth();
                sourceKey.indexTo(sourceDepth);
                int sourceStart = sourceKey.getIndex();
                sourceKey.indexTo(sourceDepth + 1);
                int sourceEnd = sourceKey.getIndex();
                byte[] sourceBytes = sourceKey.getEncodedBytes();
                int bytesToCopy = sourceEnd - sourceStart;
                byte[] targetBytes = this.key.getEncodedBytes();
                int targetSize = this.key.getEncodedSize();
                System.arraycopy(sourceBytes, sourceStart,
                                 targetBytes, targetSize,
                                 bytesToCopy);
                // We just wrote to Key internals. Invalidate cached state
                key.setEncodedSize(targetSize + bytesToCopy);
            } else if (valueSource.isNull()) {
                key.append(null);
            } else {
                collator.append(key, valueSource.getString());
            }
        }
    }

    // ValueTarget interface

    @Override
    public void putNull() {
        checkState(AkType.NULL);
        key.append(null);
        invalidate();
    }

    @Override
    public void putDate(long value) {
        checkState(AkType.DATE);
        key.append(value);
        invalidate();
    }

    @Override
    public void putDateTime(long value) {
        checkState(AkType.DATETIME);
        key.append(value);
        invalidate();
    }

    @Override
    public void putDecimal(BigDecimal value) {
        checkState(AkType.DECIMAL);
        key.append(value);
        invalidate();
    }

    @Override
    public void putDouble(double value) {
        checkState(AkType.DOUBLE);
        key.append(value);
        invalidate();
    }

    @Override
    public void putFloat(float value) {
        checkState(AkType.FLOAT);
        key.append(value);
        invalidate();
    }

    @Override
    public void putInt(long value) {
        checkState(AkType.INT);
        key.append(value);
        invalidate();
    }

    @Override
    public void putLong(long value) {
        checkState(AkType.LONG);
        key.append(value);
        invalidate();
    }

    @Override
    public void putString(String value) {
        checkState(AkType.VARCHAR);
        // TODO: Can remove this when there is always a collator for a string.
        if (collator == null)
            key.append(value);
        else
            collator.append(key, value);
        invalidate();
    }

    @Override
    public void putText(String value) {
        checkState(AkType.TEXT);
        // TODO: Can remove this when there is always a collator for a string.
        if (collator == null)
            key.append(value);
        else
            collator.append(key, value);
        invalidate();
    }

    @Override
    public void putTime(long value) {
        checkState(AkType.TIME);
        key.append(value);
        invalidate();
    }

    @Override
    public void putTimestamp(long value) {
        checkState(AkType.TIMESTAMP);
        key.append(value);
        invalidate();
    }
    
    @Override
    public void putInterval_Millis(long value) {
        throw new UnsupportedOperationException("interval not supported yet");
    }

    @Override
    public void putInterval_Month(long value) {
        throw new UnsupportedOperationException("interval not supported yet");
    }

    @Override
    public void putUBigInt(BigInteger value) {
        checkState(AkType.U_BIGINT);
        key.append(value);
        invalidate();
    }

    @Override
    public void putUDouble(double value) {
        checkState(AkType.U_DOUBLE);
        key.append(value);
        invalidate();
    }

    @Override
    public void putUFloat(float value) {
        checkState(AkType.U_FLOAT);
        key.append(value);
        invalidate();
    }

    @Override
    public void putUInt(long value) {
        checkState(AkType.U_INT);
        key.append(value);
        invalidate();
    }

    @Override
    public void putVarBinary(ByteSource value) {
        checkState(AkType.VARBINARY);
        key().appendByteArray(value.byteArray(), value.byteArrayOffset(), value.byteArrayLength());
        invalidate();
    }

    @Override
    public void putYear(long value) {
        checkState(AkType.YEAR);
        key.append(value);
        invalidate();
    }

    @Override
    public void putBool(boolean value) {
        checkState(AkType.BOOL);
        key.append(value);
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
        return key().toString();
    }

    // for use by this class

    protected final Key key() {
        return key;
    }
    
    // private methods

    private void checkState(AkType type) {
        ValueSourceHelper.checkType(this.type, type);
    }

    private void invalidate() {
        type = AkType.UNSUPPORTED;
    }

    // object state

    private Key key;
    private AkType type = AkType.UNSUPPORTED;
}
