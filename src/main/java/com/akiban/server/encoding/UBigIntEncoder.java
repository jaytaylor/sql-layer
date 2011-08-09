/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.encoding;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Type;
import com.akiban.server.AkServerUtil;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.persistit.Key;

import java.math.BigInteger;

public class UBigIntEncoder extends EncodingBase<BigInteger> {
    UBigIntEncoder() {
    }

    /**
     * Retrieve stored value from an existing RowData.
     * @param rowData RowData to take value from
     * @param offsetAndWidth value containing both offset and width of the field,
     * see {@link RowDef#fieldLocation(RowData, int)}
     * @return encoded value
     */
    private static BigInteger fromRowData(RowData rowData, long offsetAndWidth) {
        final int offset = (int)offsetAndWidth;
        final int width = (int)(offsetAndWidth >>> 32);
        return rowData.getUnsignedLongValue(offset, width);
    }

    @Override
    public Class<BigInteger> getToObjectClass() {
        return BigInteger.class;
    }
    
    public BigInteger encodeFromObject(Object obj) {
        final BigInteger value;
        if(obj == null) {
            value = null;
        } else if(obj instanceof Number || obj instanceof String) {
            value = new BigInteger(obj.toString());
        } else {
            throw new IllegalArgumentException("Requires Number or String");
        }
        return value;
    }
    
    @Override
    public boolean validate(Type type) {
        long w = type.maxSizeBytes();
        return type.fixedSize() && (w == 8);
    }

    @Override
    public BigInteger toObject(FieldDef fieldDef, RowData rowData) throws EncodingException {
        final long offsetAndWidth = getCheckedOffsetAndWidth(fieldDef, rowData);
        return fromRowData(rowData, offsetAndWidth);
    }

    @Override
    public int fromObject(FieldDef fieldDef, Object value, byte[] dest, int offset) {
        final long longValue = encodeFromObject(value).longValue();
        final int width = fieldDef.getMaxStorageSize();
        return AkServerUtil.putIntegerByWidth(dest, offset, width, longValue);
    }

    @Override
    public int widthFromObject(FieldDef fieldDef, Object value) {
        return fieldDef.getMaxStorageSize();
    }

    @Override
    public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
        final long offsetAndWidth = getOffsetAndWidth(fieldDef, rowData);
        if((int)offsetAndWidth == 0) {
            key.append(null);
        } else {
            BigInteger bigint = fromRowData(rowData, offsetAndWidth);
            key.append(bigint);
        }
    }

    @Override
    public void toKey(FieldDef fieldDef, Object value, Key key) {
        if(value == null) {
            key.append(null);
        } else {
            BigInteger bigint = encodeFromObject(value);
            key.append(bigint);
        }
    }

    /**
     * See {@link Key#appendBigInteger(BigInteger)}
     */
    @Override
    public long getMaxKeyStorageSize(Column column) {
        return (65/24) + 1;
    }
}
