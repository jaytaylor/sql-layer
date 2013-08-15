/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.foundationdb.qp.util;

import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.util.ByteSource;

public class ValueSourceHasher
{
    public static int hash(StoreAdapter adapter, ValueSource valueSource, AkCollator collator)
    {
        // TODO: Add hash() to ValueSource?
        if (valueSource.isNull()) {
            return 0;
        }
        long hash;
        switch (valueSource.getConversionType()) {
            case DATE:
                hash = valueSource.getDate();
                break;
            case DATETIME:
                hash = valueSource.getDateTime();
                break;
            case DECIMAL:
                hash = valueSource.getDecimal().hashCode();
                break;
            case DOUBLE:
                hash = Double.doubleToRawLongBits(valueSource.getDouble());
                break;
            case FLOAT:
                hash = Float.floatToRawIntBits(valueSource.getFloat());
                break;
            case INT:
                hash = valueSource.getInt();
                break;
            case LONG:
                hash = valueSource.getLong();
                break;
            case VARCHAR:
                hash = valueSource.hash(adapter, collator);
                break;
            case TEXT:
                hash = valueSource.hash(adapter, collator);
                break;
            case TIME:
                hash = valueSource.getTime();
                break;
            case TIMESTAMP:
                hash = valueSource.getTimestamp();
                break;
            case U_BIGINT:
                hash = valueSource.getUBigInt().hashCode();
                break;
            case U_DOUBLE:
                hash = Double.doubleToRawLongBits(valueSource.getUDouble());
                break;
            case U_FLOAT:
                hash = Float.floatToRawIntBits(valueSource.getUFloat());
                break;
            case U_INT:
                hash = valueSource.getUInt();
                break;
            case VARBINARY:
                ByteSource byteSource = valueSource.getVarBinary();
                byte[] bytes = byteSource.byteArray();
                int p = byteSource.byteArrayOffset();
                int end = p + byteSource.byteArrayLength();
                // Adapted from Arrays.hashCode(byte[])
                int h = 1;
                while (p < end) {
                    h = 31 * h + bytes[p++];
                }
                hash = h;
                break;
            case YEAR:
                hash = valueSource.getYear();
                break;
            case BOOL:
                hash = valueSource.getBool() ? 1 : 0;
                break;
            case INTERVAL_MILLIS:
                hash = valueSource.getInterval_Millis();
                break;
            case INTERVAL_MONTH:
                hash = valueSource.getInterval_Month();
                break;
            case RESULT_SET:
                assert false;
                hash = 0;
                break;
            case NULL:
                hash = 0;
                break;
            case UNSUPPORTED:
                assert false;
                hash = 0;
                break;
            default:
                assert false;
                hash = 0;
                break;
        }
        return ((int) (hash >> 32)) ^ (int) hash;
    }
}
