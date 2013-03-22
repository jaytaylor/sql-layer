
package com.akiban.qp.util;

import com.akiban.qp.operator.StoreAdapter;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.types.ValueSource;
import com.akiban.util.ByteSource;

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
