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

package com.akiban.qp.util;

import com.akiban.server.collation.AkCollator;
import com.akiban.server.types.ValueSource;
import com.akiban.util.ByteSource;

public class ValueSourceHasher
{
    public static int hash(ValueSource valueSource, AkCollator collator)
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
                // hash = valueSource.getString().hashCode();
                hash = valueSource.hash(collator);
                break;
            case TEXT:
                // hash = valueSource.getText().hashCode();
                hash = valueSource.hash(collator);
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
