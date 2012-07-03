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

package com.akiban.server.types3.pvalue;

import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.mcompat.mtypes.MBigDecimalWrapper;
import com.akiban.util.ByteSource;

import java.math.BigDecimal;
import java.util.Arrays;

public final class PValueSources {
    
    public static boolean areEqual(PValueSource one, PValueSource two) {
        PUnderlying underlyingType = one.getUnderlyingType();
        if (underlyingType != two.getUnderlyingType())
            return false;
        if (one.isNull())
            return two.isNull();
        if (two.isNull())
            return false;
        switch (underlyingType) {
        case BOOL:
            return one.getBoolean() == two.getBoolean();
        case INT_8:
            return one.getInt8() == two.getInt8();
        case INT_16:
            return one.getInt16() == two.getInt16();
        case UINT_16:
            return one.getInt16() == two.getInt16();
        case INT_32:
            return one.getInt32() == two.getInt32();
        case INT_64:
            return one.getInt64() == two.getInt64();
        case FLOAT:
            return one.getFloat() == two.getFloat();
        case DOUBLE:
            return one.getDouble() == two.getDouble();
        case BYTES:
            return Arrays.equals(one.getBytes(), two.getBytes());
        default:
            throw new AssertionError(underlyingType);
        }
    }

    public static int hash(PValueSource source) {
        if (source.isNull())
            return 0;
        final long hash;
        switch (source.getUnderlyingType()) {
        case BOOL:
            hash = source.getBoolean() ? 1 : 0;
            break;
        case INT_8:
            hash = source.getInt8();
            break;
        case INT_16:
            hash = source.getInt16();
            break;
        case UINT_16:
            hash = source.getUInt16();
            break;
        case INT_32:
            hash = source.getInt32();
            break;
        case INT_64:
            hash = source.getInt64();
            break;
        case FLOAT:
            hash = Float.floatToRawIntBits(source.getFloat());
            break;
        case DOUBLE:
            hash = Double.doubleToRawLongBits(source.getDouble());
            break;
        case BYTES:
            hash = Arrays.hashCode(source.getBytes());
            break;
        default:
            throw new AssertionError(source.getUnderlyingType());
        }
        return ((int) (hash >> 32)) ^ (int) hash;
    }

    public static PValueSource getNullSource(PUnderlying underlying) {
        PValueSource source = NULL_SOURCES[underlying.ordinal()];
        assert source.isNull() : source;
        return source;
    }

    private PValueSources() {}

    private static final PValueSource[] NULL_SOURCES = createNullSources();

    private static PValueSource[] createNullSources() {
        PUnderlying[] vals = PUnderlying.values();
        PValueSource[] arr = new PValueSource[vals.length];
        for (int i = 0; i < vals.length; ++i) {
            PValue pval = new PValue(vals[i]);
            pval.putNull();
            arr[i] = pval;
        }
        return arr;
    }

    public static PValueSource fromValueSource(ValueSource source, PUnderlying pUnderlying) {
        PValue result = new PValue(pUnderlying);
        plainConverter.convert(null, source, result);
        return result;
    }

    public static abstract class ValueSourceConverter<T> {

        protected abstract Object handleBigDecimal(T state, BigDecimal bigDecimal);
        protected abstract Object handleString(T state, String string);

        public final void convert(T state, ValueSource in, PValueTarget out) {
            if (in.isNull())
                out.putNull();

            long lval = 0;
            float fval = 0;
            double dval = 0;
            Object oval = UNDEF;
            boolean boolval = false;

            switch (in.getConversionType()) {
            case DATE:
                lval = in.getDate();
                break;
            case DATETIME:
                lval = in.getDateTime();
                break;
            case DECIMAL:
                oval = handleBigDecimal(state, in.getDecimal());
                break;
            case DOUBLE:
                dval = in.getDouble();
                break;
            case FLOAT:
                fval = in.getFloat();
                break;
            case INT:
                lval = in.getInt();
                break;
            case LONG:
                lval = in.getLong();
                break;
            case VARCHAR:
                oval = handleString(state, in.getString());
                break;
            case TEXT:
                oval = handleString(state, in.getText());
                break;
            case TIME:
                lval = in.getTime();
                break;
            case TIMESTAMP:
                lval = in.getTimestamp();
                break;
            case U_BIGINT:
                lval = in.getUBigInt().longValue();
                break;
            case U_DOUBLE:
                dval = in.getUDouble();
                break;
            case U_FLOAT:
                fval = in.getUFloat();
                break;
            case U_INT:
                lval = in.getUInt();
                break;
            case VARBINARY:
                ByteSource bs = in.getVarBinary();
                byte[] bval = new byte[bs.byteArrayLength()];
                System.arraycopy(bs.byteArray(), bs.byteArrayOffset(), bval, 0, bs.byteArrayLength());
                oval = bval;
                break;
            case YEAR:
                lval = in.getYear();
                break;
            case BOOL:
                boolval = in.getBool();
                break;
            case INTERVAL_MILLIS:
                lval = in.getInterval_Millis();
                break;
            case INTERVAL_MONTH:
                lval = in.getInterval_Month();
                break;
            default:
                throw new AssertionError(in.getConversionType());
            }

            if (oval != UNDEF && oval.getClass() != byte[].class) {
                out.putObject(oval);
            }
            else {
                switch (out.getUnderlyingType()) {
                case BOOL:
                    out.putBool(boolval);
                    break;
                case INT_8:
                    out.putInt8((byte)lval);
                    break;
                case INT_16:
                    out.putInt16((short)lval);
                    break;
                case UINT_16:
                    out.putUInt16((char)lval);
                    break;
                case INT_32:
                    out.putInt32((int)lval);
                    break;
                case INT_64:
                    out.putInt64(lval);
                    break;
                case FLOAT:
                    out.putFloat(fval);
                    break;
                case DOUBLE:
                    out.putDouble(dval);
                    break;
                case BYTES:
                    out.putBytes((byte[])oval); // ensured by "oval.getClass() != byte[].class" above
                    break;
                default:
                    throw new AssertionError(out.getUnderlyingType());
                }
            }
        }

        private static Object UNDEF = new Object();
    }

    private static final ValueSourceConverter<Void> plainConverter = new ValueSourceConverter<Void>() {
        @Override
        protected Object handleBigDecimal(Void state, BigDecimal bigDecimal) {
            return new MBigDecimalWrapper(bigDecimal);
        }

        @Override
        protected Object handleString(Void state, String string) {
            return string;
        }
    };
}
