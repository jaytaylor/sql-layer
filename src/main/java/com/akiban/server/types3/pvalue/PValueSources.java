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

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.common.types.StringFactory;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MBigDecimalWrapper;
import com.akiban.server.types3.mcompat.mtypes.MBinary;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.util.ByteSource;
import com.akiban.util.WrappingByteSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;

public final class PValueSources {

    private static final Logger logger = LoggerFactory.getLogger(PValueSources.class);

    /**
     * Converts AkType to TInstance. Doesn't actually have to do with PValueSources, but this is a convenient place
     * to put it.
     * @param akType the input type
     * @return its TInstance equivalent, ish.
     */
    public static TInstance fromAkType(AkType akType) {
        return fromAkType(akType, 32);
    }
    /**
     * Converts AkType to TInstance. Doesn't actually have to do with PValueSources, but this is a convenient place
     * to put it.
     * @param akType the input type
     * @param defaultStringLen the default length for character or binary strings
     * @return its TInstance equivalent, ish.
     */
    public static TInstance fromAkType(AkType akType, int defaultStringLen) {
        TInstance tInstance = null;
        TClass tClass = null;
        switch (akType) {
        case DATE:
            tClass = MDatetimes.DATE;
            break;
        case DATETIME:
            tClass = MDatetimes.DATETIME;
            break;
        case DECIMAL:
            tClass = MNumeric.DECIMAL;
            break;
        case DOUBLE:
            tClass = MApproximateNumber.DOUBLE;
            break;
        case FLOAT:
            tClass = MApproximateNumber.FLOAT;
            break;
        case INT:
            tClass = MNumeric.INT;
            break;
        case LONG:
            tClass = MNumeric.BIGINT;
            break;
        case VARCHAR:
            tInstance = MString.VARCHAR.instance(defaultStringLen);
            break;
        case TEXT:
            tClass = MString.TEXT;
            break;
        case TIME:
            tClass = MDatetimes.TIME;
            break;
        case TIMESTAMP:
            tClass = MDatetimes.TIMESTAMP;
            break;
        case U_BIGINT:
            tClass = MNumeric.BIGINT_UNSIGNED;
            break;
        case U_DOUBLE:
            tClass = MApproximateNumber.DOUBLE_UNSIGNED;
            break;
        case U_FLOAT:
            tClass = MApproximateNumber.FLOAT_UNSIGNED;
            break;
        case U_INT:
            tClass = MNumeric.INT_UNSIGNED;
            break;
        case VARBINARY:
            tInstance = MBinary.VARBINARY.instance(defaultStringLen);
            break;
        case YEAR:
            tClass = MDatetimes.YEAR;
            break;
        case BOOL:
            tInstance = MNumeric.TINYINT.instance(1);
            break;
        case NULL:
            tInstance = MBinary.VARBINARY.instance(0);
            break;
        case INTERVAL_MONTH:
        case RESULT_SET:
        case UNSUPPORTED:
            throw new UnsupportedOperationException("can't infer type of null object: " + akType);
        default:
            throw new AssertionError(akType);
        case INTERVAL_MILLIS:
            break;
        }
        return (tClass == null) ? tInstance : tClass.instance();
    }

    /**
     * Reflectively creates a {@linkplain TPreptimeValue} from the given object, optionally consulting the given
     * {@linkplain AkType} if that object is a <tt>Long</tt>. Most classes are fairly unambiguous as to what sort of
     * TClass and TInstance they provide, but Longs can represent various type, so the <tt>AkType</tt> is used to
     * a disambiguate. If it is <tt>null</tt>, the type class is assumed to be <tt>MCOMPAT_ BIGINT</tt>.
     * @param object the object to convert into a TPreptimeValue
     * @param akType the object's associated AkType, which only matters if the object is a Long
     * @return the Object as a TPreptimeValue
     */
    public static TPreptimeValue fromObject(Object object, AkType akType) {
        final PValue value;
        final TInstance tInstance;
        if (object == null) {
            if (akType == null)
                throw new UnsupportedOperationException("can't infer type of null object");
            tInstance = fromAkType(akType, 0);
            value = new PValue(tInstance.typeClass().underlyingType());
            value.putNull();
        }
        else if (object instanceof Integer) {
            tInstance = MNumeric.INT.instance();
            value = new PValue((Integer)object);
        }
        else if (object instanceof Long) {
            if (akType == null) {
                tInstance = MNumeric.BIGINT.instance();
            }
            else {
                TClass tClass;
                switch (akType) {
                case DATE:
                    tClass = MDatetimes.DATE;
                    break;
                case DATETIME:
                    tClass = MDatetimes.DATETIME;
                    break;
                case INT:
                    tClass = MNumeric.INT;
                    break;
                case LONG:
                    tClass = MNumeric.BIGINT;
                    break;
                case TIME:
                    tClass = MDatetimes.TIME;
                    break;
                case TIMESTAMP:
                    tClass = MDatetimes.TIMESTAMP;
                    break;
                case U_BIGINT:
                    tClass = MNumeric.BIGINT_UNSIGNED;
                    break;
                case U_INT:
                    tClass = MNumeric.INT_UNSIGNED;
                    break;
                case YEAR:
                    tClass = MDatetimes.YEAR;
                    break;
                case INTERVAL_MILLIS:
                case INTERVAL_MONTH:
                    throw new UnsupportedOperationException("interval literals not supported");
                default:
                    throw new IllegalArgumentException("can't convert longs of AkType " + akType);
                }
                tInstance = tClass.instance();
            }
            value = new PValue(tInstance.typeClass().underlyingType());
            pvalueFromLong((Long)object, value);
        }
        else if (object instanceof String) {
            String s = (String) object;
            tInstance = MString.VARCHAR.instance(s.length(), StringFactory.DEFAULT_CHARSET.ordinal(), -1);
            value = new PValue(s);
        }
        else if (object instanceof Double) {
            tInstance = MApproximateNumber.DOUBLE.instance();
            value = new PValue((Double)object);
        }
        else if (object instanceof Float) {
            tInstance = MApproximateNumber.FLOAT.instance();
            value = new PValue((Float)object);
        }
        else if (object instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) object;
            int precision = bd.precision();
            int scale = bd.scale();
            if (precision < scale) {
                // BigDecimal interprets something like "0.01" as having a scale of 2 and precision of 1.
                precision = scale;
            }
            tInstance = MNumeric.DECIMAL.instance(precision, scale);
            value = new PValue(PUnderlying.BYTES);
            value.putObject(new MBigDecimalWrapper(bd));
        }
        else if (object instanceof ByteSource || object instanceof byte[]) {
            byte[] bytes;
            if (object instanceof byte[]) {
                bytes = (byte[])object;
            }
            else {
                ByteSource source = (ByteSource) object;
                byte[] srcArray = source.byteArray();
                int offset = source.byteArrayOffset();
                int end = offset + source.byteArrayLength();
                bytes = Arrays.copyOfRange(srcArray, offset, end);
            }
            tInstance = MBinary.VARBINARY.instance(bytes.length);
            value = new PValue(PUnderlying.BYTES);
            value.putBytes(bytes);
        }
        else if (object instanceof BigInteger) {
            tInstance = MNumeric.BIGINT_UNSIGNED.instance();
            BigInteger bi = (BigInteger) object;
            value = new PValue(bi.longValue());
        }
        else if (object instanceof Boolean) {
            tInstance = AkBool.INSTANCE.instance();
            value = new PValue((Boolean)object);
        }
        else if (object instanceof Character) {
            tInstance = MString.VARCHAR.instance(1);
            value = new PValue(object.toString());
        }
        else if (object instanceof Short) {
            tInstance = MNumeric.SMALLINT.instance();
            value = new PValue((Short)object);
        }
        else {
            throw new UnsupportedOperationException("can't convert " + object + " of type " + object.getClass());
        }

        return new TPreptimeValue(tInstance, value);
    }

    public static void pvalueFromLong(long value, PValue result) {
        PUnderlying underlying = result.getUnderlyingType();
        switch (underlying) {
        case INT_8:
            result.putInt8((byte)value);
            break;
        case INT_16:
            result.putInt16((short)value);
            break;
        case UINT_16:
            result.putUInt16((char)value);
            break;
        case INT_32:
            result.putInt32((int)value);
            break;
        case INT_64:
            result.putInt64(value);
            break;
        default:
            throw new AssertionError(underlying);
        }
    }

    public static Object toObject(PValueSource valueSource, AkType akType) {
        if (valueSource.isNull())
            return null;

        switch (akType.underlyingType()) {
        case BOOLEAN_AKTYPE:
            return valueSource.getBoolean();
        case LONG_AKTYPE:
            long v;
            switch (valueSource.getUnderlyingType()) {
            case INT_8:
                v = valueSource.getInt8();
                break;
            case INT_16:
                v = valueSource.getInt16();
                break;
            case UINT_16:
                v = valueSource.getUInt16();
                break;
            case INT_32:
                v = valueSource.getInt32();
                break;
            case INT_64:
                v = valueSource.getInt64();
                break;
            case BOOL:
            case FLOAT:
            case DOUBLE:
            case BYTES:
            case STRING:
                v = Long.parseLong(valueSource.getString());
                break;
            default:
                throw new AssertionError(valueSource.getUnderlyingType());
            }
            return v;
        case FLOAT_AKTYPE:
            return valueSource.getUnderlyingType() == PUnderlying.STRING
                    ? Float.parseFloat(valueSource.getString())
                    : valueSource.getFloat();
        case DOUBLE_AKTYPE:
            return valueSource.getUnderlyingType() == PUnderlying.STRING
                    ? Double.parseDouble(valueSource.getString())
                    : valueSource.getDouble();
        case OBJECT_AKTYPE:
            if (valueSource.getUnderlyingType() == PUnderlying.STRING)
                return valueSource.getString();
            if (akType == AkType.VARBINARY)
                return new WrappingByteSource(valueSource.getBytes());
            if (valueSource.hasCacheValue())
                return valueSource.getObject();
            return toObject(valueSource);
        default:
            throw new AssertionError(akType + " with underlying " + akType.underlyingType());
        }
    }

    private static Object toObject(PValueSource source) {
        PUnderlying underlying = source.getUnderlyingType();
        switch (underlying) {
        case BOOL:      return source.getBoolean();
        case INT_8:     return source.getInt8();
        case INT_16:    return source.getInt16();
        case UINT_16:   return source.getUInt16();
        case INT_32:    return source.getInt32();
        case INT_64:    return source.getInt64();
        case FLOAT:     return source.getFloat();
        case DOUBLE:    return source.getDouble();
        case BYTES:     return source.getBytes();
        case STRING:    return source.getString();
        default:        throw new AssertionError(underlying);
        }
    }

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
        case STRING:
            return one.getString().equals(two.getString());
        case BYTES:
            return Arrays.equals(one.getBytes(), two.getBytes());
        default:
            throw new AssertionError(underlyingType);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T getCached(PValueSource source, TInstance tInstance, PValueCacher<? extends T> cacher) {
        if (source.hasCacheValue())
            return (T) source.getObject();
        return cacher.valueToCache(source, tInstance);
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
        case STRING:
            hash = source.getString().hashCode();
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

    public static PValueSource fromValueSource(ValueSource source, TInstance tInstance) {
        PValue result = new PValue(tInstance.typeClass().underlyingType());
        plainConverter.convert(null, source, result, tInstance);
        return result;
    }

    public static void toStringSimple(PValueSource source, StringBuilder out) {
        if (source.isNull()) {
            out.append("NULL");
            return;
        }
        if (!source.hasAnyValue()) {
            out.append("<unset>");
            return;
        }
        if (source.hasCacheValue()) {
            out.append(source.getObject());
            return;
        }
        
        switch (source.getUnderlyingType()) {
        case BOOL:
            out.append(source.getBoolean());
            break;
        case INT_8:
            out.append(source.getInt8());
            break;
        case INT_16:
            out.append(source.getInt16());
            break;
        case UINT_16:
            // display as int instead of char, to reinforce that it's not a char, it's an int with unsigned collation
            out.append((int)source.getUInt16());
            break;
        case INT_32:
            out.append(source.getInt32());
            break;
        case INT_64:
            out.append(source.getInt64());
            break;
        case FLOAT:
            out.append(source.getFloat());
            break;
        case DOUBLE:
            out.append(source.getDouble());
            break;
        case BYTES:
            out.append(Arrays.toString(source.getBytes()));
            break;
        case STRING:
            out.append(source.getString());
            break;
        default:
            // toStrings are non-critical, so let's not throw an error
            logger.warn("unknown PValueSource underlying type: {} ({})", source.getUnderlyingType(), source);
            out.append("<?>");
            break;
        }
    }

    public static String toStringSimple(PValueSource source) {
        StringBuilder sb = new StringBuilder();
        toStringSimple(source, sb);
        return sb.toString();
    }

    public static abstract class ValueSourceConverter<T> {

        protected abstract Object handleBigDecimal(T state, BigDecimal bigDecimal);
        protected abstract Object handleString(T state, String string);
        protected abstract ValueSource tweakSource(T state, ValueSource source);

        public final void convert(T state, ValueSource in, PValueTarget out, TInstance tInstance) {
            if (in.isNull())
                out.putNull();

            long lval = 0;
            float fval = 0;
            double dval = 0;
            Object oval = UNDEF;
            boolean boolval = false;

            in = tweakSource(state, in);

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
            case NULL:
                oval = null;
                break;
            default:
                throw new AssertionError(in.getConversionType());
            }

            if (oval == null) {
                out.putNull();
            }
            else if (oval != UNDEF && oval.getClass() != byte[].class) {
                if (oval instanceof String) {
                    String sval = (String) oval;
                    if (out.getUnderlyingType() == PUnderlying.STRING) {
                        out.putString(sval, null);
                    }
                    else {
                        TClass tClass = tInstance.typeClass();
                        PValue sValue = new PValue(sval);
                        TExecutionContext forErrors = new TExecutionContext(
                                null,
                                Collections.singletonList(tInstance),
                                tInstance,
                                null, null, null, null
                        );
                        tClass.fromObject(forErrors, sValue, out);
                    }
                }
                else {
                    out.putObject(oval);
                }
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
                case STRING:
                    out.putString((String)(oval), null);
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

        @Override
        protected ValueSource tweakSource(Void state, ValueSource source) {
            return source;
        }
    };
}
