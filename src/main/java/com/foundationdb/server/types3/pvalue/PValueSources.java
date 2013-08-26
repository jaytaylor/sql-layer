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

package com.foundationdb.server.types3.pvalue;

import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types3.TClass;
import com.foundationdb.server.types3.TExecutionContext;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.TPreptimeValue;
import com.foundationdb.server.types3.aksql.aktypes.AkBool;
import com.foundationdb.server.types3.common.types.StringFactory;
import com.foundationdb.server.types3.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types3.mcompat.mtypes.MBigDecimalWrapper;
import com.foundationdb.server.types3.mcompat.mtypes.MBinary;
import com.foundationdb.server.types3.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types3.mcompat.mtypes.MString;
import com.foundationdb.util.ByteSource;
import com.foundationdb.util.WrappingByteSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;

public final class PValueSources {

    private static final Logger logger = LoggerFactory.getLogger(PValueSources.class);

    public static TClass tClass(PValueSource source) {
        return TInstance.tClass(source.tInstance());
    }

    public static PUnderlying pUnderlying(PValueSource source) {
        return TInstance.pUnderlying(source.tInstance());
    }

    /**
     * Gets a long from one of the signed int methods on source. The source instance must have a non-null raw value
     * of one of the signed INT types.
     * @param source the source to extract a long from
     * @return the source's value as a long
     */
    public static long getLong(PValueSource source) {
        switch (pUnderlying(source)) {
        case INT_8:
            return source.getInt8();
        case INT_16:
            return source.getInt16();
        case INT_32:
            return source.getInt32();
        case INT_64:
            return source.getInt64();
        default:
            throw new UnsupportedOperationException(source.tInstance().toString());
        }
    }

    public static PValue pValuefromObject (Object object, TInstance tInstance) {
        PValue pValue = new PValue(tInstance);
        if (object == null) {
            pValue.putNull();
        }
        else if (object instanceof String) {
            // This is the common case, so let's test for it first
            if (TInstance.pUnderlying(tInstance) == PUnderlying.STRING)
                pValue.putString((String)object, null);
            else if (tInstance == null) {
                tInstance = MString.VARCHAR.instance(
                        ((String)object).length(), StringFactory.DEFAULT_CHARSET.ordinal(), StringFactory.NULL_COLLATION_ID, false);
                pValue = new PValue(tInstance, (String)object);
            }
        }
        else if (tInstance == null) {
            pValue = fromObject(object);
        }
        else {
            switch (TInstance.pUnderlying(tInstance)) {
            case INT_8:
            case INT_16:
            case UINT_16:
            case INT_32:
            case INT_64:
                if (object instanceof Number)
                    pvalueFromLong(((Number)object).longValue(), pValue);
                break;
            case FLOAT:
                if (object instanceof Number)
                    pValue.putFloat(((Number)object).floatValue());
                break;
            case DOUBLE:
                if (object instanceof Number)
                    pValue.putDouble(((Number)object).doubleValue());
                break;
            case BYTES:
                if (object instanceof byte[])
                    pValue.putBytes((byte[])object);
                else if (object instanceof ByteSource)
                    pValue.putBytes(((ByteSource)object).toByteSubarray());
                break;
            case STRING:
                pValue.putString(object.toString(), null);
                break;
            case BOOL:
                if (object instanceof Boolean)
                    pValue.putBool((Boolean)object);
                break;
            }
        }
        if (!pValue.hasAnyValue()) {
            if (tInstance == null) {
                pValue = fromObject(object);
            } else {
                pValue = convertFromObject(object, tInstance);
            }
        }
        return pValue;
    }
    
    private static PValue convertFromObject (Object object, TInstance tInstance) {
        PValue in = fromObject(object);
        TInstance inTInstance = in.tInstance();
        PValue out = null;
        if (!inTInstance.equals(tInstance)) {
            TExecutionContext context =
                    new TExecutionContext(Collections.singletonList(in.tInstance()),
                                tInstance,
                                null);
            out = new PValue (tInstance);
            tInstance.typeClass().fromObject(context, in, out);
        } else {
            out = in;
        }
        return out;
    }
    /**
     * Reflectively create a {@linkplain TPreptimeValue} from the given object and the {@linkplain TInstance}
     * 
     * @param object
     * @param tInstance
     * @return
     */
    public static TPreptimeValue fromObject(Object object, TInstance tInstance) {
        PValueSource pvalue = pValuefromObject(object, tInstance);
        if (tInstance == null) {
            if (pvalue.tInstance() == null) {
                return new TPreptimeValue(pvalue.tInstance());
            }
            return new TPreptimeValue(pvalue.tInstance(), pvalue);
        }
        return new TPreptimeValue (tInstance,pvalue);
    }
   
    private static PValue fromObject(Object object) {
        final TInstance tInstance;
        PValue value = null;
           
        if (object instanceof String) {
            String s = (String) object;
            tInstance = MString.VARCHAR.instance(
                    s.length(), StringFactory.DEFAULT_CHARSET.ordinal(), StringFactory.NULL_COLLATION_ID, false);
            value = new PValue(tInstance, s);
        }
        else if (object instanceof Long) {
            tInstance = MNumeric.BIGINT.instance(false);
            value = new PValue (tInstance, (Long)object);
        }
        else if (object instanceof Integer) {
            tInstance = MNumeric.INT.instance(false);
            value = new PValue(tInstance, (Integer) object);
        }
        else if (object instanceof Double) {
            tInstance = MApproximateNumber.DOUBLE.instance(false);
            value = new PValue(tInstance, (Double) object);
        }
        else if (object instanceof Float) {
            tInstance = MApproximateNumber.FLOAT.instance(false);
            value = new PValue(tInstance, (Float)object);
        }
        else if (object instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) object;
            int precision = bd.precision();
            int scale = bd.scale();
            if (precision < scale) {
                // BigDecimal interprets something like "0.01" as having a scale of 2 and precision of 1.
                precision = scale;
            }
            tInstance = MNumeric.DECIMAL.instance(precision, scale, false);
            value = new PValue(tInstance);
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
            tInstance = MBinary.VARBINARY.instance(bytes.length, false);
            value = new PValue(tInstance, bytes);
        }
        else if (object instanceof BigInteger) {
            tInstance = MNumeric.BIGINT_UNSIGNED.instance(false);
            BigInteger bi = (BigInteger) object;
            value = new PValue(tInstance, bi.longValue());
        }
        else if (object instanceof Boolean) {
            tInstance = AkBool.INSTANCE.instance(false);
            value = new PValue(tInstance, (Boolean)object);
        }
        else if (object instanceof Character) {
            tInstance = MString.VARCHAR.instance(1, false);
            value = new PValue(tInstance, object.toString());
        }
        else if (object instanceof Short) {
            tInstance = MNumeric.SMALLINT.instance(false);
            value = new PValue(tInstance, (Short)object);
        }
        else if (object instanceof Byte) {
            tInstance = MNumeric.TINYINT.instance(false);
            value = new PValue(tInstance, (Byte)object);
        }
        else {
            throw new UnsupportedOperationException("can't convert " + object + " of type " + object.getClass());
        }

        return value;
    }

    public static void pvalueFromLong(long value, PValue result) {
        PUnderlying underlying = pUnderlying(result);
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
/*            
        case BYTES:
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putLong(value);
            result.putBytes(buffer.array()); 
            break;
        case STRING:
            result.putString(Long.toString(value), null);
            break;
        case DOUBLE:
            result.putDouble(Long.valueOf(value).doubleValue());
            break;
        case FLOAT:
            result.putFloat(Long.valueOf(value).floatValue());
            break;
*/            
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
            switch (pUnderlying(valueSource)) {
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
                throw new AssertionError(valueSource.tInstance());
            }
            return v;
        case FLOAT_AKTYPE:
            return pUnderlying(valueSource) == PUnderlying.STRING
                    ? Float.parseFloat(valueSource.getString())
                    : valueSource.getFloat();
        case DOUBLE_AKTYPE:
            return pUnderlying(valueSource) == PUnderlying.STRING
                    ? Double.parseDouble(valueSource.getString())
                    : valueSource.getDouble();
        case OBJECT_AKTYPE:
            if (pUnderlying(valueSource) == PUnderlying.STRING)
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

    public static Object toObject(PValueSource source) {
        if (source == null || source.isNull()) {
            return null;
        }
        if (source.tInstance().typeClass() == MNumeric.DECIMAL ||
            source.tInstance().typeClass() == MNumeric.DECIMAL_UNSIGNED) {
            if (source.getObject() instanceof MBigDecimalWrapper) {
                return ((MBigDecimalWrapper)source.getObject()).asBigDecimal();
            }
            logger.error("MDecimal with underlying object of : {}", source.getObject().getClass());
        }
        if (source.tInstance().typeClass() == MBinary.LONGBLOB ||
             source.tInstance().typeClass() == MBinary.BLOB ||
             source.tInstance().typeClass() == MBinary.MEDIUMBLOB ||
             source.tInstance().typeClass() == MBinary.TINYBLOB ||
             source.tInstance().typeClass() == MBinary.VARBINARY) {
            return new WrappingByteSource(source.getBytes());
        }
        
        PUnderlying underlying = pUnderlying(source);
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

    public static boolean areEqual(PValueSource one, PValueSource two, TInstance instance) {
        TInstance oneTInstance = one.tInstance();
        TInstance twoTInstance = two.tInstance();
        if (oneTInstance == null || twoTInstance == null)
            return oneTInstance == null && twoTInstance == null;
        if (!oneTInstance.equalsExcludingNullable(twoTInstance))
            return false;
        if (one.isNull())
            return two.isNull();
        if (two.isNull())
            return false;
        if (one.hasCacheValue() && two.hasCacheValue())
            return one.getObject().equals(two.getObject());
        switch (TInstance.pUnderlying(oneTInstance)) {
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
            throw new AssertionError(String.valueOf(oneTInstance));
        }
    }

    public static int hash(PValueSource source, AkCollator collator) {
        if (source.isNull())
            return 0;
        final long hash;
        switch (pUnderlying(source)) {
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
            String stringVal;
            if (source.canGetRawValue())
                stringVal = source.getString();
            else if (source.hasCacheValue())
                stringVal = (String) source.getObject();
            else
                throw new AssertionError("no value to hash from");
            hash = collator.hashCode(stringVal);
            break;
        default:
            throw new AssertionError(source.tInstance());
        }
        return ((int) (hash >> 32)) ^ (int) hash;
    }

    public static PValueSource getNullSource(TInstance underlying) {
        PValue result = new PValue(underlying);
        result.putNull();
        return result;
    }

    private PValueSources() {}

    public static PValueSource fromValueSource(ValueSource source, TInstance tInstance) {
        PValue result = new PValue(tInstance);
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
        
        switch (pUnderlying(source)) {
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
            logger.warn("unknown PValueSource underlying type: {} ({})", source.tInstance(), source);
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
                    if (PValueTargets.pUnderlying(out) == PUnderlying.STRING) {
                        out.putString(sval, null);
                    }
                    else {
                        PValue sValue = new PValue(tInstance, sval);
                        TExecutionContext forErrors = new TExecutionContext(
                                null,
                                Collections.singletonList(tInstance),
                                tInstance,
                                null, null, null, null
                        );
                        tInstance.typeClass().fromObject(forErrors, sValue, out);
                    }
                }
                else {
                    out.putObject(oval);
                }
            }
            else {
                switch (PValueTargets.pUnderlying(out)) {
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
                    throw new AssertionError(out.tInstance());
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
