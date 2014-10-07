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

package com.foundationdb.server.types.value;

import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.aksql.aktypes.AkGUID;
import com.foundationdb.server.types.common.BigDecimalWrapperImpl;
import com.foundationdb.server.types.common.types.StringFactory;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MBinary;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.util.ByteSource;
import com.foundationdb.util.WrappingByteSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

public final class ValueSources {

    private static final Logger logger = LoggerFactory.getLogger(ValueSources.class);

    public static TClass tClass(ValueSource source) {
        return TInstance.tClass(source.getType());
    }

    public static UnderlyingType underlyingType(ValueSource source) {
        return TInstance.underlyingType(source.getType());
    }

    /**
     * Gets a long from one of the signed int methods on source. The source instance must have a non-null raw value
     * of one of the signed INT types.
     * @param source the source to extract a long from
     * @return the source's value as a long
     */
    public static long getLong(ValueSource source) {
        switch (underlyingType(source)) {
        case INT_8:
            return source.getInt8();
        case INT_16:
            return source.getInt16();
        case INT_32:
            return source.getInt32();
        case INT_64:
            return source.getInt64();
        default:
            throw new UnsupportedOperationException(source.getType().toString());
        }
    }

    public static Value valuefromObject(Object object, TInstance type) {
        return valuefromObject(object, type, null);
    }

    public static Value valuefromObject(Object object, TInstance type, QueryContext queryContext) {
        Value value = new Value(type);
        if (object == null) {
            value.putNull();
        }
        else if (object instanceof String) {
            // This is the common case, so let's test for it first
            if (TInstance.underlyingType(type) == UnderlyingType.STRING)
                value.putString((String)object, null);
            else if (type == null) {
                type = MString.VARCHAR.instance(
                        ((String)object).length(), StringFactory.DEFAULT_CHARSET.ordinal(), StringFactory.NULL_COLLATION_ID, false);
                value = new Value(type, (String)object);
            }
        }
        else if (type == null) {
            value = fromObject(object);
        }
        else {
            switch (TInstance.underlyingType(type)) {
            case INT_8:
            case INT_16:
            case UINT_16:
            case INT_32:
            case INT_64:
                if (object instanceof Number)
                    valueFromLong(((Number)object).longValue(), value);
                break;
            case FLOAT:
                if (object instanceof Number)
                    value.putFloat(((Number)object).floatValue());
                break;
            case DOUBLE:
                if (object instanceof Number)
                    value.putDouble(((Number)object).doubleValue());
                break;
            case BYTES:
                if (object instanceof byte[])
                    value.putBytes((byte[])object);
                else if (object instanceof ByteSource)
                    value.putBytes(((ByteSource)object).toByteSubarray());
                break;
            case STRING:
                if(!(object instanceof CharSequence || object instanceof Number)) {
                    throw new IllegalArgumentException("Unsafe toString(): " + object.getClass());
                }
                value.putString(object.toString(), null);
                break;
            case BOOL:
                if (object instanceof Boolean)
                    value.putBool((Boolean)object);
                break;
            }
        }
        if (!value.hasAnyValue()) {
            if (type == null) {
                value = fromObject(object);
            } else {
                value = convertFromObject(object, type, queryContext);
            }
        }
        return value;
    }
    
    private static Value convertFromObject (Object object, TInstance type, QueryContext queryContext) {
        Value in = fromObject(object);
        TInstance inType = in.getType();
        Value out = null;
        if (!inType.equals(type)) {
            TExecutionContext context =
                    new TExecutionContext(Collections.singletonList(in.getType()),
                                          type,
                                          queryContext);
            out = new Value(type);
            type.typeClass().fromObject(context, in, out);
        } else {
            out = in;
        }
        return out;
    }
    /**
     * Reflectively create a {@linkplain TPreptimeValue} from the given object and the {@linkplain TInstance}
     * 
     * @param object
     * @param type
     * @return
     */
    public static TPreptimeValue fromObject(Object object, TInstance type) {
        ValueSource value = valuefromObject(object, type);
        if (type == null) {
            if (value.getType() == null) {
                return new TPreptimeValue(value.getType());
            }
            return new TPreptimeValue(value);
        }
        return new TPreptimeValue (type,value);
    }
   
    private static Value fromObject(Object object) {
        final TInstance type;
        Value value = null;
           
        if (object instanceof String) {
            String s = (String) object;
            type = MString.VARCHAR.instance(
                    s.length(), StringFactory.DEFAULT_CHARSET.ordinal(), StringFactory.NULL_COLLATION_ID, false);
            value = new Value(type, s);
        }
        else if (object instanceof Long) {
            type = MNumeric.BIGINT.instance(false);
            value = new Value(type, (Long)object);
        }
        else if (object instanceof Integer) {
            type = MNumeric.INT.instance(false);
            value = new Value(type, (Integer) object);
        }
        else if (object instanceof Double) {
            type = MApproximateNumber.DOUBLE.instance(false);
            value = new Value(type, (Double) object);
        }
        else if (object instanceof Float) {
            type = MApproximateNumber.FLOAT.instance(false);
            value = new Value(type, (Float)object);
        }
        else if (object instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) object;
            int precision = bd.precision();
            int scale = bd.scale();
            if (precision < scale) {
                // BigDecimal interprets something like "0.01" as having a scale of 2 and precision of 1.
                precision = scale;
            }
            type = MNumeric.DECIMAL.instance(precision, scale, false);
            value = new Value(type);
            value.putObject(new BigDecimalWrapperImpl(bd));
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
            type = MBinary.VARBINARY.instance(bytes.length, false);
            value = new Value(type, bytes);
        }
        else if (object instanceof BigInteger) {
            type = MNumeric.BIGINT_UNSIGNED.instance(false);
            BigInteger bi = (BigInteger) object;
            value = new Value(type, bi.longValue());
        }
        else if (object instanceof Boolean) {
            type = AkBool.INSTANCE.instance(false);
            value = new Value(type, (Boolean)object);
        }
        else if (object instanceof Character) {
            type = MString.VARCHAR.instance(1, false);
            value = new Value(type, object.toString());
        }
        else if (object instanceof Short) {
            type = MNumeric.SMALLINT.instance(false);
            value = new Value(type, (Short)object);
        }
        else if (object instanceof Byte) {
            type = MNumeric.TINYINT.instance(false);
            value = new Value(type, (Byte)object);
        }
        else {
            throw new UnsupportedOperationException("can't convert " + object + " of type " + object.getClass());
        }

        return value;
    }

    public static void valueFromLong(long value, Value result) {
        UnderlyingType underlying = underlyingType(result);
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

    public static Object toObject(ValueSource source) {
        if (source == null || source.isNull()) {
            return null;
        }
        if (source.getType().typeClass() == MNumeric.DECIMAL ||
            source.getType().typeClass() == MNumeric.DECIMAL_UNSIGNED) {
            if (source.getObject() instanceof BigDecimalWrapperImpl) {
                return ((BigDecimalWrapperImpl)source.getObject()).asBigDecimal();
            }
            logger.error("MDecimal with underlying object of : {}", source.getObject().getClass());
        }
        
        if (source.getType().typeClass() == AkGUID.INSTANCE.widestComparable()) {
            if (source.getObject() instanceof UUID) {
                return (UUID) source.getObject();
            }
            logger.error("GUID with underlying object of : {}", source.getObject().getClass());
        }
        
        if (source.getType().typeClass() == MBinary.LONGBLOB ||
             source.getType().typeClass() == MBinary.BLOB ||
             source.getType().typeClass() == MBinary.MEDIUMBLOB ||
             source.getType().typeClass() == MBinary.TINYBLOB ||
             source.getType().typeClass() == MBinary.VARBINARY) {
            return new WrappingByteSource(source.getBytes());
        }
        
        UnderlyingType underlying = underlyingType(source);
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

    public static boolean areEqual(ValueSource one, ValueSource two) {
        TInstance oneType = one.getType();
        TInstance twoType = two.getType();
        if (oneType == null || twoType == null)
            return oneType == null && twoType == null;
        if (!oneType.equalsExcludingNullable(twoType))
            return false;
        if (one.isNull())
            return two.isNull();
        if (two.isNull())
            return false;
        if (one.hasCacheValue() && two.hasCacheValue())
            return one.getObject().equals(two.getObject());
        switch (TInstance.underlyingType(oneType)) {
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
            throw new AssertionError(String.valueOf(oneType));
        }
    }

    public static int hash(ValueSource source, AkCollator collator) {
        if (source.isNull())
            return 0;
        final long hash;
        switch (underlyingType(source)) {
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
            hash = AkCollator.hashValue(source, collator);
            break;
        default:
            throw new AssertionError(source.getType());
        }
        return ((int) (hash >> 32)) ^ (int) hash;
    }

    public static ValueSource getNullSource(TInstance underlying) {
        Value result = new Value(underlying);
        result.putNull();
        return result;
    }

    private ValueSources() {}

    public static void toStringSimple(ValueSource source, StringBuilder out) {
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
        
        switch (underlyingType(source)) {
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
            logger.warn("unknown ValueSource underlying type: {} ({})", source.getType(), source);
            out.append("<?>");
            break;
        }
    }

    public static String toStringSimple(ValueSource source) {
        StringBuilder sb = new StringBuilder();
        toStringSimple(source, sb);
        return sb.toString();
    }

}
