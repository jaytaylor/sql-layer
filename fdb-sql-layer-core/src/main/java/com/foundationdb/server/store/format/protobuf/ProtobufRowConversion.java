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

package com.foundationdb.server.store.format.protobuf;

import com.foundationdb.server.rowdata.ConversionHelperBigDecimal;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.common.BigDecimalWrapperImpl;
import com.foundationdb.server.types.common.types.DecimalAttribute;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MBinary;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.AkibanAppender;

import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

public abstract class ProtobufRowConversion
{
    /** Get the Protobuf field type. */
    public abstract Type getType();

    // TODO: Should use ValueTarget, but NewRow does not currently.
    protected abstract Object valueFromRaw(Object raw);
    protected abstract Object rawFromValue(ValueSource value);

    /** Get the field as a suitable Object. */
    public Object getValue(DynamicMessage message, FieldDescriptor field) {
        Object raw = message.getField(field);
        if (raw == null) {
            return null;
        }
        else {
            return valueFromRaw(raw);
        }
    }

    /** Set the field from the given value. */
    public void setValue(DynamicMessage.Builder builder, FieldDescriptor field,
                         ValueSource value) {
        if (!value.isNull()) {
            builder.setField(field, rawFromValue(value));
        }
    }

    public int getDecimalScale() {
        return -1;
    }

    public static ProtobufRowConversion forTInstance(TInstance type) {
        TClass tclass = TInstance.tClass(type);
        if (tclass instanceof TBigDecimal) {
            int precision = type.attribute(DecimalAttribute.PRECISION);
            if (precision < 19) { // log10(Long.MAX_VALUE) = 18.965
                return new DecimalAsLong(type);
            }
            else {
                return new DecimalAsBytes(type);
            }
        }
        else {
            return TYPE_MAPPING.get(tclass);
        }
    }

    private static final Map<TClass,ProtobufRowConversion> TYPE_MAPPING = new HashMap<>();
    static {
        TYPE_MAPPING.put(AkBool.INSTANCE,
                         new CompatibleConversion(Type.TYPE_BOOL, UnderlyingType.BOOL));
        TYPE_MAPPING.put(MNumeric.BIGINT,
                         new IntegerConversion(Type.TYPE_SINT64, UnderlyingType.INT_64));
        TYPE_MAPPING.put(MNumeric.BIGINT_UNSIGNED,
                         new IntegerConversion(Type.TYPE_UINT64, UnderlyingType.INT_64));
        TYPE_MAPPING.put(MApproximateNumber.DOUBLE,
                         new CompatibleConversion(Type.TYPE_DOUBLE, UnderlyingType.DOUBLE));
        TYPE_MAPPING.put(MApproximateNumber.DOUBLE_UNSIGNED,
                         TYPE_MAPPING.get(MApproximateNumber.DOUBLE));
        TYPE_MAPPING.put(MApproximateNumber.FLOAT,
                         new CompatibleConversion(Type.TYPE_FLOAT, UnderlyingType.FLOAT));
        TYPE_MAPPING.put(MApproximateNumber.FLOAT_UNSIGNED,
                         TYPE_MAPPING.get(MApproximateNumber.FLOAT));
        TYPE_MAPPING.put(MNumeric.INT,
                         new IntegerConversion(Type.TYPE_SINT32, UnderlyingType.INT_32));
        TYPE_MAPPING.put(MNumeric.INT_UNSIGNED,
                         new IntegerConversion(Type.TYPE_UINT32, UnderlyingType.INT_32));
        TYPE_MAPPING.put(MNumeric.MEDIUMINT,
                         new IntegerConversion(Type.TYPE_SINT32, UnderlyingType.INT_32));
        TYPE_MAPPING.put(MNumeric.MEDIUMINT_UNSIGNED,
                         new IntegerConversion(Type.TYPE_UINT32, UnderlyingType.INT_32));
        TYPE_MAPPING.put(MNumeric.SMALLINT,
                         new IntegerConversion(Type.TYPE_SINT32, UnderlyingType.INT_16));
        TYPE_MAPPING.put(MNumeric.SMALLINT_UNSIGNED,
                         new IntegerConversion(Type.TYPE_UINT32, UnderlyingType.INT_16));
        TYPE_MAPPING.put(MNumeric.TINYINT,
                         new IntegerConversion(Type.TYPE_SINT32, UnderlyingType.INT_8));
        TYPE_MAPPING.put(MNumeric.TINYINT_UNSIGNED,
                         new IntegerConversion(Type.TYPE_UINT32, UnderlyingType.INT_8));
        TYPE_MAPPING.put(MDateAndTime.DATE,
                         new DateConversion());
        TYPE_MAPPING.put(MDateAndTime.DATETIME,
                         new DatetimeConversion());
        TYPE_MAPPING.put(MDateAndTime.YEAR,
                         new YearConversion());
        TYPE_MAPPING.put(MDateAndTime.TIME,
                         new TimeConversion());
        TYPE_MAPPING.put(MDateAndTime.TIMESTAMP,
                         new IntegerConversion(Type.TYPE_UINT32, UnderlyingType.INT_32));
        TYPE_MAPPING.put(MBinary.VARBINARY,
                         new BytesConversion());
        TYPE_MAPPING.put(MBinary.BINARY,
                         TYPE_MAPPING.get(MBinary.VARBINARY));
        TYPE_MAPPING.put(MString.VARCHAR,
                         new CompatibleConversion(Type.TYPE_STRING, UnderlyingType.STRING));
        TYPE_MAPPING.put(MString.CHAR,
                         TYPE_MAPPING.get(MString.VARCHAR));
        TYPE_MAPPING.put(MString.TINYTEXT,
                         TYPE_MAPPING.get(MString.VARCHAR));
        TYPE_MAPPING.put(MString.TEXT,
                         TYPE_MAPPING.get(MString.VARCHAR));
        TYPE_MAPPING.put(MString.MEDIUMTEXT,
                         TYPE_MAPPING.get(MString.VARCHAR));
        TYPE_MAPPING.put(MString.LONGTEXT,
                         TYPE_MAPPING.get(MString.VARCHAR));
    }

    static final class CompatibleConversion extends ProtobufRowConversion {
        private final Type type;
        private final UnderlyingType underlying;

        public CompatibleConversion(Type type, UnderlyingType underlying) {
            this.type = type;
            this.underlying = underlying;
        }

        @Override
        public Type getType() {
            return type;
        }

        @Override
        protected Object valueFromRaw(Object raw) {
            return raw;
        }

        @Override
        protected Object rawFromValue(ValueSource value) {
            switch (underlying) {
            case BOOL:
                return value.getBoolean();
            case FLOAT:
                return value.getFloat();
            case DOUBLE:
                return value.getDouble();
            case STRING:
            default:
                return value.getString();
            }
        }
    }

    static final class IntegerConversion extends ProtobufRowConversion {
        private final Type type;
        private final UnderlyingType underlying;

        public IntegerConversion(Type type, UnderlyingType underlying) {
            this.type = type;
            this.underlying = underlying;
        }

        @Override
        public Type getType() {
            return type;
        }

        @Override
        protected Object valueFromRaw(Object raw) {
            Number n = (Number)raw;
            switch (underlying) {
            case INT_8:
                return n.byteValue();
            case UINT_16: 
                return (char)n.shortValue();
            case INT_16: 
                return n.shortValue();
            case INT_32:
                return n.intValue();
            case INT_64:
                return n.longValue();
            default:
                return n;
            }
        }

        @Override
        protected Object rawFromValue(ValueSource value) {
            long lval;
            switch (underlying) {
            case INT_8:
                lval = value.getInt8();
                break;
            case UINT_16: 
                lval = value.getUInt16();
                break;
            case INT_16: 
                lval = value.getInt16();
                break;
            case INT_32:
                lval = value.getInt32();
                break;
            case INT_64:
            default:
                lval = value.getInt64();
            }
            switch (type) {
            case TYPE_FIXED32:
            case TYPE_INT32:
            case TYPE_SINT32:
            case TYPE_UINT32:
                return (int)lval;
            case TYPE_FIXED64:
            case TYPE_INT64:
            case TYPE_SINT64:
            case TYPE_UINT64:
            default:
                return lval;
            }
        }
    }

    static final class BytesConversion extends ProtobufRowConversion {
        @Override
        public Type getType() {
            return Type.TYPE_BYTES;
        }

        @Override
        protected Object valueFromRaw(Object raw) {
            return ((ByteString)raw).toByteArray();
        }

        @Override
        protected Object rawFromValue(ValueSource value) {
            return ByteString.copyFrom(value.getBytes());
        }
    }

    static final class DateConversion extends ProtobufRowConversion {
        @Override
        public Type getType() {
            return Type.TYPE_STRING;
        }

        @Override
        protected Object valueFromRaw(Object raw) {
            String date = (String)raw;
            return MDateAndTime.parseAndEncodeDate(date);
        }

        @Override
        protected Object rawFromValue(ValueSource value) {
            int date = value.getInt32();
            return MDateAndTime.dateToString(date);
        }
    }

    static final class DatetimeConversion extends ProtobufRowConversion {
        @Override
        public Type getType() {
            return Type.TYPE_STRING;
        }

        @Override
        protected Object valueFromRaw(Object raw) {
            String datetime = (String)raw;
            return MDateAndTime.parseAndEncodeDateTime(datetime);
        }

        @Override
        protected Object rawFromValue(ValueSource value) {
            long datetime = value.getInt64();
            return MDateAndTime.dateTimeToString(datetime);
        }
    }

    static final class TimeConversion extends ProtobufRowConversion {
        @Override
        public Type getType() {
            return Type.TYPE_SINT32;
        }

        @Override
        protected Object valueFromRaw(Object raw) {
            int secs = (Integer)raw;
            boolean negate = false;
            if (secs < 0) {
                secs = - secs;
                negate = true;
            }
            int h = secs / 3600;
            secs -= h * 3600;
            int m = secs / 60;
            secs -= m * 60;
            int s = secs;
            int hhmmss = h * 10000 + m * 100 + s;
            if (negate) {
                hhmmss = - hhmmss;
            }
            return hhmmss;
        }

        @Override
        protected Object rawFromValue(ValueSource value) {
            int hhmmss = value.getInt32();
            boolean negate = false;
            if (hhmmss < 0) {
                hhmmss = - hhmmss;
                negate = true;
            }
            int h = hhmmss / 10000;
            hhmmss -= h * 10000;
            int m = hhmmss / 100;
            hhmmss -= m * 100;
            int s = hhmmss;
            int secs = h * 3600 + m * 60 + s;
            if (negate) {
                secs = - secs;
            }
            return secs;
        }
    }

    static final class YearConversion extends ProtobufRowConversion {
        @Override
        public Type getType() {
            return Type.TYPE_SINT32;
        }

        @Override
        protected Object valueFromRaw(Object raw) {
            return (short)(((Integer)raw) - 1900);
        }

        @Override
        protected Object rawFromValue(ValueSource value) {
            short y = value.getInt16();
            return y + 1900;
        }
    }

    static final class DecimalAsLong extends ProtobufRowConversion {
        private final TInstance type;

        public DecimalAsLong(TInstance type) {
            this.type = type;
        }

        @Override
        public Type getType() {
            return Type.TYPE_SINT64;
        }

        @Override
        public int getDecimalScale() {
            return type.attribute(DecimalAttribute.SCALE);
        }

        @Override
        protected Object valueFromRaw(Object raw) {
            Long lval = (Long)raw;
            BigDecimal decimal = new BigDecimal(BigInteger.valueOf(lval), getDecimalScale());
            return new BigDecimalWrapperImpl(decimal);
        }

        @Override
        protected Object rawFromValue(ValueSource value) {
            BigDecimalWrapper wrapper = TBigDecimal.getWrapper(value, type);
            return wrapper.asBigDecimal().unscaledValue().longValue();
        }
    }

    // TODO: This is whatever byte encoding ConversionHelperBigDecimal uses.
    // Is that what we want?
    static final class DecimalAsBytes extends ProtobufRowConversion {
        private final TInstance type;

        public DecimalAsBytes(TInstance type) {
            this.type = type;
        }

        @Override
        public Type getType() {
            return Type.TYPE_BYTES;
        }

        @Override
        public int getDecimalScale() {
            return type.attribute(DecimalAttribute.SCALE);
        }

        @Override
        protected Object valueFromRaw(Object raw) {
            ByteString bytes = (ByteString)raw;
            int precision = type.attribute(DecimalAttribute.PRECISION);
            int scale = type.attribute(DecimalAttribute.SCALE);
            StringBuilder sb = new StringBuilder();
            ConversionHelperBigDecimal.decodeToString(bytes.toByteArray(), 0, precision, scale, AkibanAppender.of(sb));
            return new BigDecimalWrapperImpl(sb.toString());
        }

        @Override
        protected Object rawFromValue(ValueSource value) {
            BigDecimalWrapper wrapper = TBigDecimal.getWrapper(value, type);
            int precision = type.attribute(DecimalAttribute.PRECISION);
            int scale = type.attribute(DecimalAttribute.SCALE);
            return ByteString.copyFrom(ConversionHelperBigDecimal.bytesFromObject(wrapper.asBigDecimal(), precision, scale));
        }
    }
}
