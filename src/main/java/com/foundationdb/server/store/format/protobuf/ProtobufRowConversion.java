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
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MBigDecimal;
import com.foundationdb.server.types.mcompat.mtypes.MBigDecimalWrapper;
import com.foundationdb.server.types.mcompat.mtypes.MBinary;
import com.foundationdb.server.types.mcompat.mtypes.MDatetimes;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.AkibanAppender;

import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import org.joda.time.DateTime;
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

    public static ProtobufRowConversion forTInstance(TInstance tinst) {
        TClass tclass = TInstance.tClass(tinst);
        if (tclass instanceof MBigDecimal) {
            int precision = tinst.attribute(MBigDecimal.Attrs.PRECISION);
            if (precision < 19) { // log10(Long.MAX_VALUE) = 18.965
                return new DecimalAsLong(tinst);
            }
            else {
                return new DecimalAsBytes(tinst);
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
        TYPE_MAPPING.put(MDatetimes.DATE,
                         new DateConversion());
        TYPE_MAPPING.put(MDatetimes.DATETIME,
                         new DatetimeConversion());
        TYPE_MAPPING.put(MDatetimes.YEAR,
                         new YearConversion());
        TYPE_MAPPING.put(MDatetimes.TIME,
                         new TimeConversion());
        TYPE_MAPPING.put(MDatetimes.TIMESTAMP,
                         new IntegerConversion(Type.TYPE_UINT32, UnderlyingType.INT_32));
        TYPE_MAPPING.put(MBinary.VARBINARY,
                         new BytesConversion());
        TYPE_MAPPING.put(MBinary.BINARY,
                         TYPE_MAPPING.get(MBinary.VARBINARY));
        TYPE_MAPPING.put(MString.VARCHAR,
                         new CompatibleConversion(Type.TYPE_STRING, UnderlyingType.STRING));
        TYPE_MAPPING.put(MString.CHAR,
                         TYPE_MAPPING.get(MString.VARCHAR));
        TYPE_MAPPING.put(MBinary.TINYBLOB,
                         TYPE_MAPPING.get(MBinary.VARBINARY));
        TYPE_MAPPING.put(MString.TINYTEXT,
                         TYPE_MAPPING.get(MString.VARCHAR));
        TYPE_MAPPING.put(MBinary.BLOB,
                         TYPE_MAPPING.get(MBinary.VARBINARY));
        TYPE_MAPPING.put(MString.TEXT,
                         TYPE_MAPPING.get(MString.VARCHAR));
        TYPE_MAPPING.put(MBinary.MEDIUMBLOB,
                         TYPE_MAPPING.get(MBinary.VARBINARY));
        TYPE_MAPPING.put(MString.MEDIUMTEXT,
                         TYPE_MAPPING.get(MString.VARCHAR));
        TYPE_MAPPING.put(MBinary.LONGBLOB,
                         TYPE_MAPPING.get(MBinary.VARBINARY));
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
            case UINT_16: 
                lval = value.getUInt16();
            case INT_16: 
                lval = value.getInt16();
            case INT_32:
                lval = value.getInt32();
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

    // TODO: Might an ISO string be a better choice?
    static final class DateConversion extends ProtobufRowConversion {
        @Override
        public Type getType() {
            return Type.TYPE_SINT32;
        }

        @Override
        protected Object valueFromRaw(Object raw) {
            int secs = (Integer)raw;
            DateTime dt = new DateTime(secs * 1000);
            return dt.getYear() * 512
                + dt.getMonthOfYear() * 32
                + dt.getDayOfMonth();
        }

        @Override
        protected Object rawFromValue(ValueSource value) {
            int ival = value.getInt32();
            int y = ival / 512;
            ival -= y * 512;
            int m = ival / 32;
            int d = ival - m * 32;
            long millis = new DateTime(y, m, d, 0, 0, 0).getMillis();
            return (int)(millis / 1000);
        }
    }

    // TODO: Might an ISO string be a better choice?
    static final class DatetimeConversion extends ProtobufRowConversion {
        @Override
        public Type getType() {
            return Type.TYPE_SINT32;
        }

        @Override
        protected Object valueFromRaw(Object raw) {
            int secs = (Integer)raw;
            DateTime dt = new DateTime(secs * 1000);
            return dt.getYear() * 10000000000L
                + dt.getMonthOfYear() * 100000000L
                + dt.getDayOfMonth() *  1000000L
                + dt.getHourOfDay() * 10000L
                + dt.getMinuteOfHour() * 100L
                + dt.getSecondOfMinute();
        }

        @Override
        protected Object rawFromValue(ValueSource value) {
            long lval = value.getInt64();
            int y = (int)(lval / 10000000000L);
            lval -= y * 10000000000L;
            int m = (int)(lval / 100000000L);
            lval -= m * 100000000L;
            int d = (int)(lval / 1000000L);
            lval -= d * 1000000L;
            int h = (int)(lval / 10000L);
            lval -= h * 10000L;
            int n = (int)(lval / 100L);
            lval -= n * 100L;
            int s = (int)lval;
            long millis = new DateTime(y, m, d, h, n, s).getMillis();
            return (int)(millis / 1000);
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
        private final TInstance tinst;

        public DecimalAsLong(TInstance tinst) {
            this.tinst = tinst;
        }

        @Override
        public Type getType() {
            return Type.TYPE_SINT64;
        }

        @Override
        public int getDecimalScale() {
            return tinst.attribute(MBigDecimal.Attrs.SCALE);
        }

        @Override
        protected Object valueFromRaw(Object raw) {
            Long lval = (Long)raw;
            BigDecimal decimal = new BigDecimal(BigInteger.valueOf(lval), getDecimalScale());
            return new MBigDecimalWrapper(decimal);
        }

        @Override
        protected Object rawFromValue(ValueSource value) {
            BigDecimalWrapper wrapper = MBigDecimal.getWrapper(value, tinst);
            return wrapper.asBigDecimal().unscaledValue().longValue();
        }
    }

    // TODO: This is whatever byte encoding ConversionHelperBigDecimal uses.
    // Is that what we want?
    static final class DecimalAsBytes extends ProtobufRowConversion {
        private final TInstance tinst;

        public DecimalAsBytes(TInstance tinst) {
            this.tinst = tinst;
        }

        @Override
        public Type getType() {
            return Type.TYPE_BYTES;
        }

        @Override
        public int getDecimalScale() {
            return tinst.attribute(MBigDecimal.Attrs.SCALE);
        }

        @Override
        protected Object valueFromRaw(Object raw) {
            ByteString bytes = (ByteString)raw;
            int precision = tinst.attribute(MBigDecimal.Attrs.PRECISION);
            int scale = tinst.attribute(MBigDecimal.Attrs.SCALE);
            StringBuilder sb = new StringBuilder();
            ConversionHelperBigDecimal.decodeToString(bytes.toByteArray(), 0, precision, scale, AkibanAppender.of(sb));
            return new MBigDecimalWrapper(sb.toString());
        }

        @Override
        protected Object rawFromValue(ValueSource value) {
            BigDecimalWrapper wrapper = MBigDecimal.getWrapper(value, tinst);
            int precision = tinst.attribute(MBigDecimal.Attrs.PRECISION);
            int scale = tinst.attribute(MBigDecimal.Attrs.SCALE);
            return ByteString.copyFrom(ConversionHelperBigDecimal.bytesFromObject(wrapper.asBigDecimal(), precision, scale));
        }
    }
}
