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

package com.akiban.server.types;

import com.akiban.util.ByteSource;
import com.akiban.util.WrappingByteSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.EnumMap;
import java.util.Map;

final class LegacyTransformations {

    // public interface

    public static final LegacyTransformations INSTANCE = new LegacyTransformations(getTransformers());

    /**
     * Tries to transform the given object to a suitable equivalent. This method exists to ease
     * the transition from the old conversion system to the new one.
     * @param type the desired type
     * @param object the object to possibly transform
     * @return the incoming object, or its transformed instance
     */
    public Object tryTransformations(AkType type, Object object) {
        Transformer converter = transformers.get(type);
        if (converter != null) {
            object = converter.tryTransform(object);
        }
        return object;
    }

    // for use in this class

    private static Map<AkType,Transformer> getTransformers() {
        Map<AkType,Transformer> map = new EnumMap<AkType, Transformer>(AkType.class);

        new LongTransformer().addTo(map, AkType.INT, AkType.U_INT, AkType.LONG);
        new FloatTransformer().addTo(map, AkType.FLOAT, AkType.U_FLOAT);
        new DoubleTransformer().addTo(map, AkType.DOUBLE, AkType.U_DOUBLE);
        new ByteSourceTransformer().addTo(map, AkType.VARBINARY);
        new YearTransformer().addTo(map, AkType.YEAR);
        new DateTransformer().addTo(map, AkType.DATE);
        new DatetimeTransformer().addTo(map, AkType.DATETIME);
        new TimeTransformer().addTo(map, AkType.TIME);
        new TimestampTransformer().addTo(map, AkType.TIMESTAMP);
        new BigDecimalTransformer().addTo(map, AkType.DECIMAL);
        new BigIntegerTransformer().addTo(map, AkType.U_BIGINT);
        new StringTransformer().addTo(map, AkType.VARCHAR, AkType.TEXT);

        return map;
    }


    private LegacyTransformations(Map<AkType, Transformer> transformers) {
        this.transformers = transformers;
    }

    // object state

    private final Map<AkType,Transformer> transformers;

    // consts

    static final long DATE_SCALE = 1000000L;
    static final long YEAR_SCALE = 10000L * DATE_SCALE;
    static final long MONTH_SCALE = 100L * DATE_SCALE;
    static final long DAY_SCALE = 1L * DATE_SCALE;
    static final long HOUR_SCALE = 10000L;
    static final long MIN_SCALE = 100L;
    static final long SEC_SCALE = 1L;
    static final long TIME_HOURS_SCALE = 10000;
    static final long TIME_MINUTES_SCALE = 100;

    // nested classes
    
    private interface Transformer {
        Object tryTransform(Object in);
    }

    private abstract static class AbstractTransformer<T> implements Transformer {
        protected abstract T fromString(String in);

        protected T doTransform(Object in) {
            throw badTransformation(in);
        }

        public void addTo(Map<AkType,? super Transformer> map, AkType... types) {
            for (AkType type : types) {
                Object old = map.put(type, this);
                assert old == null : old;
            }
        }

        @Override
        public final T tryTransform(Object in) {
            if (in == null) {
                return null;
            }
            if (targetClass.isInstance(in)) {
                return targetClass.cast(in);
            }
            if (in instanceof String) {
                return fromString((String)in);
            }
            return doTransform(in);
        }

        protected AbstractTransformer(Class<T> targetClass) {
            this.targetClass = targetClass;
        }

        protected RuntimeException badTransformation(Object instance) {
            throw new IllegalArgumentException(String.format("can't transform %s (%s) to %s",
                    instance,
                    instance.getClass(),
                    targetClass)
            );
        }

        private final Class<T> targetClass;
    }

    private static class DoubleTransformer extends AbstractTransformer<Double> {
        @Override
        public Double doTransform(Object in) {
            if (in instanceof Number) {
                return ((Number)in).doubleValue();
            }
            return super.doTransform(in);
        }

        @Override
        protected Double fromString(String in) {
            return Double.parseDouble(in);
        }

        private DoubleTransformer() {
            super(Double.class);
        }
    }

    private static class FloatTransformer extends AbstractTransformer<Float> {
        @Override
        protected Float doTransform(Object in) {
            if (in instanceof Number) {
                return ((Number)in).floatValue();
            }
            return super.doTransform(in);
        }

        @Override
        protected Float fromString(String in) {
            return Float.parseFloat(in);
        }

        private FloatTransformer() {
            super(Float.class);
        }
    }

    private abstract static class AbstractLongTransformer extends AbstractTransformer<Long> {
        protected AbstractLongTransformer() {
            super(Long.class);
        }
    }

    private static class LongTransformer extends AbstractLongTransformer {
        @Override
        protected Long doTransform(Object in) {
            if (in instanceof Number) {
                return ((Number)in).longValue();
            }
            return super.doTransform(in);
        }

        @Override
        protected Long fromString(String string) {
            return Long.parseLong(string);
        }
    }

    private static class ByteSourceTransformer extends AbstractTransformer<ByteSource> {
        @Override
        protected ByteSource doTransform(Object in) {
            if (in instanceof byte[]) {
                return new WrappingByteSource().wrap((byte[])in);
            }
            return super.doTransform(in);
        }

        @Override
        protected ByteSource fromString(String in) {
            throw badTransformation(in);
        }

        private ByteSourceTransformer() {
            super(ByteSource.class);
        }
    }

    private static class YearTransformer extends AbstractLongTransformer {
        @Override
        protected Long fromString(String string) {
            long value = Long.parseLong(string);
            return value == 0 ? 0 : (value - 1900);
        }
    }

    private static class DateTransformer extends AbstractLongTransformer {
        @Override
        protected Long fromString(String string) {
            // YYYY-MM-DD
            final String values[] = string.split("-");
            long y = 0, m = 0, d = 0;
            switch(values.length) {
            case 3: d = Integer.parseInt(values[2]); // fall
            case 2: m = Integer.parseInt(values[1]); // fall
            case 1: y = Integer.parseInt(values[0]); break;
            default:
                throw new IllegalArgumentException("Invalid date string");
            }
            return d + m*32 + y*512;
        }
    }

    private static class DatetimeTransformer extends AbstractLongTransformer {
        @Override
        protected Long fromString(String string) {
            final String parts[] = string.split(" ");
            if(parts.length != 2) {
                throw new IllegalArgumentException("Invalid DATETIME string");
            }

            final String dateParts[] = parts[0].split("-");
            if(dateParts.length != 3) {
                throw new IllegalArgumentException("Invalid DATE portion");
            }

            final String timeParts[] = parts[1].split(":");
            if(timeParts.length != 3) {
                throw new IllegalArgumentException("Invalid TIME portion");
            }

            return  Long.parseLong(dateParts[0]) * YEAR_SCALE +
                    Long.parseLong(dateParts[1]) * MONTH_SCALE +
                    Long.parseLong(dateParts[2]) * DAY_SCALE +
                    Long.parseLong(timeParts[0]) * HOUR_SCALE +
                    Long.parseLong(timeParts[1]) * MIN_SCALE +
                    Long.parseLong(timeParts[2]) * SEC_SCALE;
        }
    }

    private static class TimeTransformer extends AbstractLongTransformer {
        @Override
        protected Long fromString(String string) {
            // (-)HH:MM:SS
            int mul = 1;
            if(string.length() > 0 && string.charAt(0) == '-') {
                mul = -1;
                string = string.substring(1);
            }
            int hours = 0;
            int minutes = 0;
            int seconds = 0;
            int offset = 0;
            final String values[] = string.split(":");
            switch(values.length) {
            case 3: hours   = Integer.parseInt(values[offset++]); // fall
            case 2: minutes = Integer.parseInt(values[offset++]); // fall
            case 1: seconds = Integer.parseInt(values[offset]);   break;
            default:
                throw new IllegalArgumentException("Invalid TIME string");
            }
            minutes += seconds/60;
            seconds %= 60;
            hours += minutes/60;
            minutes %= 60;
            return mul * (hours* TIME_HOURS_SCALE + minutes* TIME_MINUTES_SCALE + seconds);
        }
    }

    private static class TimestampTransformer extends AbstractLongTransformer {
        @Override
        protected Long fromString(String string) {
            try {
                return SDF.parse(string).getTime() / 1000;
            } catch(ParseException e) {
                throw new IllegalArgumentException(e);
            }
        }

        private final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    private static class BigDecimalTransformer extends AbstractTransformer<BigDecimal> {

        @Override
        protected BigDecimal fromString(String in) {
            return new BigDecimal(in);
        }

        private BigDecimalTransformer() {
            super(BigDecimal.class);
        }
    }

    private static class BigIntegerTransformer extends AbstractTransformer<BigInteger> {
        @Override
        protected BigInteger fromString(String in) {
            return new BigInteger(in);
        }

        private BigIntegerTransformer() {
            super(BigInteger.class);
        }
    }

    private static class StringTransformer extends AbstractTransformer<String> {
        @Override
        protected String fromString(String in) {
            return in;
        }

        @Override
        protected String doTransform(Object in) {
            return String.valueOf(in);
        }

        private StringTransformer() {
            super(String.class);
        }
    }
}
