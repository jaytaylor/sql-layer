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

import java.util.EnumMap;
import java.util.Map;

final class LegacyTransformations {

    // public interface

    public static final LegacyTransformations TRIVIAL_TRANSFORMATIONS = new LegacyTransformations(getTransformers());

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
        new StringTransformer().addTo(map, AkType.VARCHAR, AkType.TEXT);

        return map;
    }


    private LegacyTransformations(Map<AkType, Transformer> transformers) {
        this.transformers = transformers;
    }

    // object state

    private final Map<AkType,Transformer> transformers;

    // nested classes
    
    private interface Transformer {
        Object tryTransform(Object in);
    }

    private abstract static class AbstractTransformer<T> implements Transformer {
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

        private FloatTransformer() {
            super(Float.class);
        }
    }

    private static class LongTransformer extends AbstractTransformer<Long> {
        @Override
        protected Long doTransform(Object in) {
            if (in instanceof Number) {
                return ((Number)in).longValue();
            }
            return super.doTransform(in);
        }

        protected LongTransformer() {
            super(Long.class);
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

        private ByteSourceTransformer() {
            super(ByteSource.class);
        }
    }

    private static class StringTransformer extends AbstractTransformer<String> {
        @Override
        protected String doTransform(Object in) {
            return String.valueOf(in);
        }

        private StringTransformer() {
            super(String.class);
        }
    }
}
