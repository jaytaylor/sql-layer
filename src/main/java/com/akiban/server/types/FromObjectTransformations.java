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

package com.akiban.server.types;

import com.akiban.util.ByteSource;
import com.akiban.util.WrappingByteSource;

import java.util.EnumMap;
import java.util.Map;

final class FromObjectTransformations {

    // public interface

    public static final FromObjectTransformations TRIVIAL_TRANSFORMATIONS = new FromObjectTransformations(getTransformers());

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
        new BoolTransformer().addTo(map, AkType.BOOL);

        return map;
    }


    private FromObjectTransformations(Map<AkType, Transformer> transformers) {
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

    private static class BoolTransformer extends AbstractTransformer<Boolean> {
        @Override
        protected Boolean doTransform(Object in) {
            if (in instanceof Boolean) {
                return (Boolean) in;
            }
            return super.doTransform(in);
        }

        private BoolTransformer() {
            super(Boolean.class);
        }
    }
}
