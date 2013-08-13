/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server.types3.mcompat.mtypes;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.rowdata.ConversionHelperBigDecimal;
import com.akiban.server.types3.Attribute;
import com.akiban.server.types3.IllegalNameException;
import com.akiban.server.types3.PValueIO;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TClassBase;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TParsers;
import com.akiban.server.types3.aksql.AkCategory;
import com.akiban.server.types3.common.BigDecimalWrapper;
import com.akiban.server.types3.common.NumericFormatter;
import com.akiban.server.types3.mcompat.MBundle;
import com.akiban.server.types3.pvalue.PBasicValueSource;
import com.akiban.server.types3.pvalue.PBasicValueTarget;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueCacher;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.Serialization;
import com.akiban.server.types3.texpressions.SerializeAs;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;
import com.akiban.util.AkibanAppender;
import com.akiban.util.ByteSource;

import java.math.BigDecimal;

public class MBigDecimal extends TClassBase {

    public enum Attrs implements Attribute {
        @SerializeAs(Serialization.LONG_1) PRECISION,
        @SerializeAs(Serialization.LONG_2) SCALE
    }

    public static final int MAX_INDEX = 0;
    public static final int MIN_INDEX = 1;

    public static BigDecimalWrapper getWrapper(PValueSource source, TInstance tInstance) {
        if (source.hasCacheValue())
            return (BigDecimalWrapper) source.getObject();
        byte[] bytes = source.getBytes();
        int precision = tInstance.attribute(Attrs.PRECISION);
        int scale = tInstance.attribute(Attrs.SCALE);
        StringBuilder sb = new StringBuilder();
        ConversionHelperBigDecimal.decodeToString(bytes, 0, precision, scale, AkibanAppender.of(sb));
        return new MBigDecimalWrapper(sb.toString());
    }

    public static BigDecimalWrapper getWrapper(TExecutionContext context, int index) {
        BigDecimalWrapper wrapper = (BigDecimalWrapper)context.exectimeObjectAt(index);
        if (wrapper == null) {
            wrapper = new MBigDecimalWrapper();
            context.putExectimeObject(index, wrapper);
        }
        wrapper.reset();
        return wrapper;
    }

    public static void adjustAttrsAsNeeded(TExecutionContext context, PValueSource source,
                                           TInstance targetInstance, PValueTarget target)
    {
        TInstance inputInstance = context.inputTInstanceAt(0);
        int inputPrecision = inputInstance.attribute(Attrs.PRECISION);
        int targetPrecision = targetInstance.attribute(Attrs.PRECISION);
        int inputScale = inputInstance.attribute(Attrs.SCALE);
        int targetScale = targetInstance.attribute(Attrs.SCALE);
        if ( (inputPrecision != targetPrecision) || (inputScale != targetScale) ) {
            BigDecimalWrapper bdw = new MBigDecimalWrapper().set(getWrapper(source, inputInstance));
            bdw.round(targetScale);
            target.putObject(bdw);
        }
        else if (source.hasCacheValue()) {
            target.putObject(source.getObject());
        }
        else if (source.hasRawValue()) {
            target.putBytes(source.getBytes());
        }
        else {
            throw new IllegalStateException("no value set");
        }
    }

    @Override
    public boolean attributeIsPhysical(int attributeIndex) {
        return true;
    }

    @Override
    protected PValueIO getPValueIO() {
        return valueIO;
    }

    public MBigDecimal(String name, int defaultVarcharLen){
        super(MBundle.INSTANCE.id(), name, AkCategory.DECIMAL, Attrs.class, NumericFormatter.FORMAT.BIGDECIMAL, 1, 1, 8,
                PUnderlying.BYTES, TParsers.DECIMAL, defaultVarcharLen);
    }

    @Override
    public Object formatCachedForNiceRow(PValueSource source) {
        return ((BigDecimalWrapper)source.getObject()).asBigDecimal();
    }

    @Override
    protected int doCompare(TInstance instanceA, PValueSource sourceA, TInstance instanceB, PValueSource sourceB)
    {
        if (sourceA.hasRawValue() && sourceB.hasRawValue()) // both have bytearrays
            return super.doCompare(instanceA, sourceA, instanceB, sourceB);
        else
            return getWrapper(sourceA, instanceA).compareTo(getWrapper(sourceB, instanceB));
    }

    @Override
    public void selfCast(TExecutionContext context, TInstance sourceInstance, PValueSource source,
                         TInstance targetInstance, PValueTarget target)
    {
        adjustAttrsAsNeeded(context, source, targetInstance, target);
    }

    @Override
    public boolean normalizeInstancesBeforeComparison() {
        return true;
    }

    @Override
    protected DataTypeDescriptor dataTypeDescriptor(TInstance instance) {
        // stolen from TypesTranslation
        int precision = instance.attribute(Attrs.PRECISION);
        int scale = instance.attribute(Attrs.SCALE);
        return new DataTypeDescriptor(TypeId.DECIMAL_ID, precision, scale, instance.nullability(),
                DataTypeDescriptor.computeMaxWidth(precision, scale));
    }

    public TClass widestComparable()
    {
        return this;
    }
    
    @Override
    public PValueCacher cacher() {
        return cacher;
    }

    @Override
    public TInstance instance(boolean nullable) {
        return instance(10, 0, nullable);
    }

    @Override
    protected void validate(TInstance instance) {
        int precision = instance.attribute(Attrs.PRECISION);
        int scale = instance.attribute(Attrs.SCALE);
        if (precision < scale)
            throw new IllegalNameException("precision must be >= scale");
    }

    @Override
    protected TInstance doPickInstance(TInstance left, TInstance right, boolean suggestedNullability) {
        int scaleL = left.attribute(Attrs.SCALE);
        int scaleR = right.attribute(Attrs.SCALE);

        int precisionL = left.attribute(Attrs.PRECISION);
        int precisionR = right.attribute(Attrs.PRECISION);

        return pickPrecisionAndScale(MBigDecimal.this, precisionL, scaleL, precisionR, scaleR, suggestedNullability);
    }

    public static TInstance pickPrecisionAndScale(TClass tclass,
                                                  int precisionL, int scaleL, int precisionR, int scaleR,
                                                  boolean nullable)
    {
        int resultPrecision, resultScale;

        if (scaleL == scaleR) {
            resultScale = scaleL;
            resultPrecision = Math.max(precisionL, precisionR);
        }
        else {
            int precisionOfSmallerScale;
            if (scaleL > scaleR) {
                resultScale = scaleL;
                resultPrecision = scaleL; // might be swapped later
                precisionOfSmallerScale = precisionR;
            }
            else {
                resultScale = scaleR;
                resultPrecision = scaleR; // might be swapped later
                precisionOfSmallerScale = precisionL;
            }
            // Whatever the precision was of the DECIMAL of smaller scale, widen it so that we can have this DECIMAL
            // have the big scale
            precisionOfSmallerScale += Math.abs(scaleL - scaleR);
            resultPrecision = Math.max(precisionOfSmallerScale, resultPrecision);
        }
        return tclass.instance(resultPrecision, resultScale, nullable);
    }

    public static final PValueCacher cacher = new PValueCacher() {

        @Override
        public void cacheToValue(Object bdw, TInstance tInstance, PBasicValueTarget target) {
            BigDecimal bd = ((BigDecimalWrapper)bdw).asBigDecimal();
            int precision = tInstance.attribute(Attrs.PRECISION);
            int scale = tInstance.attribute(Attrs.SCALE);
            byte[] bb = ConversionHelperBigDecimal.bytesFromObject(bd, precision, scale);
            target.putBytes(bb);
        }

        @Override
        public BigDecimalWrapper valueToCache(PBasicValueSource value, TInstance tInstance) {
            int precision = tInstance.attribute(Attrs.PRECISION);
            int scale = tInstance.attribute(Attrs.SCALE);
            byte[] bb = value.getBytes();
            StringBuilder sb = new StringBuilder(precision + 2); // +2 for dot and minus sign
            ConversionHelperBigDecimal.decodeToString(bb, 0, precision, scale, AkibanAppender.of(sb));
            return new MBigDecimalWrapper(sb.toString());
        }

        @Override
        public Object sanitize(Object object) {
            if (object instanceof BigDecimal)
                return new MBigDecimalWrapper((BigDecimal)object);
            else if (object instanceof MBigDecimalWrapper)
                return object;
            else if (object instanceof String)
                return new MBigDecimalWrapper((String)object);
            throw new UnsupportedOperationException(String.valueOf(object));
        }

        @Override
        public boolean canConvertToValue(Object cached) {
            return true;
        }
    };

    private static final PValueIO valueIO = new PValueIO() {
        @Override
        public void copyCanonical(PValueSource in, TInstance typeInstance, PValueTarget out) {
            if (in.hasCacheValue()) {
                if (out.supportsCachedObjects())
                    out.putObject(in.getObject());
                else
                    cacher.cacheToValue(in.getObject(), typeInstance, out);
            }
            else if (in.hasRawValue()) {
                out.putBytes(in.getBytes());
            }
            else
                throw new AssertionError("no value");
        }

        @Override
        public void writeCollating(PValueSource in, TInstance typeInstance, PValueTarget out) {
            BigDecimalWrapper wrapper = getWrapper(in, typeInstance);
            out.putObject(wrapper.asBigDecimal());
        }

        @Override
        public void readCollating(PValueSource in, TInstance typeInstance, PValueTarget out) {
            BigDecimal bigDecimal = (BigDecimal) in.getObject();
            int allowedScale = typeInstance.attribute(Attrs.SCALE);
            int allowedPrecision = typeInstance.attribute(Attrs.PRECISION);
            if (allowedPrecision < bigDecimal.precision()) {
                throw new AkibanInternalException("precision of " + bigDecimal.precision()
                        + " is greater than " + allowedPrecision + " for value " + bigDecimal);
            }
            if (allowedScale < bigDecimal.scale()) {
                throw new AkibanInternalException("scale of " + bigDecimal.scale()
                        + " is greater than " + allowedScale + " for value " + bigDecimal);
            }
            BigDecimalWrapper wrapper = new MBigDecimalWrapper(bigDecimal).round(allowedScale);
            out.putObject(wrapper);
        }
    };

    @Override
    protected boolean tryFromObject(TExecutionContext context, PValueSource in, PValueTarget out) {
        // If the incoming PValueSource is a DECIMAL, *and* it has a cache value (ie an BigDecimalWrapper), then
        // we can just copy the wrapper into the output. If the incoming is a DECIMAL with bytes (its raw form), we
        // can only copy those bytes if the TInstance match -- and super.tryFromObject already makes that check.
        if (in.tInstance().typeClass() instanceof MBigDecimal && in.hasCacheValue()) {
            BigDecimalWrapper cached = (BigDecimalWrapper) in.getObject();
            out.putObject(new MBigDecimalWrapper(cached.asBigDecimal()));
            return true;
        }
        return super.tryFromObject(context, in, out);
    }
}