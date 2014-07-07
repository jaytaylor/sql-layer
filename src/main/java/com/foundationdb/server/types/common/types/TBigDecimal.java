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

package com.foundationdb.server.types.common.types;

import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.rowdata.ConversionHelperBigDecimal;
import com.foundationdb.server.types.*;
import com.foundationdb.server.types.ValueIO;
import com.foundationdb.server.types.aksql.AkCategory;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.common.BigDecimalWrapperImpl;
import com.foundationdb.server.types.common.NumericFormatter;
import com.foundationdb.server.types.mcompat.MParsers;
import com.foundationdb.server.types.value.*;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;
import com.foundationdb.util.AkibanAppender;

import java.math.BigDecimal;
import java.sql.Types;

public class TBigDecimal extends TClassBase {

    public static final int MAX_INDEX = 0;
    public static final int MIN_INDEX = 1;

    public static BigDecimalWrapper getWrapper(ValueSource source, TInstance type) {
        if (source.hasCacheValue())
            return (BigDecimalWrapper) source.getObject();
        byte[] bytes = source.getBytes();
        int precision = type.attribute(DecimalAttribute.PRECISION);
        int scale = type.attribute(DecimalAttribute.SCALE);
        StringBuilder sb = new StringBuilder();
        ConversionHelperBigDecimal.decodeToString(bytes, 0, precision, scale, AkibanAppender.of(sb));
        return new BigDecimalWrapperImpl(sb.toString());
    }

    public static BigDecimalWrapper getWrapper(TExecutionContext context, int index) {
        BigDecimalWrapper wrapper = (BigDecimalWrapper)context.exectimeObjectAt(index);
        if (wrapper == null) {
            wrapper = new BigDecimalWrapperImpl();
            context.putExectimeObject(index, wrapper);
        }
        wrapper.reset();
        return wrapper;
    }

    public static void adjustAttrsAsNeeded(TExecutionContext context, ValueSource source,
                                           TInstance targetInstance, ValueTarget target)
    {
        TInstance inputInstance = context.inputTypeAt(0);
        int inputPrecision = inputInstance.attribute(DecimalAttribute.PRECISION);
        int targetPrecision = targetInstance.attribute(DecimalAttribute.PRECISION);
        int inputScale = inputInstance.attribute(DecimalAttribute.SCALE);
        int targetScale = targetInstance.attribute(DecimalAttribute.SCALE);
        if ( (inputPrecision != targetPrecision) || (inputScale != targetScale) ) {
            BigDecimalWrapper bdw = new BigDecimalWrapperImpl().set(getWrapper(source, inputInstance));
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
    protected boolean attributeAlwaysDisplayed(int attributeIndex) {
        return true;
    }

    @Override
    protected ValueIO getValueIO() {
        return valueIO;
    }

    protected TBigDecimal(TBundle bundle, String name, int defaultVarcharLen){
        super(bundle.id(), name, AkCategory.DECIMAL, DecimalAttribute.class, NumericFormatter.FORMAT.BIGDECIMAL, 1, 1, -1,
                UnderlyingType.BYTES, MParsers.DECIMAL, defaultVarcharLen);
    }

    @Override
    public Object formatCachedForNiceRow(ValueSource source) {
        return ((BigDecimalWrapper)source.getObject()).asBigDecimal();
    }

    @Override
    protected int doCompare(TInstance typeA, ValueSource sourceA, TInstance typeB, ValueSource sourceB)
    {
        if (sourceA.hasRawValue() && sourceB.hasRawValue()) // both have bytearrays
            return super.doCompare(typeA, sourceA, typeB, sourceB);
        else
            return getWrapper(sourceA, typeA).compareTo(getWrapper(sourceB, typeB));
    }

    @Override
    public void selfCast(TExecutionContext context, TInstance sourceInstance, ValueSource source,
                         TInstance targetInstance, ValueTarget target)
    {
        adjustAttrsAsNeeded(context, source, targetInstance, target);
    }

    @Override
    public boolean normalizeInstancesBeforeComparison() {
        return true;
    }

    @Override
    public int jdbcType() {
        return Types.DECIMAL;
    }

    @Override
    protected DataTypeDescriptor dataTypeDescriptor(TInstance type) {
        int precision = type.attribute(DecimalAttribute.PRECISION);
        int scale = type.attribute(DecimalAttribute.SCALE);
        return new DataTypeDescriptor(TypeId.DECIMAL_ID, precision, scale, type.nullability(),
                DataTypeDescriptor.computeMaxWidth(precision, scale));
    }

    public TClass widestComparable()
    {
        return this;
    }
    
    @Override
    public ValueCacher cacher() {
        return cacher;
    }

    @Override
    public TInstance instance(boolean nullable) {
        return instance(10, 0, nullable);
    }

    @Override
    protected void validate(TInstance type) {
        int precision = type.attribute(DecimalAttribute.PRECISION);
        int scale = type.attribute(DecimalAttribute.SCALE);
        if (precision < scale)
            throw new IllegalNameException("precision must be >= scale");
    }

    @Override
    protected TInstance doPickInstance(TInstance left, TInstance right, boolean suggestedNullability) {
        int scaleL = left.attribute(DecimalAttribute.SCALE);
        int scaleR = right.attribute(DecimalAttribute.SCALE);

        int precisionL = left.attribute(DecimalAttribute.PRECISION);
        int precisionR = right.attribute(DecimalAttribute.PRECISION);

        return pickPrecisionAndScale(TBigDecimal.this, precisionL, scaleL, precisionR, scaleR, suggestedNullability);
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

    public static final ValueCacher cacher = new ValueCacher() {

        @Override
        public void cacheToValue(Object bdw, TInstance type, BasicValueTarget target) {
            BigDecimal bd = ((BigDecimalWrapper)bdw).asBigDecimal();
            int precision = type.attribute(DecimalAttribute.PRECISION);
            int scale = type.attribute(DecimalAttribute.SCALE);
            byte[] bb = ConversionHelperBigDecimal.bytesFromObject(bd, precision, scale);
            target.putBytes(bb);
        }

        @Override
        public BigDecimalWrapper valueToCache(BasicValueSource value, TInstance type) {
            int precision = type.attribute(DecimalAttribute.PRECISION);
            int scale = type.attribute(DecimalAttribute.SCALE);
            byte[] bb = value.getBytes();
            StringBuilder sb = new StringBuilder(precision + 2); // +2 for dot and minus sign
            ConversionHelperBigDecimal.decodeToString(bb, 0, precision, scale, AkibanAppender.of(sb));
            return new BigDecimalWrapperImpl(sb.toString());
        }

        @Override
        public Object sanitize(Object object) {
            if (object instanceof BigDecimal)
                return new BigDecimalWrapperImpl((BigDecimal)object);
            else if (object instanceof BigDecimalWrapperImpl)
                return object;
            else if (object instanceof String)
                return new BigDecimalWrapperImpl((String)object);
            else if (object instanceof Long)
                return new BigDecimalWrapperImpl((long)object);
            throw new UnsupportedOperationException(String.valueOf(object));
        }

        @Override
        public boolean canConvertToValue(Object cached) {
            return true;
        }
    };

    private static final ValueIO valueIO = new ValueIO() {
        @Override
        public void copyCanonical(ValueSource in, TInstance typeInstance, ValueTarget out) {
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
        public void writeCollating(ValueSource in, TInstance typeInstance, ValueTarget out) {
            BigDecimalWrapper wrapper = getWrapper(in, typeInstance);
            out.putObject(wrapper.asBigDecimal());
        }

        @Override
        public void readCollating(ValueSource in, TInstance typeInstance, ValueTarget out) {
            Object input = in.getObject();
            BigDecimal bigDecimal;
            if (input instanceof BigDecimalWrapperImpl) {
                bigDecimal = ((BigDecimalWrapperImpl)input).asBigDecimal();
            } else if (input instanceof BigDecimal) {
                bigDecimal = (BigDecimal)input;
            } else {
                bigDecimal = null;
                assert false : "bad ValueSource input type: " + input.getClass().toString();
            }
            int allowedScale = typeInstance.attribute(DecimalAttribute.SCALE);
            int allowedPrecision = typeInstance.attribute(DecimalAttribute.PRECISION);
            if (allowedPrecision < bigDecimal.precision()) {
                throw new AkibanInternalException("precision of " + bigDecimal.precision()
                        + " is greater than " + allowedPrecision + " for value " + bigDecimal);
            }
            if (allowedScale < bigDecimal.scale()) {
                throw new AkibanInternalException("scale of " + bigDecimal.scale()
                        + " is greater than " + allowedScale + " for value " + bigDecimal);
            }
            BigDecimalWrapper wrapper = new BigDecimalWrapperImpl(bigDecimal).round(allowedScale);
            out.putObject(wrapper);
        }
    };

    @Override
    protected boolean tryFromObject(TExecutionContext context, ValueSource in, ValueTarget out) {
        // If the incoming ValueSource is a DECIMAL, *and* it has a cache value (ie an BigDecimalWrapper), then
        // we can just copy the wrapper into the output. If the incoming is a DECIMAL with bytes (its raw form), we
        // can only copy those bytes if the TInstance match -- and super.tryFromObject already makes that check.
        if (in.getType().typeClass() instanceof TBigDecimal && in.hasCacheValue()) {
            BigDecimalWrapper cached = (BigDecimalWrapper) in.getObject();
            out.putObject(new BigDecimalWrapperImpl(cached.asBigDecimal()));
            return true;
        }
        return super.tryFromObject(context, in, out);
    }


    @Override
    public boolean hasFixedSerializationSize(TInstance type) {
        return true;
    }

    @Override
    public int fixedSerializationSize(TInstance type) {
        final int TYPE_SIZE = 4;
        final int DIGIT_PER = 9;
        final int BYTE_DIGITS[] = { 0, 1, 1, 2, 2, 3, 3, 4, 4, 4 };

        final int precision = type.attribute(DecimalAttribute.PRECISION);
        final int scale = type.attribute(DecimalAttribute.SCALE);

        final int intCount = precision - scale;
        final int intFull = intCount / DIGIT_PER;
        final int intPart = intCount % DIGIT_PER;
        final int fracFull = scale / DIGIT_PER;
        final int fracPart = scale % DIGIT_PER;

        return (intFull + fracFull) * TYPE_SIZE +
            BYTE_DIGITS[intPart] + BYTE_DIGITS[fracPart];
    }

}
