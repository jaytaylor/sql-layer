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

package com.akiban.server.types3.mcompat.mtypes;

import com.akiban.server.rowdata.ConversionHelperBigDecimal;
import com.akiban.server.types3.Attribute;
import com.akiban.server.types3.IllegalNameException;
import com.akiban.server.types3.PValueIO;
import com.akiban.server.types3.SimplePValueIO;
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
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;
import com.akiban.util.AkibanAppender;

import java.math.BigDecimal;

public class MBigDecimal extends TClassBase {

    public enum Attrs implements Attribute {
        PRECISION, SCALE
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
    protected PValueIO getPValueIO() {
        return valueIO;
    }

    public MBigDecimal(String name, int defaultVarcharLen){
        super(MBundle.INSTANCE.id(), name, AkCategory.DECIMAL, Attrs.class, NumericFormatter.FORMAT.BIGDECIMAL, 1, 1, 8,
                PUnderlying.BYTES, TParsers.DECIMAL, defaultVarcharLen);
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
            int precisionOfSmallerScale, precisionOfLargerScale;
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
    };

    private static final PValueIO valueIO = new SimplePValueIO() {
        @Override
        protected void copy(PValueSource in, TInstance typeInstance, PValueTarget out) {
            byte[] bytes;
            if (in.hasRawValue()) {
                bytes = in.getBytes();
            }
            else {
                BigDecimalWrapper value = (BigDecimalWrapper) in.getObject();
                int precision = typeInstance.attribute(Attrs.PRECISION);
                int scale = typeInstance.attribute(Attrs.SCALE);
                bytes = ConversionHelperBigDecimal.bytesFromObject(
                        value.asBigDecimal(),
                        precision,
                        scale);
            }
            out.putBytes(bytes);
        }
    };
}