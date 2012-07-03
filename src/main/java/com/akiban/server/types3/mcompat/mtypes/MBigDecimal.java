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

import com.akiban.qp.operator.QueryContext;
import com.akiban.server.rowdata.ConversionHelperBigDecimal;
import com.akiban.server.types3.Attribute;
import com.akiban.server.types3.IllegalNameException;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TFactory;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.common.BigDecimalWrapper;
import com.akiban.server.types3.mcompat.MBundle;
import com.akiban.server.types3.pvalue.PBasicValueSource;
import com.akiban.server.types3.pvalue.PBasicValueTarget;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueCacher;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;
import com.akiban.util.AkibanAppender;

import java.math.BigDecimal;
import java.util.Arrays;

public class MBigDecimal extends TClass {

    public enum Attrs implements Attribute {
        PRECISION, SCALE
    }

    private static final int MAX_INDEX = 0;
    private static final int MIN_INDEX = 1;
    
    public MBigDecimal(){
        super(MBundle.INSTANCE.id(), "decimal", Attrs.class, 1, 1, 8, PUnderlying.BYTES);
    }

    @Override
    public DataTypeDescriptor dataTypeDescriptor(TInstance instance) {
        // stolen from TypesTranslation
        int precision = instance.attribute(Attrs.PRECISION);
        int scale = instance.attribute(Attrs.SCALE);
        return new DataTypeDescriptor(TypeId.DECIMAL_ID, precision, scale, instance.nullability(),
                DataTypeDescriptor.computeMaxWidth(precision, scale));
    }

    @Override
    public void writeCanonical(PValueSource in, TInstance typeInstance, PValueTarget out) {
        PValueTargets.copyFrom(in, out, cacher, typeInstance);
    }

    public static String getNum(int scale, int precision)
    {
        assert precision >= scale : "precision has to be >= scale";
        
        char val[] = new char[precision + 1];
        Arrays.fill(val, '9');
        val[precision - scale] = '.';
        
        return new String(val);
    }

    @Override
    public TInstance instance() {
        return instance(10, 0);
    }

    @Override
    public void putSafety(QueryContext context,
                          TInstance sourceInstance,
                          PValueSource sourceValue,
                          TInstance targetInstance,
                          PValueTarget targetValue)
    {
        MBigDecimalWrapper num = (MBigDecimalWrapper) sourceValue.getObject();
        int pre = num.getPrecision();
        int scale = num.getScale();
        
        int expectedPre = targetInstance.attribute(Attrs.PRECISION);
        int expectedScale = targetInstance.attribute(Attrs.SCALE);

        BigDecimalWrapper meta[] = (BigDecimalWrapper[]) targetInstance.getMetaData();
        
        if (meta == null)
        {
            // compute the max value:
            meta = new BigDecimalWrapper[2];
            meta[MAX_INDEX] = new MBigDecimalWrapper(getNum(expectedScale, expectedPre));
            meta[MIN_INDEX] = meta[MAX_INDEX].negate();
       
            targetInstance.setMetaData(meta);
        }

        if (num.compareTo(meta[MAX_INDEX]) >= 0)
            targetValue.putObject(meta[MAX_INDEX]);
        else if (num.compareTo(meta[MIN_INDEX]) <= 0)
            targetValue.putObject(meta[MIN_INDEX]);
        else if (scale > expectedScale) // check the sacle
            targetValue.putObject(num.round(expectedPre, expectedScale));
        else // else put the original value
            targetValue.putValueSource(sourceValue);
    }

    @Override
    public TFactory factory() {
        return new MNumericFactory(this);
    }

    @Override
    protected void validate(TInstance instance) {
        int precision = instance.attribute(Attrs.PRECISION);
        int scale = instance.attribute(Attrs.SCALE);
        if (precision < scale)
            throw new IllegalNameException("precision must be >= scale");
    }

    @Override
    protected TInstance doPickInstance(TInstance instance0, TInstance instance1) {
        // Determine precision of TInstance
        /*switch (mode) {
            case COMBINE:
            case CHOOSE:
        }*/
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public static final PValueCacher<BigDecimalWrapper> cacher = new PValueCacher<BigDecimalWrapper>() {

        @Override
        public void cacheToValue(BigDecimalWrapper bdw, TInstance tInstance, PBasicValueTarget target) {
            BigDecimal bd = bdw.asBigDecimal();
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
    };
}