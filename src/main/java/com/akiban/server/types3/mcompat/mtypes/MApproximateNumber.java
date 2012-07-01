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
import com.akiban.server.error.OutOfRangeException;
import com.akiban.server.types3.TAttributeValues;
import com.akiban.server.types3.TAttributesDeclaration;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TFactory;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.common.types.DoubleAttribute;
import com.akiban.server.types3.mcompat.MBundle;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

public class MApproximateNumber extends TClass
{
    public static final TClass INSTANCE = new MApproximateNumber();
    
    public static final int DEFAULT_DOUBLE_PRECISION = -1;
    public static final int DEFAULT_DOUBLE_SCALE = -1;

    private static final int MAX_INDEX = 0;
    private static final int MIN_INDEX = 1;
    
    public static double round(TInstance instance, double val)
    {
        assert instance.typeClass() instanceof MApproximateNumber : "instance has to be of type MDouble";

        // meta data
        double meta[] = (double[])instance.getMetaData();
        
        
        int precision = instance.attribute(DoubleAttribute.PRECISION);
        int scale = instance.attribute(DoubleAttribute.SCALE);

        if (meta == null)
        {
            boolean neg;
            String st = Double.toString((neg = val < 0) ? -val : val);
            int point = st.indexOf('.');

            // check the digits before the decimal point
            if (point > precision - scale)
            {
                // cache the max value
                meta = new double[2];
                meta[MAX_INDEX] = Double.parseDouble(MBigDecimal.getNum(scale, precision));
                meta[MIN_INDEX] = meta[MAX_INDEX] * -1;
                instance.setMetaData(meta);
                return neg ? meta[MIN_INDEX] : meta[MAX_INDEX];
            }

            // check the scale
            if (point >= 0)
            {
                int lastDigit = scale + point;

                // actual length is longer than expected, then trucate/round it
                if (st.length() > lastDigit)
                {
                    double factor = Math.pow(10, scale);
                    return  Math.round(factor * val) / factor;
                }
            }
        }
        else
        {
            assert meta.length == 2 : "MDouble's TInstace's meta data should be Double[2]";
            
            if (Double.compare(Math.abs(val), meta[MAX_INDEX]) >= 0)
                return meta[MAX_INDEX];
            
            // check the scale
            double factor = Math.pow(10, scale);
            return  Math.round(factor * val) / factor;
        }
        return val;
    }

    @Override
    public void putSafety(QueryContext context, 
                          TInstance sourceInstance,
                          PValueSource sourceValue,
                          TInstance targetInstance,
                          PValueTarget targetValue)
    {
        double raw = sourceValue.getDouble();
        double rounded = round(targetInstance, raw);

        // TODO: in strict (My)SQL mode, this would be an error
        // NOT a warning
        if (Double.compare(raw, rounded) != 0)
            context.warnClient(new OutOfRangeException(Double.toString(raw)));

        targetValue.putDouble(rounded);
    }

    private class DoubleFactory implements TFactory
    {
        @Override
        public TInstance create(TAttributesDeclaration declaration)
        {
            // DOUBLE could have 0 attributes
            TAttributeValues values = declaration.validate(2, 0);
            return instance(
                    values.intAt(DoubleAttribute.PRECISION, DEFAULT_DOUBLE_PRECISION),
                    values.intAt(DoubleAttribute.SCALE, DEFAULT_DOUBLE_SCALE));
        }
    }

    MApproximateNumber()
    {
        super(MBundle.INSTANCE.id(), "double", 
                DoubleAttribute.class,
                1, 1, 8,
                PUnderlying.DOUBLE);
    }
    
    @Override
    public TInstance instance()
    {
        return instance(DEFAULT_DOUBLE_PRECISION, DEFAULT_DOUBLE_SCALE);
    }
    
    @Override
    public TFactory factory()
    {
        return new DoubleFactory();
    }

    @Override
    protected TInstance doPickInstance(TInstance instance0, TInstance instance1)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected void validate(TInstance instance) {
        // TODO
    }
}
