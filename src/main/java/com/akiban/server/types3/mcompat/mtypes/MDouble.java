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

import com.akiban.server.types3.TAttributeValues;
import com.akiban.server.types3.TAttributesDeclaration;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TFactory;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.common.types.DoubleAttribute;
import com.akiban.server.types3.mcompat.MBundle;
import com.akiban.server.types3.pvalue.PUnderlying;

public class MDouble extends TClass
{
    public static final TClass INSTANCE = new MDouble();
    
    public static final int DEFAULT_DOUBLE_PRECISION = -1;
    public static final int DEFAULT_DOUBLE_SCALE = -1;
    
    private static final TInstance PLAIN_DOUBLE = new TInstance(INSTANCE, 
            DEFAULT_DOUBLE_PRECISION,
            DEFAULT_DOUBLE_SCALE);
    
    private static class DoubleFactory implements TFactory
    {
        private final TClass tclass;
        
        DoubleFactory (TClass tclass)
        {
            this.tclass = tclass;
        }
        
        @Override
        public TInstance create(TAttributesDeclaration declaration)
        {
            // DOUBLE could have 0 attributes
            TAttributeValues values = declaration.validate(2, 0);
            return new TInstance(tclass,
                    values.intAt(DoubleAttribute.PRECISION, DEFAULT_DOUBLE_PRECISION),
                    values.intAt(DoubleAttribute.SCALE, DEFAULT_DOUBLE_SCALE));
        }
        
    }
    
    MDouble()
    {
        super(MBundle.INSTANCE.id(), "double", 
                DoubleAttribute.values(),
                1, 1, 8,
                PUnderlying.DOUBLE);
    }
    
    @Override
    public TInstance instance()
    {
        return PLAIN_DOUBLE;
    }
    
    @Override
    public TInstance instance(int precision, int scale)
    {
        return new TInstance(this, precision, scale);
    }
    
    @Override
    public TFactory factory()
    {
        return new DoubleFactory(this);
    }

    @Override
    protected TInstance doPickInstance(TInstance instance0, TInstance instance1)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
