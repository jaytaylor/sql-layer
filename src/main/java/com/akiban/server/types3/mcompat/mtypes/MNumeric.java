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
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TFactory;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TypeDeclarationException;
import com.akiban.server.types3.common.types.NumericAttribute;
import com.akiban.server.types3.common.types.SimpleDtdTClass;
import com.akiban.server.types3.mcompat.MBundle;
import com.akiban.server.types3.mcompat.mcasts.Cast_From_Varchar;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.sql.types.TypeId;

public abstract class MNumeric extends SimpleDtdTClass {

    protected MNumeric(String name, int serializationSize, PUnderlying pUnderlying, int defaultWidth) {
        super(MBundle.INSTANCE.id(), name, 
                NumericAttribute.class,
                1, 1, serializationSize, 
                pUnderlying, inferTypeid(name));
        this.defaultWidth = defaultWidth;
    }

    private static TypeId inferTypeid(String name) {
        if (name.contains("unsigned"))
            return TypeId.INTEGER_UNSIGNED_ID;
        else
            return TypeId.INTEGER_ID;
    }

    @Override
    public TInstance instance() {
        return instance(defaultWidth);
    }

    @Override
    public void putSafety(QueryContext context, 
                          TInstance sourceInstance,
                          PValueSource sourceValue,
                          TInstance targetInstance,
                          PValueTarget targetValue)
    {
       assert sourceInstance.typeClass() instanceof MNumeric
                    && targetInstance.typeClass() instanceof MNumeric
               : "expected instances of mcompat.types.MNumeric";
       targetValue.putValueSource(sourceValue);
    }
        
    @Override
    protected void validate(TInstance instance) {
        int m = instance.attribute(NumericAttribute.WIDTH);
        if (m < 0 || m > 255)
            throw new TypeDeclarationException("width must be 0 < M < 256");
    }

    @Override
    public TFactory factory() {
        return new MNumericFactory(this);
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
    
    private final int defaultWidth;
    
    // numeric types
    // TODO verify default widths
    public static final MNumeric TINYINT = new MNumeric("tinyint", 1, PUnderlying.INT_8, 5) 
    {
        @Override
        protected void doFromObject(TExecutionContext contextForErrors, PValueSource in, PValueTarget out)
        {
            Cast_From_Varchar.TO_TINYINT.evaluate(contextForErrors, in, out);
        }
    };

    public static final MNumeric TINYINT_UNSIGNED = new MNumeric("tinyintunsigned", 4, PUnderlying.INT_16, 4)
    {
        @Override
        protected void doFromObject(TExecutionContext contextForErrors, PValueSource in, PValueTarget out)
        {
            Cast_From_Varchar.TO_UNSIGNED_TINYINT.evaluate(contextForErrors, in, out);
        }
    };

    public static final MNumeric SMALLINT = new MNumeric("smallint", 2, PUnderlying.INT_16, 7)
    {
        @Override
        protected void doFromObject(TExecutionContext contextForErrors, PValueSource in, PValueTarget out)
        {
            Cast_From_Varchar.TO_SMALLINT.evaluate(contextForErrors, in, out);
        }
    };
    
    public static final MNumeric SMALLINT_UNSIGNED = new MNumeric("smallintunsigned", 4, PUnderlying.INT_32, 6)
    {
        @Override
        protected void doFromObject(TExecutionContext contextForErrors, PValueSource in, PValueTarget out)
        {
            Cast_From_Varchar.TO_UNSIGNED_SMALLINT.evaluate(contextForErrors, in, out);
        }
    };

    public static final MNumeric MEDIUMINT = new MNumeric("mediumint", 3, PUnderlying.INT_32, 9)
    {
        @Override
        protected void doFromObject(TExecutionContext contextForErrors, PValueSource in, PValueTarget out)
        {
            Cast_From_Varchar.TO_MEDIUMINT.evaluate(contextForErrors, in, out);
        }
    };

    public static final MNumeric MEDIUMINT_UNSIGNED = new MNumeric("mediumintunsigned", 8, PUnderlying.INT_64, 8)
    {
        @Override
        protected void doFromObject(TExecutionContext contextForErrors, PValueSource in, PValueTarget out)
        {
            Cast_From_Varchar.TO_UNSIGNED_MEDIUMINT.evaluate(contextForErrors, in, out);
        }
    };

    public static final MNumeric INT = new MNumeric("int", 4, PUnderlying.INT_32, 11)
    {
        @Override
        protected void doFromObject(TExecutionContext contextForErrors, PValueSource in, PValueTarget out)
        {
            Cast_From_Varchar.TO_INT.evaluate(contextForErrors, in, out);
        }
    };

    public static final MNumeric INT_UNSIGNED = new MNumeric("intunsigned", 8, PUnderlying.INT_64, 10)
    {
        @Override
        protected void doFromObject(TExecutionContext contextForErrors, PValueSource in, PValueTarget out)
        {
            Cast_From_Varchar.TO_UNSIGNED_INT.evaluate(contextForErrors, in, out);
        }
    };

    public static final MNumeric BIGINT = new MNumeric("bigint", 8, PUnderlying.INT_64, 21)
    {
        @Override
        protected void doFromObject(TExecutionContext contextForErrors, PValueSource in, PValueTarget out)
        {
            Cast_From_Varchar.TO_BIGINT.evaluate(contextForErrors, in, out);
        }
    };

    public static final MNumeric BIGINT_UNSIGNED = new MNumeric("bigintunsigned", 8, PUnderlying.INT_64, 20)
    {
        @Override
        protected void doFromObject(TExecutionContext contextForErrors, PValueSource in, PValueTarget out)
        {
            Cast_From_Varchar.TO_UNSIGNED_BIGINT.evaluate(contextForErrors, in, out);
        }
    };
    
    public static final TClass DECIMAL = new MBigDecimal();

    public static final TClass DECIMAL_UNSIGNED = new MBigDecimal();

    protected abstract void doFromObject(TExecutionContext contextForErrors, PValueSource in, PValueTarget out);
    @Override
    public void fromObject(TExecutionContext contextForErrors,
                           PValueSource in, TInstance outTInstance, PValueTarget out)
    {
        switch(in.getUnderlyingType())
        {
            case STRING:
                doFromObject(contextForErrors, in, out);
                break;
            default:
                throw new IllegalArgumentException("Unexpected PUnderlying: " + in.getUnderlyingType());
        }
    }
}