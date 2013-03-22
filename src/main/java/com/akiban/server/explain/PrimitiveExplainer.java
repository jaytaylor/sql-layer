
package com.akiban.server.explain;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.LongExtractor;
import java.math.BigDecimal;
import java.math.BigInteger;

public class PrimitiveExplainer extends Explainer
{
    public static PrimitiveExplainer getInstance (String st)
    {
        return new PrimitiveExplainer(Type.STRING, st);
    }
    
    public static PrimitiveExplainer getInstance (double n)
    {
        return new PrimitiveExplainer(Type.FLOATING_POINT, n);
    }
    
    public static PrimitiveExplainer getInstance (long n)
    {
        return new PrimitiveExplainer(Type.EXACT_NUMERIC, n);
    }
    
    public static PrimitiveExplainer getInstance (boolean n)
    {
        return new PrimitiveExplainer(Type.BOOLEAN, n);
    }
    
    public static PrimitiveExplainer getInstance(BigInteger num)
    {
        return PrimitiveExplainer.getInstance(num.longValue());
    }
    
    public static PrimitiveExplainer getInstance(BigDecimal num)
    {
        return PrimitiveExplainer.getInstance(num.doubleValue());
    }

    private final Type type;
    private final Object o;
    
    public PrimitiveExplainer (Type type, Object o)
    {
        if (type.generalType() != Type.GeneralType.SCALAR_VALUE)
            throw new IllegalArgumentException("Type must be a SCALAR_VALUE");
        this.type = type;
        this.o = o;
    }       
    
    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public Object get()
    {
        return o;
    }
}
