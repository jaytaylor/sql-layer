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

package com.akiban.server.explain;

import com.akiban.qp.rowtype.RowType;
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
        return new PrimitiveExplainer(Type.EXACT_NUMERIC, n);
    }
    
    public static PrimitiveExplainer getInstance(BigInteger num)
    {
        return PrimitiveExplainer.getInstance(num.longValue());
    }
    
    public static PrimitiveExplainer getInstance(BigDecimal num)
    {
        return PrimitiveExplainer.getInstance(num.doubleValue());
    }

    public static PrimitiveExplainer getInstance(RowType type)
    {
        return PrimitiveExplainer.getInstance(type == null ? "NULL" : type.toString());
    }
    
    public static PrimitiveExplainer getInstance(Object o)
    {
        if (o instanceof BigDecimal) return getInstance((BigDecimal)o);
        else if (o instanceof BigInteger) return getInstance((BigInteger)o);
        else if (o instanceof String) return getInstance((String)o);
        else if (o instanceof RowType) return getInstance((RowType)o);
        else throw new UnsupportedOperationException("Explainer for " + o.getClass() + " is not supported yet");
    }

    public static PrimitiveExplainer getInstance(ValueSource source)
    {
        AkType type = source.getConversionType();
        if (type == AkType.NULL) return getInstance("NULL");
        switch(type.underlyingType())
        {
            case LONG_AKTYPE:    LongExtractor lExtractor = Extractors.getLongExtractor(type);
                                 return getInstance(lExtractor.getLong(source));
            case FLOAT_AKTYPE:
            case DOUBLE_AKTYPE:  return getInstance(Extractors.getDoubleExtractor().getDouble(source));
            case BOOLEAN_AKTYPE: return getInstance(source.getBool());
            case OBJECT_AKTYPE:  return getInstance(Extractors.getObjectExtractor(type).getObject(source));
            default:             throw new UnsupportedOperationException("Explainer for type " + type + " is not supported yet");                
        }
    }
    
    // TODO:  add more as needed
       
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
