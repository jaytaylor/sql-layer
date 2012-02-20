/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.optimizer.explain;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.LongExtractor;
import java.math.BigDecimal;
import java.math.BigInteger;

public class PrimitiveExplainer<T> extends Explainer
{
    public static PrimitiveExplainer getInstance (String st)
    {
        return new PrimitiveExplainer<String>(Type.STRING, st);
    }
    
    public static PrimitiveExplainer getInstance (double n)
    {
        return new PrimitiveExplainer<Double>(Type.FLOATING_POINT, n);
    }
    
    public static PrimitiveExplainer getInstance (long n)
    {
        return new PrimitiveExplainer<Long>(Type.EXACT_NUMERIC, n);
    }
    
    public static PrimitiveExplainer getInstance (boolean n)
    {
        return new PrimitiveExplainer<Boolean>(Type.EXACT_NUMERIC, n);
    }
    
    public static PrimitiveExplainer getInstance(BigInteger num)
    {
        return PrimitiveExplainer.getInstance(num.longValue());
    }
    
    public static PrimitiveExplainer getInstance(BigDecimal num)
    {
        return PrimitiveExplainer.getInstance(num.doubleValue());
    }

    public static PrimitiveExplainer getInstance(Object o)
    {
        if (o instanceof BigDecimal) return getInstance((BigDecimal)o);
        else if (o instanceof BigInteger) return getInstance((BigInteger)o);
        else if (o instanceof String) return getInstance((String)o);
        else throw new UnsupportedOperationException("Explainer for type " + o.getClass() + " is not supported yet");
    }

    public static PrimitiveExplainer getInstance(ValueSource source)
    {
        AkType type = source.getConversionType();
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
    private final T o;
    
    public PrimitiveExplainer (Type type, T o)
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
    public T get()
    {
        return o;
    }

    @Override
    public boolean hasAttributes()
    {
        return false;
    }

    @Override
    public boolean addAttribute(Label label, Explainer ex)
    {
        throw new UnsupportedOperationException("Primitive Explainer cannot have any attribute.");
    }
}
