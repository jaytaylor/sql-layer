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

package com.foundationdb.server.explain;

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
