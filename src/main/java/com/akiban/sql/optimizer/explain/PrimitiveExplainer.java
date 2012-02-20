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

public class PrimitiveExplainer<T> implements Explainer
{
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
    public boolean hasChildren()
    {
        return false;
    }
}
