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

public abstract class Explainer<T>
{    
    public abstract Type getType();
    
    /**
     * 
     * @return a map of this object's attributes if it's an OperationExplainer
     *         a primitive object (Integer, Double, etc ...), otherwise.
     */
    public abstract T get();
    
    public abstract boolean hasAttributes();
 
    public abstract boolean addAttribute (Label label, Explainer ex);
    
    @Override
    public final boolean equals (Object o)
    {
        if (o != null && o instanceof Explainer)
        {
            return ((Explainer)o).get() == get();
        }
        else
            return false;
    }
    
    @Override
    public final int hashCode ()
    {
        return get().hashCode();
    }
}
