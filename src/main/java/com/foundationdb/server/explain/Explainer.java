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

public abstract class Explainer 
{
    public abstract Type getType();
    
    /**
     * 
     * @return a map of this object's attributes if it's an OperationExplainer
     *         a primitive object (Integer, Double, etc ...), otherwise.
     */
    public abstract Object get();
    
    @Override
    public final boolean equals (Object o)
    {
        if (o != null && o instanceof Explainer)
        {
            Explainer other = (Explainer)o;
            return (getType().equals(other.getType()) &&
                    get().equals(other.get()));
        }
        else
            return false;
    }
    
    @Override
    public final int hashCode ()
    {
        return getType().hashCode() + get().hashCode();
    }
}
