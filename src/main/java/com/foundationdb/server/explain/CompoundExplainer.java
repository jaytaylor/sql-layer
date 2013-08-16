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

public class CompoundExplainer extends Explainer 
{
    private final Type type; 
    private Attributes states;
        
    public CompoundExplainer(Type type)
    {
        this(type, new Attributes());
    }

    public CompoundExplainer(Type type, Attributes states)
    {
        this.type = type;
        this.states = states;
    }   
    
    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public Attributes get()
    {
        return states;
    }
    
    public final boolean addAttribute(Label label, Explainer ex)
    {
        if (states.containsKey(label)) return false;
        states.put(label, ex);
        return true;
    }    
}
