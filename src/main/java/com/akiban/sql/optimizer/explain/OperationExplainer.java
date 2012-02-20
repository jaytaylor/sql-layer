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

import com.akiban.sql.optimizer.explain.Attributes;
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.sql.optimizer.explain.Type;

public  class OperationExplainer implements Explainer<Attributes>
{     
    private final Type type; 
    private Attributes states;
        
    public OperationExplainer (Type type, Attributes states)
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
       
    @Override
    public final boolean hasChildren()
    {
        return !(states == null || states.isEmpty());
    }  
    
    // TODO:
    // could return a new OperationExplainer 
    // if we want to make OperationExplainer immutable.
//    public final boolean addAttribute (Label label, Explainer ex)
//    {
//        if (states.containsKey(label)) return false;
//        states.put(label, ex);
//        return true;
//    }    
}
