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

package com.foundationdb.server.explain.std;

import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import java.util.Arrays;
import java.util.List;

public class TExpressionExplainer extends CompoundExplainer
{  
    public TExpressionExplainer(Type type, String name, ExplainContext context, List<? extends TPreparedExpression> exs)
    {
        super(checkType(type), buildMap(name, context, exs));
    }
     
    public TExpressionExplainer(Type type, String name, ExplainContext context, TPreparedExpression ... operand)
    {
        this(type, name, context, Arrays.asList(operand));
    }
        
    private static Attributes buildMap(String name, ExplainContext context, List<? extends TPreparedExpression> exs)
    {
        Attributes states = new Attributes();
        states.put(Label.NAME, PrimitiveExplainer.getInstance(name));
        if (exs != null)
            for (TPreparedExpression ex : exs)
                states.put(Label.OPERAND, ex.getExplainer(context));
        return states;
    }
    
    private static Type checkType(Type type)
    {
        if (type.generalType() != Type.GeneralType.EXPRESSION)
            throw new IllegalArgumentException("Expected sub-category of Type.GeneralType.EXPRESSION but got " + type);
        return type;
    }
}
