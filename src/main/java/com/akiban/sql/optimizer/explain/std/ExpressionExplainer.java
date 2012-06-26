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

package com.akiban.sql.optimizer.explain.std;

import com.akiban.server.expression.Expression;
import com.akiban.sql.optimizer.explain.*;
import java.util.Arrays;
import java.util.List;

public class ExpressionExplainer extends OperationExplainer
{  
    public ExpressionExplainer (Type type, String name, List<? extends Expression> exs)
    {
        super(checkType(type), buildMap(name, exs));
    }
     
    public ExpressionExplainer (Type type, String name, Expression ... operand)
    {
        this(type, name, Arrays.asList(operand));
    }
        
    private static Attributes buildMap (String name, List<? extends Expression> exs)
    {
        Attributes states = new Attributes();
        states.put(Label.NAME, PrimitiveExplainer.getInstance(name));
        if (exs != null)
            for (Expression ex : exs)
                states.put(Label.OPERAND, ex.getExplainer());
        return states;
    }
    
    private static Type checkType(Type type)
    {
        if (type.generalType() != Type.GeneralType.EXPRESSION)
            throw new IllegalArgumentException("Expected sub-category of Type.GeneralType.EXPRESSION but got " + type);
        return type;
    }
}
