
package com.akiban.server.explain.std;

import com.akiban.qp.exec.Plannable;
import com.akiban.server.expression.Expression;
import com.akiban.server.explain.*;
import java.util.Arrays;
import java.util.List;

public class ExpressionExplainer extends CompoundExplainer
{  
    public ExpressionExplainer(Type type, String name, ExplainContext context, List<? extends Expression> exs)
    {
        super(checkType(type), buildMap(name, context, exs));
    }
     
    public ExpressionExplainer(Type type, String name, ExplainContext context, Expression ... operand)
    {
        this(type, name, context, Arrays.asList(operand));
    }
        
    private static Attributes buildMap(String name, ExplainContext context, List<? extends Expression> exs)
    {
        Attributes states = new Attributes();
        states.put(Label.NAME, PrimitiveExplainer.getInstance(name));
        if (exs != null)
            for (Expression ex : exs)
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
