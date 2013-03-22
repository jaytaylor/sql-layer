
package com.akiban.server.explain.std;

import com.akiban.qp.exec.Plannable;
import com.akiban.server.explain.*;
import com.akiban.server.types3.texpressions.TPreparedExpression;
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
