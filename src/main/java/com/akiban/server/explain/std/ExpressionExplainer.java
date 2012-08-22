/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
