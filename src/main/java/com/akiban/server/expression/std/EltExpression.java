/**
 * Copyright (C) 2012 Akiban Technologies Inc.
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

package com.akiban.server.expression.std;

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.TypesList;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import java.util.List;

public class EltExpression extends AbstractCompositeExpression
{
    @Scalar("elt")
    public static final ExpressionComposer COMPOSER = new ExpressionComposer()
    {
        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            int size = argumentTypes.size();
            if (size < 2) throw new WrongExpressionArityException(2, size);
            
            argumentTypes.setType(0, AkType.LONG);
            AkType top = CoalesceExpression.getTopType(argumentTypes.subList(1, size));
            
            int maxPre = 0;
            int maxScale = 0;
            for (int n = 1; n < size; ++n)
            {
                argumentTypes.setType(n, top);
                maxPre = Math.max(maxPre, argumentTypes.get(n).getPrecision());
                maxScale = Math.max(maxScale, argumentTypes.get(n).getScale());
            }
            
            return ExpressionTypes.newType(top, maxPre, maxScale);
        }

        @Override
        public Expression compose(List<? extends Expression> arguments)
        {
            return new EltExpression(arguments);
        }
        
    };
    
    private static class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        InnerEvaluation (List<? extends ExpressionEvaluation> evals)
        {
            super(evals);
        }
        
        @Override
        public ValueSource eval()
        {
            ValueSource indexSource = children().get(0).eval();
            long index;
            if (indexSource.isNull() || (index = indexSource.getLong()) <= 0 || index >= children().size()) 
                return NullValueSource.only();
            return valueHolder().copyFrom(children().get((int)index).eval());
        }
    }

    @Override
    protected boolean nullIsContaminating()
    {
        return false;
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append("ELF");
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(childrenEvaluations());
    }
    
    EltExpression (List<? extends Expression> children)
    {
        super(checkType(children),  children);
    }
    
    private static AkType checkType(List<? extends Expression> children)
    {
        if (children.size() < 2) 
            throw new WrongExpressionArityException(2, children.size());
        else                                    // input types have been "homogenised", so it's
            return children.get(1).valueType(); // safe to just return the 'first' arg in the list
    }
}
