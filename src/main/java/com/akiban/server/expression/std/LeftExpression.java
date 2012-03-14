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


public class LeftExpression extends AbstractBinaryExpression
{
    @Scalar("left")
    public static final ExpressionComposer COMPOSER = new BinaryComposer()
    {
        @Override
        protected Expression compose(Expression first, Expression second)
        {
            return new LeftExpression(first, second);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 2)
                throw new WrongExpressionArityException(2, argumentTypes.size());
            
            argumentTypes.setType(0, AkType.VARCHAR);
            argumentTypes.setType(1, AkType.LONG);
            
            return argumentTypes.get(0); // this might or might not be the correct precision
        }
        
    };
    
    private static class InnerEvaluation extends AbstractTwoArgExpressionEvaluation
    {
        public InnerEvaluation (List<? extends ExpressionEvaluation> evals)
        {
            super(evals);
        }
        
        @Override
        public ValueSource eval()
        {
            ValueSource strSource = left();
            if (strSource.isNull()) return NullValueSource.only();
           
            ValueSource lenSource = right();
            if (lenSource.isNull()) return NullValueSource.only();
            
            String str = strSource.getString();
            long len = lenSource.getLong();
            
            valueHolder().putString(str.substring(0, (int)(str.length() < len ? str.length() : len)));
            return valueHolder();
        }
    }
    
    @Override
    protected boolean nullIsContaminating()
    {
        return true;
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append("LEFT");
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(childrenEvaluations());
    }
    
    protected LeftExpression (Expression str, Expression len)
    {
        super(AkType.VARCHAR, str, len);
    }
}
