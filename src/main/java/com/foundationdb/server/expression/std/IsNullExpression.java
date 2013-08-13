/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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


package com.foundationdb.server.expression.std;

import com.foundationdb.server.error.WrongExpressionArityException;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.expression.ExpressionType;
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.util.BoolValueSource;
import com.foundationdb.sql.StandardException;
import com.foundationdb.server.expression.TypesList;


public class IsNullExpression extends AbstractUnaryExpression
{
    @Scalar("isnull")
    public static final ExpressionComposer COMPOSER = new  UnaryComposer ()
    {
        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType) 
        {
            return new IsNullExpression(argument);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());
            return ExpressionTypes.BOOL;
        }
    };
        
    private static final class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        public InnerEvaluation (ExpressionEvaluation ev)
        {
            super(ev);
        }

        @Override
        public ValueSource eval() 
        {
           return BoolValueSource.of(operand().isNull()); 
        }
         
     }
    
    public IsNullExpression (Expression e)
    {
        super(AkType.BOOL, e);
    }

    @Override
    public String name() 
    {
        return "ISNULL";
    }

    @Override
    public ExpressionEvaluation evaluation() 
    {
        return new InnerEvaluation(operandEvaluation());
    }
}
