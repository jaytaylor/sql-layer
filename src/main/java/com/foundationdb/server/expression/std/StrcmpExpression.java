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
import com.foundationdb.server.expression.TypesList;
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.sql.StandardException;
import java.util.List;
import java.lang.String;
        
public class StrcmpExpression extends AbstractBinaryExpression 
{
     @Scalar("strcmp")
     public static final ExpressionComposer COMPOSER = new BinaryComposer() 
     {

        @Override
        protected Expression compose(Expression first, Expression second, ExpressionType firstType, ExpressionType secondType, ExpressionType resultType)
        {
            return new StrcmpExpression(first, second);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            int argc = argumentTypes.size();
            if (argc != 2)
                throw new WrongExpressionArityException(2, argc);
            
            for (int i = 0; i < argc; i++)
                argumentTypes.setType(i, AkType.VARCHAR);
            
            return ExpressionTypes.INT;
        }
    };

    @Override
    public String name() {
        return "STRCMP";
    }

     private static class InnerEvaluation extends AbstractTwoArgExpressionEvaluation
     {
        public InnerEvaluation (AkType type, List<? extends ExpressionEvaluation> childrenEval)
        {
            super(childrenEval);
        }
        
        @Override
        public ValueSource eval()
        {
            if (this.left().isNull() || this.right().isNull())
                return NullValueSource.only();
            
            String stringFirst = left().getString();
            String stringSecond = right().getString();
            
            int result = stringFirst.compareTo(stringSecond);
            
            valueHolder().putInt(Integer.signum(result) );
            return valueHolder();
        }
         
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append(name());
    }

    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(valueType(), childrenEvaluations());
    }
    
    protected StrcmpExpression(Expression first, Expression second)
    {
        super(AkType.VARCHAR, first, second);
    }
     
}
