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

package com.foundationdb.server.expression.std;

import com.foundationdb.server.error.WrongExpressionArityException;
import com.foundationdb.server.expression.*;
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.sql.StandardException;
import java.util.List;

public class PowExpression extends AbstractBinaryExpression
{
    @Scalar({"pow", "power"})
    public static final ExpressionComposer POW = new BinaryComposer() 
    {
        @Override
        protected Expression compose(Expression first, Expression second, ExpressionType firstType, ExpressionType secondType, ExpressionType resultType)
        {
            return new PowExpression(first, second);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 2)
                throw new WrongExpressionArityException(2, argumentTypes.size());
                
            argumentTypes.setType(0, AkType.DOUBLE);
            argumentTypes.setType(1, AkType.DOUBLE);
            
            return ExpressionTypes.DOUBLE;
        }
    };

    @Override
    public String name() {
        return "POWER";
    }
    
    private static class InnerEvaluation extends AbstractTwoArgExpressionEvaluation
    {
        public InnerEvaluation (List<? extends ExpressionEvaluation> evals)
        {
            super(evals);
        }
        
        @Override
        public ValueSource eval()
        {
            ValueSource base, exp;
            if ((base = left()).isNull() || (exp = right()).isNull())
                return NullValueSource.only();
            
            valueHolder().putDouble(Math.pow(base.getDouble(), exp.getDouble()));
            return valueHolder();
        }
    }
    
    PowExpression (Expression left, Expression right)
    {
        super(AkType.DOUBLE, left, right);
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
        return new InnerEvaluation(childrenEvaluations());
    }
}
