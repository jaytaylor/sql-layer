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
import com.foundationdb.server.expression.*;
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.sql.StandardException;
import java.util.List;

public class PeriodDiffExpression extends AbstractBinaryExpression {
    @Scalar("period_diff")
    public static final ExpressionComposer COMPOSER = new InternalComposer();

    @Override
    public String name() {
        return "PERIOD_DIFF";
    }
    
    private static class InternalComposer extends BinaryComposer
    {
        @Override
        protected Expression compose(Expression first, Expression second, ExpressionType firstType, ExpressionType secondType, ExpressionType resultType)
        {
            return new PeriodDiffExpression(first, second);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            int argc = argumentTypes.size();
            if (argc != 2)
                throw new WrongExpressionArityException(2, argc);
            
            for (int i = 0; i < argc; i++)
                argumentTypes.setType(i, AkType.LONG);
            
            // Return a number of months
            return ExpressionTypes.LONG;
        }
    }
    
    private static class InnerEvaluation extends AbstractTwoArgExpressionEvaluation
    {
                
        public InnerEvaluation(List<? extends ExpressionEvaluation> childrenEval) 
        {
            super(childrenEval);
        }
        
        @Override
        public ValueSource eval()
        {
            if (left().isNull() || right().isNull())
                return NullValueSource.only();
            
            // COMPATIBILITY: MySQL currently has undefined behavior for negative numbers
            // Our behavior follows our B.C. year numbering (-199402 + 1 = -199401)
            long periodLeft = left().getLong(), periodRight = right().getLong();
                        
            long result = PeriodAddExpression.fromPeriod(periodLeft) - PeriodAddExpression.fromPeriod(periodRight);
            valueHolder().putLong(result);
            return valueHolder();
        }
        
    }
    
    protected PeriodDiffExpression(Expression leftOperand, Expression rightOperand)
    {
        super(AkType.LONG, leftOperand, rightOperand);
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
