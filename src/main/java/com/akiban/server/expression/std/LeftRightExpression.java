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


public class LeftRightExpression extends AbstractBinaryExpression
{
    @Override
    public String name() {
        return op.name();
    }
    protected enum Op
    {
        LEFT
        {
            @Override
            String getSubstring(String st, int length)
            {
                 return st.substring(0, length);
            }
        },
        RIGHT
        {
            @Override
            String getSubstring(String st, int length)
            {
                 return st.substring(st.length() - length, st.length());
            }
        };
        
        abstract String getSubstring(String st, int length);
    }
    
    @Scalar("getLeft")
    public static final ExpressionComposer LEFT_COMPOSER = new InnerComposer(Op.LEFT);
    
    @Scalar("getRight")
    public static final ExpressionComposer RIGHT_COMPOSER = new InnerComposer(Op.RIGHT);
    
    private static class InnerComposer extends BinaryComposer
    {
        private final Op op;
        
        InnerComposer (Op op)
        {
            this.op = op;
        }
        
        @Override
        protected Expression compose(Expression first, Expression second, ExpressionType firstType, ExpressionType secondType, ExpressionType resultType)
        {
            return new LeftRightExpression(first, second, op);
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
        private final Op op;
        public InnerEvaluation (List<? extends ExpressionEvaluation> evals, Op op)
        {
            super(evals);
            this.op = op;
        }
        
        @Override
        public ValueSource eval()
        {
            ValueSource strSource = left();
            if (strSource.isNull()) return NullValueSource.only();
           
            ValueSource lenSource = right();
            if (lenSource.isNull()) return NullValueSource.only();
            
            String str = strSource.getString();
            int len = (int)lenSource.getLong();
            len = len < 0
                    ? 0
                    : len > str.length() ? str.length() : len;

            valueHolder().putString(op.getSubstring(str, len));
            return valueHolder();
        }
    }
    
    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append(op);
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(childrenEvaluations(), op);
    }
    
    protected LeftRightExpression (Expression str, Expression len, Op op)
    {
        super(AkType.VARCHAR, str, len);
        this.op = op;
    }
    
    private final Op op;
}
