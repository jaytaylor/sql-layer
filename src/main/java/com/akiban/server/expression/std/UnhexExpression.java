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

import com.akiban.qp.operator.QueryContext;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.*;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import com.akiban.util.ByteSource;
import com.akiban.util.Strings;
import com.akiban.util.WrappingByteSource;

public class UnhexExpression extends AbstractUnaryExpression
{
    @Scalar("unhex")
    public static final ExpressionComposer COMPOSER = new UnaryComposer()
    {
        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType)
        {
            return new UnhexExpression(argument);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());
            
            argumentTypes.setType(0, AkType.VARCHAR);            
            return ExpressionTypes.varbinary(argumentTypes.get(0).getPrecision() / 2 + 1);
        }
        
    };
    
    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private static final ByteSource EMPTY = new WrappingByteSource(new byte[0]);
        
        InnerEvaluation(ExpressionEvaluation arg)
        {
            super(arg);
        }
       
        @Override
        public ValueSource eval()
        {
            ValueSource source = operand();
            if (source.isNull())
                return NullValueSource.only();
            
            String st = source.getString();
            if (st.isEmpty())
                valueHolder().putVarBinary(EMPTY);
            else
                try
                {
                    valueHolder().putVarBinary(Strings.parseHexWithout0x(st));
                }
                catch (InvalidOperationException e)
                {
                    QueryContext qc = queryContext();
                    if (qc != null)
                        qc.warnClient(e);
                    return NullValueSource.only();
                }
            
            return valueHolder();
        }
    }
    
    UnhexExpression (Expression arg)
    {
        super(AkType.VARBINARY, arg);
    }
    
    @Override
    public String name()
    {
        return "UNHEX";
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(operandEvaluation());
    }
}
