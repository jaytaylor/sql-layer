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

import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.error.InvalidCharToNumException;
import com.foundationdb.server.error.WrongExpressionArityException;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.expression.ExpressionType;
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.extract.Extractors;
import com.foundationdb.server.types.util.ValueHolder;
import com.foundationdb.sql.StandardException;
import com.foundationdb.server.expression.TypesList;

public class InetatonExpression extends AbstractUnaryExpression
{   
    @Scalar ("inet_aton")
    public static final ExpressionComposer COMPOSER = new UnaryComposer ()
    {
        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType)
        {
            return new InetatonExpression(argument);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());
            argumentTypes.setType(0, AkType.VARCHAR);
            return ExpressionTypes.LONG;
        }
    };

    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private static final long FACTORS[] = {16777216L,  65536, 256};        
        public InnerEvaluation (ExpressionEvaluation ev)
        {
            super(ev);
        }

        @Override
        public ValueSource eval()
        {
            String strs[];
            if ((strs = Extractors.getStringExtractor().getObject(operand()).split("\\.")).length > 4)
                return NullValueSource.only();
            try
            {
                short num = Short.parseShort(strs[strs.length-1]);                
                long sum = num;
                if (sum < 0 || sum > 255) return NullValueSource.only();
                else if(strs.length == 1) return new ValueHolder(AkType.LONG, sum);                
                for (int i = 0; i < strs.length -1; ++i)
                {                    
                    if ((num = Short.parseShort(strs[i])) < 0 || num > 255) return NullValueSource.only();
                    sum += num * FACTORS[i];
                }
                valueHolder().putLong(sum);
                return valueHolder();
            }
            catch (NumberFormatException e)
            {
                QueryContext context = queryContext();
                if (context != null) 
                    context.warnClient(new InvalidCharToNumException(e.getMessage()));
                return NullValueSource.only();
            }
        }
    }

    public InetatonExpression (Expression e)
    {
        super(AkType.LONG, e);
    }

    @Override
    public String name()
    {
        return "INET_ATON";
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(operandEvaluation());
    }
}
