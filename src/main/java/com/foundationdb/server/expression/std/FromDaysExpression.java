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
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.expression.ExpressionType;
import com.foundationdb.server.expression.TypesList;
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.extract.Extractors;
import com.foundationdb.sql.StandardException;
import org.joda.time.DateTimeZone;


public class FromDaysExpression extends AbstractUnaryExpression
{
    @Scalar("FROM_DAYS")
    public static final ExpressionComposer COMPOSER = new UnaryComposer()
    {
        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType)
        {
            return new FromDaysExpression(argument);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());
            
            argumentTypes.setType(0, AkType.LONG);
            return ExpressionTypes.DATE;
        }
        
    };
    
    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private static final long BEGINNING = Extractors.getLongExtractor(AkType.DATE).stdLongToUnix(33, DateTimeZone.UTC);
        private  static final long FACTOR = 3600L * 1000 * 24;
        
        InnerEvaluation (ExpressionEvaluation eval)
        {
            super(eval);
        }
        
        @Override
        public ValueSource eval()
        {
            ValueSource days = operand();
            if (days.isNull())
                return NullValueSource.only();
            
            long val = days.getLong();
            valueHolder().putDate( val < 366
                    ? 0L
                    :Extractors.getLongExtractor(AkType.DATE).unixToStdLong(days.getLong() * FACTOR + BEGINNING, DateTimeZone.UTC));
            return valueHolder();
        }    
    }
    
    FromDaysExpression (Expression arg)
    {
        super(AkType.DATE, arg);
    }
    
    @Override
    public String name()
    {
        return "FROM_DAYS";
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(operandEvaluation());
    }
    
}
