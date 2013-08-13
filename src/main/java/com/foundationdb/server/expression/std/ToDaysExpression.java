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
import com.akiban.server.error.InvalidParameterValueException;
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
import com.akiban.server.types.extract.Extractors;
import com.akiban.sql.StandardException;
import org.joda.time.DateTimeZone;
import org.joda.time.IllegalFieldValueException;


public class ToDaysExpression  extends AbstractUnaryExpression
{   
    @Scalar("to_days")
    public static final ExpressionComposer COMPOSER = new UnaryComposer()
    {
        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType)
        {
            return new ToDaysExpression(argument);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());
            argumentTypes.setType(0, AkType.DATE);
            
            return ExpressionTypes.LONG;
        }
    };

    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private static final long BEGINNING = Extractors.getLongExtractor(AkType.DATE).stdLongToUnix(33, DateTimeZone.UTC);
        private  static final long FACTOR = 3600L * 1000 * 24;
        
        public InnerEvaluation (ExpressionEvaluation eval)
        {
            super(eval);
        }

        @Override
        public ValueSource eval()
        {
            ValueSource date = operand();
            if (date.isNull())
                return NullValueSource.only();
            
            try
            {
                valueHolder().putLong((Extractors.getLongExtractor(AkType.DATE).stdLongToUnix(date.getDate(), DateTimeZone.UTC)
                                        - BEGINNING) / FACTOR);
                return valueHolder();
            }
            catch ( IllegalFieldValueException e) // zero dates
            {
                QueryContext qc = queryContext();
                if (qc != null)
                    qc.warnClient(new InvalidParameterValueException(e.getMessage()));
                return NullValueSource.only();
            }
        }
        
    }
    
    ToDaysExpression (Expression arg)
    {
        super(AkType.LONG, arg);
    }
    
    @Override
    public String name()
    {
        return "TO_DAYS";
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(operandEvaluation());
    }
}
