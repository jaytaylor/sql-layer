/**
 * Copyright (C) 2011 Akiban Technologies Inc.
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

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;

/**
 * 
 * A numeric value can *naturally* be translated to an INTERVAL_MILLIS without changing 
 * the actual value (ie., 100 of LONG ====> 100 of INTERVAL_MILLIS)
 * 
 * This expression, however, turns a number into an INTERVAL_MILLIS assuming that
 * the number represents the number of days.
 * This is odd and is only used in DATE_ADD, DATE_SUB
 */
class NumericToIntervalDay extends AbstractUnaryExpression
{
    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private static final long M_SECS_OF_DAY = 86400000L;

        public InnerEvaluation (ExpressionEvaluation eval)
        {
            super(eval);
        }

        @Override
        public ValueSource eval()
        {
            ValueSource source = operand();
            if (source.isNull()) return NullValueSource.only();
            valueHolder().putInterval_Millis((long)(M_SECS_OF_DAY *
                    Extractors.getDoubleExtractor().getDouble(source)));
            return valueHolder();
        }

    }

    protected NumericToIntervalDay (Expression arg)
    {
        super(AkType.INTERVAL_MILLIS, arg);
    }

    @Override
    protected String name()
    {
        return "NUM_TO_INTERVAL";
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(operandEvaluation());
    }
}
