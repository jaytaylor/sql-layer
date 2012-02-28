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
 * the number represents the number of days or seconds.
 * This is odd and is only used in DATE_ADD, DATE_SUB
 */
class NumericToIntervalMillis extends AbstractUnaryExpression
{
    static enum TargetType
    {
        DAY(86400000L, AkType.DATE),
        SECOND(1000L, AkType.TIME);
        
        final long multiplier;
        final AkType operandType;
        TargetType (long val, AkType type)
        {
            multiplier = val;
            operandType = type;
        }
    }
    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private final TargetType  targetType;
        public InnerEvaluation (ExpressionEvaluation eval, TargetType target)
        {
            super(eval);
            targetType = target;
        }

        @Override
        public ValueSource eval()
        {
            ValueSource source = operand();
            if (source.isNull()) return NullValueSource.only();
            valueHolder().putInterval_Millis((long)(targetType.multiplier *
                    Extractors.getDoubleExtractor().getDouble(source)));
            return valueHolder();
        }

    }

    private final TargetType target;
    protected NumericToIntervalMillis (Expression arg, TargetType targetType)
    {
        super(AkType.INTERVAL_MILLIS, arg);
        target = targetType;
    }

    @Override
    protected String name()
    {
        return "NUM_TO_INTERVAL";
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(operandEvaluation(), target);
    }
}
