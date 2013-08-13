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

import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.extract.Extractors;

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
    public String name()
    {
        return valueType().name() + " TO " + target;
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(operandEvaluation(), target);
    }
}
