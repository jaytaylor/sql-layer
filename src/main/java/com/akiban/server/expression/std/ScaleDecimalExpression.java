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

import com.akiban.server.error.OverflowException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class ScaleDecimalExpression extends AbstractUnaryExpression
{
    public ScaleDecimalExpression(int precision, int scale, Expression operand) {
        super(AkType.DECIMAL, operand);
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public String name() {
        return "SCALE_DECIMAL";
    }

    @Override
    public String toString() {
        return name() + "(" + operand() + "," + precision + "." + scale + ")";
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(precision, scale, RoundingMode.UP, 
                                   operandEvaluation());
    }

    private final int precision, scale;

    private static final class InnerEvaluation extends AbstractUnaryExpressionEvaluation {
        public InnerEvaluation(int precision, int scale, RoundingMode roundingMode,
                               ExpressionEvaluation ev) {
            super(ev);
            this.precision = precision;
            this.scale = scale;
            this.roundingMode = roundingMode;
        }

        @Override
        public ValueSource eval() {
            ValueSource operandSource = operand();
            if (operandSource.isNull())
                return NullValueSource.only();
            BigDecimal value = operandSource.getDecimal();
            value = value.setScale(scale, roundingMode);
            if (value.precision() > precision)
                throw new OverflowException();
            valueHolder().putDecimal(value);
            return valueHolder();
        }

        private ValueSource hold(BigDecimal value) {
            if (holder == null) {
                holder = new ValueHolder();
            }
            holder.putDecimal(value);
            return holder;
        }

        private final int precision, scale;
        private final RoundingMode roundingMode;
        private ValueHolder holder;
    }
}
