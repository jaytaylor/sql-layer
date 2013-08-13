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

import com.foundationdb.server.error.OverflowException;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.types.ValueSource;

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

        private final int precision, scale;
        private final RoundingMode roundingMode;            
    }
}
