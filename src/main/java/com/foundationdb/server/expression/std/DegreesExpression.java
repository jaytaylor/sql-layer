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

import com.foundationdb.server.error.WrongExpressionArityException;
import com.foundationdb.server.expression.*;
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.sql.StandardException;

public class DegreesExpression extends AbstractUnaryExpression {

    @Scalar("degrees")
    public static final ExpressionComposer COMPOSER = new UnaryComposer() {

        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType) {
            return new DegreesExpression(argument);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException {
            if (argumentTypes.size() != 1) {
                throw new WrongExpressionArityException(1, argumentTypes.size());
            }
            argumentTypes.setType(0, AkType.DOUBLE);
            return ExpressionTypes.DOUBLE;
        }
    };

    protected DegreesExpression(Expression operand) {
        super(AkType.DOUBLE, operand);
    }

    @Override
    public String name() {
        return "DEGREES";
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(operandEvaluation());
    }

    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation {

        public InnerEvaluation(ExpressionEvaluation eval) {
            super(eval);
        }

        @Override
        public ValueSource eval() {
            if (operand().isNull()) {
                return NullValueSource.only();
            }

            double degreeResult = Math.toDegrees(operand().getDouble());
            valueHolder().putDouble(degreeResult);

            return valueHolder();
        }
    }
}
