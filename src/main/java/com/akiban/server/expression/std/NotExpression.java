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
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.util.BoolValueSource;

import java.util.List;

public final class NotExpression extends AbstractUnaryExpression {

    @Scalar("not") public static final ExpressionComposer COMPOSER = new ExpressionComposer() {
        @Override
        public Expression compose(List<? extends Expression> arguments) {
            if (arguments.size() != 0)
                throw new IllegalArgumentException("NOT must take exactly one argument: " + arguments);
            return new NotExpression(arguments.get(0));
        }
    };

    @Override
    protected String name() {
        return "NOT";
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(operandEvaluation());
    }

    public NotExpression(Expression operand) {
        super(AkType.BOOL, operand);
    }

    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation {

        @Override
        public ValueSource eval() {
            Boolean answer = Extractors.getBooleanExtractor().getBoolean(operand(), null);
            if (answer != null)
                answer = !answer;
            return BoolValueSource.of(answer);
        }

        private InnerEvaluation(ExpressionEvaluation operandEvaluation) {
            super(operandEvaluation);
        }
    }
}
