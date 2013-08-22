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
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.extract.Extractors;
import com.foundationdb.server.types.util.BoolValueSource;
import com.foundationdb.sql.StandardException;
import com.foundationdb.server.expression.TypesList;

public final class NotExpression extends AbstractUnaryExpression {

    @Scalar("not") public static final ExpressionComposer COMPOSER = new UnaryComposer() {
        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType) {
            return new NotExpression(argument);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());
            return ExpressionTypes.BOOL;
        }
    };

    @Override
    public String name() {
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
