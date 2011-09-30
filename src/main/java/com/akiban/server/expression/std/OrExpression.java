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
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.BooleanExtractor;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.util.BoolValueSource;

import java.util.List;

public final class OrExpression extends AbstractTwoArgExpression {

    @Override
    protected void describe(StringBuilder sb) {
        sb.append("AND");
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InternalEvaluation(childrenEvaluations());
    }

    public OrExpression(List<? extends Expression> children) {
        super(AkType.BOOL, children);
    }

    private static final BooleanExtractor extractor = Extractors.getBooleanExtractor();

    private static class InternalEvaluation extends AbstractTwoArgExpressionEvaluation {

        @Override
        public ValueSource eval() {
            Boolean left = extractor.getBoolean(left(), null);
            Boolean right = extractor.getBoolean(right(), null);
            final Boolean result;
            if (left == null || right == null) {
                result = (left == Boolean.TRUE || right == Boolean.TRUE) ? true : null;
            }
            else {
                result = left || right;
            }
            return BoolValueSource.of(result);
        }

        private InternalEvaluation(List<? extends ExpressionEvaluation> children) {
            super(children);
        }
    }
}
