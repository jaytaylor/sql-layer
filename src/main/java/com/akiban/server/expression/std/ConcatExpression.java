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
import com.akiban.server.types.util.ValueHolder;

import java.util.List;

public final class ConcatExpression extends AbstractCompositeExpression {
    @Override
    protected void describe(StringBuilder sb) {
        sb.append("CONCAT");
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(childrenEvaluations());
    }

    @Override
    public boolean isConstant() {
        for (Expression child : children()) {
            if (!child.isConstant())
                return false;
            if (child.evaluation().eval().isNull())
                return true;
        }
        return true;
    }

    public ConcatExpression(List<? extends Expression> children) {
        super(AkType.VARCHAR, children);
    }

    private static final class InnerEvaluation extends AbstractCompositeExpressionEvaluation {
        @Override
        public ValueSource eval() {
            StringBuilder sb = new StringBuilder();
            for (ValueSource childSource : childrenSources()) {
                if (childSource.isNull())
                    return NullValueSource.only();
            }
            return new ValueHolder(AkType.VARCHAR, sb.toString());
        }

        private InnerEvaluation(List<? extends ExpressionEvaluation> children) {
            super(children);
        }
    }
}
