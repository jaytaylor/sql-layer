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

package com.akiban.server.expression.subquery;

import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.BoolValueSource;

public final class ExistsSubqueryExpression extends SubqueryExpression {

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(subquery(), outerRowType(), innerRowType(),
                                   bindingPosition());
    }

    @Override
    public AkType valueType() {
        return AkType.BOOL;
    }

    @Override
    public String toString() {
        return "EXISTS(" + subquery() + ")";
    }

    public ExistsSubqueryExpression(Operator subquery, RowType outerRowType, 
                                    RowType innerRowType, int bindingPosition) {
        super(subquery, outerRowType, innerRowType, bindingPosition);
    }

    @Override
    public String name()
    {
        return "EXISTS";
    }
                                 
    private static final class InnerEvaluation extends SubqueryExpressionEvaluation {
        @Override
        public ValueSource doEval() {
            boolean empty = (next() == null);
            return BoolValueSource.of(!empty);
        }

        private InnerEvaluation(Operator subquery,
                                RowType outerRowType, RowType innerRowType, 
                                int bindingPosition) {
            super(subquery, outerRowType, innerRowType, bindingPosition);
        }
    }

}
