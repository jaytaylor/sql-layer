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

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;

public final class ResultSetSubqueryExpression extends SubqueryExpression {

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(subquery(), outerRowType(),
                                   bindingPosition());
    }

    @Override
    public AkType valueType() {
        return AkType.RESULT_SET;
    }

    @Override
    public String toString() {
        return "RESULT_SET(" + subquery() + ")";
    }

    public ResultSetSubqueryExpression(Operator subquery,
                                       RowType outerRowType, RowType innerRowType, 
                                       int bindingPosition) {
        super(subquery, outerRowType, innerRowType, bindingPosition);
    }

    // TODO: Could refactor SubqueryExpressionEvaluation into a common piece.
    private static final class InnerEvaluation implements ExpressionEvaluation {
        @Override
        public void of(StoreAdapter adapter) {
            this.adapter = adapter;
        }

        @Override
        public void of(Bindings bindings) {
            this.bindings = bindings;
        }

        @Override
        public void of(Row row) {
            if (row.rowType() != outerRowType) {
                throw new IllegalArgumentException("wrong row type: " + outerRowType +
                                                   " != " + row.rowType());
            }
            outerRow = row;
        }

        @Override
        public ValueSource eval() {
            bindings.set(bindingPosition, outerRow);
            Cursor cursor = API.cursor(subquery, adapter);
            cursor.open(bindings);
            return new ValueHolder(AkType.RESULT_SET, cursor);
        }

        // Shareable interface

        @Override
        public void acquire() {
            outerRow.acquire();
        }

        @Override
        public boolean isShared() {
            return outerRow.isShared();
        }

        @Override
        public void release() {
            outerRow.release();
        }

        protected InnerEvaluation(Operator subquery,
                                  RowType outerRowType,
                                  int bindingPosition) {
            this.subquery = subquery;
            this.outerRowType = outerRowType;
            this.bindingPosition = bindingPosition;
        }

        private final Operator subquery;
        private final RowType outerRowType;
        private final int bindingPosition;
        private StoreAdapter adapter;
        private Bindings bindings;
        private Row outerRow;
    }

}
