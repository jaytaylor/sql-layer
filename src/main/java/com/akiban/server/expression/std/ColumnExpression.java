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

import com.akiban.ais.model.Column;
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;

/**
 * This is similar to a FieldExpression, except that fields are specified by an AIS Column, rather than
 * by RowType + int.
 */
public final class ColumnExpression implements Expression {

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean needsBindings() {
        return false;
    }

    @Override
    public boolean needsRow() {
        return true;
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(column, position);
    }

    @Override
    public AkType valueType() {
        return column.getType().akType();
    }

    // Object interface


    @Override
    public String toString() {
        return String.format("Field(%d)", position);
    }

    public ColumnExpression(Column column, int position) {
        this.column = column;
        this.position = position;
    }

    private final Column column;
    private final int position;

    // nested classes

    private static class InnerEvaluation implements ExpressionEvaluation {

        // ExpressionEvaluation interface

        @Override
        public void of(Row row) {
            RowType incomingType = row.rowType();
            if (! (incomingType instanceof UserTableRowType)) {
                throw new IllegalArgumentException("wrong row type: " + incomingType + " != " + incomingType);
            }
            UserTableRowType utRowType = (UserTableRowType) incomingType;
            if (!utRowType.userTable().equals(column.getUserTable())) {
                throw new IllegalArgumentException("wrong row type: " + incomingType + " != " + incomingType);
            }
            ValueSource incomingSource = row.eval(position);
            this.row = row;
            this.rowSource = incomingSource;
        }

        @Override
        public void of(Bindings bindings) {
        }

        @Override
        public ValueSource eval() {
            if (rowSource == null)
                throw new IllegalStateException("haven't seen a row to target");
            return rowSource;
        }

        // Shareable interface

        @Override
        public void acquire() {
            row.acquire();
        }

        @Override
        public boolean isShared() {
            return row.isShared();
        }

        @Override
        public void release() {
            row.release();
        }

        // private methods

        private InnerEvaluation(Column column, int position) {
            this.column = column;
            this.position = position;
        }

        private final Column column;
        private final int position;
        private Row row;
        private ValueSource rowSource;
    }
}
