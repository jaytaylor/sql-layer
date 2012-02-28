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

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.util.ArgumentValidation;

public final class FieldExpression implements Expression {

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
        return new InnerEvaluation(rowType, fieldIndex, valueType());
    }

    @Override
    public AkType valueType() {
        return rowType.typeAt(fieldIndex);
    }

    // Object interface


    @Override
    public String toString() {
        return String.format("Field(%d)", fieldIndex);
    }

    public FieldExpression(RowType rowType, int fieldIndex) {
        this.rowType = rowType;
        this.fieldIndex = fieldIndex;
        if (this.fieldIndex < 0 || this.fieldIndex >= rowType.nFields()) {
            throw new IllegalArgumentException("fieldIndex out of range: " + this.fieldIndex + " for " + this.rowType);
        }
    }

    private final RowType rowType;
    private final int fieldIndex;

    // nested classes

    private static class InnerEvaluation implements ExpressionEvaluation {

        // ExpressionEvaluation interface

        @Override
        public void of(Row row) {
            RowType incomingType = row.rowType();
            if (!rowType.equals(incomingType)) {
                throw new IllegalArgumentException("wrong row type: " + incomingType + " != " + rowType);
            }
            ValueSource incomingSource = row.eval(fieldIndex);
            AkType incomingAkType = incomingSource.getConversionType();
            if (incomingAkType != AkType.NULL && !akType.equals(incomingAkType)) {
                throw new AkibanInternalException(
                        row + "[" + fieldIndex + "] had akType " + incomingAkType + "; expected " + akType
                );
            }
            this.row = row;
        }

        @Override
        public void of(QueryContext context) {
        }

        @Override
        public ValueSource eval() {
            if (row == null)
                throw new IllegalStateException("haven't seen a row to target");
            return row.eval(fieldIndex);
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

        private InnerEvaluation(RowType rowType, int fieldIndex, AkType akType) {
            assert rowType != null;
            assert akType != null;
            this.rowType = rowType;
            this.fieldIndex = fieldIndex;
            this.akType = akType;
        }

        private final RowType rowType;
        private final int fieldIndex;
        private final AkType akType;
        private Row row;
    }
}
