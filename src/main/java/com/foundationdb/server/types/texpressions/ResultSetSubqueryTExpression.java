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

package com.foundationdb.server.types.texpressions;

import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.aksql.aktypes.AkResultSet;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;

public class ResultSetSubqueryTExpression extends SubqueryTExpression
{
    private static final class InnerEvaluation implements TEvaluatableExpression
    {
        @Override
        public ValueSource resultValue() {
            return value;
        }

        @Override
        public void evaluate() {
            bindings.setRow(bindingPosition, outerRow);
            Cursor cursor = API.cursor(subquery, context, bindings);
            cursor.openTopLevel();
            value.putObject(cursor);
        }

        @Override
        public void with(Row row) {
            if (row.rowType() != outerRowType) {
                throw new IllegalArgumentException("wrong row type: " + outerRowType +
                                                   " != " + row.rowType());
            }
            outerRow = row;
        }

        @Override
        public void with(QueryContext context) {
            this.context = context;
        }

        @Override
        public void with(QueryBindings bindings) {
            this.bindings = bindings;
        }

        InnerEvaluation(Operator subquery, RowType outerRowType, int bindingPosition, TInstance type)
        {
            this.subquery = subquery;
            this.outerRowType = outerRowType;
            this.bindingPosition = bindingPosition;
            this.value = new Value(type);
        }

        private final Operator subquery;
        private final RowType outerRowType;
        private final int bindingPosition;
        private final Value value;
        private QueryContext context;
        private QueryBindings bindings;
        private Row outerRow;
    }

    public ResultSetSubqueryTExpression(Operator subquery, TInstance type,
                                        RowType outerRowType, RowType innerRowType, 
                                        int bindingPosition)
    {
        super(subquery, outerRowType, innerRowType, bindingPosition);
        assert (type.typeClass() instanceof AkResultSet) : type;
        this.type = type;
    }

    @Override
    public TInstance resultType()
    {
        return type;
    }

    @Override
    public TEvaluatableExpression build()
    {
        return new InnerEvaluation(subquery(), outerRowType(), bindingPosition(), type);
    }

    private final TInstance type;
}
