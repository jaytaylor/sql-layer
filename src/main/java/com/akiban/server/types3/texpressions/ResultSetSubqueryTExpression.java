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

package com.akiban.server.types3.texpressions;

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryBindings;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.aksql.aktypes.AkResultSet;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;

public class ResultSetSubqueryTExpression extends SubqueryTExpression
{
    private static final class InnerEvaluation implements TEvaluatableExpression
    {
        @Override
        public PValueSource resultValue() {
            return pvalue;
        }

        @Override
        public void evaluate() {
            bindings.setRow(bindingPosition, outerRow);
            Cursor cursor = API.cursor(subquery, context, bindings);
            cursor.open();
            pvalue.putObject(cursor);
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

        InnerEvaluation(Operator subquery, RowType outerRowType, int bindingPosition)
        {
            this.subquery = subquery;
            this.outerRowType = outerRowType;
            this.bindingPosition = bindingPosition;
            this.pvalue = new PValue();
        }

        private final Operator subquery;
        private final RowType outerRowType;
        private final int bindingPosition;
        private final PValue pvalue;
        private QueryContext context;
        private QueryBindings bindings;
        private Row outerRow;
    }

    public ResultSetSubqueryTExpression(Operator subquery, TInstance tInstance,
                                        RowType outerRowType, RowType innerRowType, 
                                        int bindingPosition)
    {
        super(subquery, outerRowType, innerRowType, bindingPosition);
        assert (tInstance.typeClass() instanceof AkResultSet) : tInstance;
        this.tInstance = tInstance;
    }

    @Override
    public TInstance resultType()
    {
        return tInstance;
    }

    @Override
    public TEvaluatableExpression build()
    {
        return new InnerEvaluation(subquery(), outerRowType(), bindingPosition());
    }

    private final TInstance tInstance;
}
