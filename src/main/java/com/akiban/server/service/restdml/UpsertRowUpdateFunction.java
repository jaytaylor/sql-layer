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
package com.akiban.server.service.restdml;

import java.util.List;

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.UpdateFunction;
import com.akiban.qp.row.OverlayingRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types3.texpressions.TEvaluatableExpression;
import com.akiban.server.types3.texpressions.TPreparedExpression;

/** 
 * Update a row by sustituting expressions for some fields.
 * For use specificalally for the REST API upsert processing. 
 * @author tjoneslo
 *
 */
public class UpsertRowUpdateFunction implements UpdateFunction{

    private final List<TPreparedExpression> pExpressions;
    private final RowType rowType;
    
    public UpsertRowUpdateFunction(List<TPreparedExpression> pExpressions, RowType rowType) {
        this.pExpressions = pExpressions;
        this.rowType = rowType;
    }

    @Override
    public boolean rowIsSelected(Row row) {
        return row.rowType().equals(rowType);
    }

    @Override
    public boolean usePValues() {
        return true;
    }

    @Override
    public Row evaluate(Row original, QueryContext context) {
        OverlayingRow result = new OverlayingRow(original, pExpressions != null);
        int nfields = rowType.nFields();
        for (int i = 0; i < nfields; i++) {
            TPreparedExpression expression = pExpressions.get(i);
            if (expression != null) {
                TEvaluatableExpression evaluation = expression.build();
                evaluation.with(original);
                evaluation.with(context);
                evaluation.evaluate();
                result.overlay(i, evaluation.resultValue());
            }
        }
        return result;
    }

    @Override
    public String toString() {
        String exprs = pExpressions.toString();
        return getClass().getSimpleName() + "(" + exprs + ")";
    }
}
