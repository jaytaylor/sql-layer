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

package com.akiban.qp.operator;

import java.util.ArrayList;
import java.util.Collection;

import com.akiban.qp.row.BindableRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;


public final class TestOperator extends ValuesScan_Default {

    public TestOperator(RowsBuilder rowsBuilder) {
        super (bindableRows(rowsBuilder.rows()), rowsBuilder.rowType());
    }
    
    public TestOperator (Collection<? extends Row> rows, RowType rowType) {
        super(bindableRows(rows), rowType);
    }

    private static Collection<? extends BindableRow> bindableRows(Collection<? extends Row> rows) {
        Collection<BindableRow> result = new ArrayList<BindableRow>();
        for (Row row : rows) {
            result.add(BindableRow.of(row));
        }
        return result;
    }
}
