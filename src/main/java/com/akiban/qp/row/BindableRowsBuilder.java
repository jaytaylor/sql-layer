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

package com.akiban.qp.row;

import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class BindableRowsBuilder {

    public BindableRowsBuilder add(List<? extends Expression> expressions) {
        // validate inputs
        if (expressions.size() != rowType.nFields())
            throw new IllegalArgumentException(expressions + " has wrong number of values for " + rowType);
        for (int i=0; i < rowType.nFields(); ++i) {
            AkType expressionType = expressions.get(i).valueType();
            AkType fieldType = rowType.typeAt(i);
            if (expressionType != fieldType)
                throw new IllegalArgumentException(
                        "expression[" + i + "] should have type " + fieldType + ", was " + expressionType
                );
        }
        expressionsList.add(expressions);
        return this;
    }

    public Collection<BindableRow> get() {
        List<BindableRow> result = new ArrayList<BindableRow>();
        for (List<? extends Expression> expressionsRow : expressionsList) {
            result.add(BindableRow.of(rowType, expressionsRow));
        }
        return result;
    }

    public BindableRowsBuilder(RowType rowType) {
        this.rowType = rowType;
    }

    private final RowType rowType;
    private final List<List<? extends Expression>> expressionsList = new ArrayList<List<? extends Expression>>();
}
