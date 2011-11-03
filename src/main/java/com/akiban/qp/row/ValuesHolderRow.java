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
import com.akiban.server.types.util.ValueHolder;

public class ValuesHolderRow extends AbstractValuesHolderRow {

    // ValuesHolderRow interface -- mostly just promoting visiblity

    @Override
    public void clear() {
        super.clear();
    }

    @Override
    public ValueHolder holderAt(int index) {
        return super.holderAt(index);
    }

    public ValuesHolderRow(RowType rowType) {
        super(rowType, true);
    }
}
