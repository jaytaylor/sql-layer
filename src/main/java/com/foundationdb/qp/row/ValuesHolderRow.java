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

package com.foundationdb.qp.row;


import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.util.ValueHolder;
import com.foundationdb.server.types3.pvalue.PValue;

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

    @Override
    public PValue pvalueAt(int index) {
        return super.pvalueAt(index);
    }

    @Deprecated
    public ValuesHolderRow(RowType rowType) {
        this(rowType, true);
    }

    public ValuesHolderRow(RowType rowType, boolean usePValues) {
        super(rowType, true, usePValues);
    }
}
