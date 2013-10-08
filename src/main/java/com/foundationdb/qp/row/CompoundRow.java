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

import com.foundationdb.qp.rowtype.CompoundRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.pvalue.PValueSource;
import com.foundationdb.server.types.pvalue.PValueSources;
import com.foundationdb.util.ShareHolder;

public class CompoundRow extends AbstractRow {

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public HKey hKey() {
        return null;
    }

    @Override
    public PValueSource pvalue(int i) {
        PValueSource source;
        if (i < firstRowFields) {
            source = firstRowh.isEmpty() ? nullPValue(i) : firstRowh.get().pvalue(i);
        } else {
            source = secondRowh.isEmpty() ? nullPValue(i) : secondRowh.get().pvalue(i - rowOffset);
        }
        return source;
    }
    
    @Override
    public Row subRow(RowType subRowType)
    {
        Row subRow;
        if (subRowType == rowType.first()) {
            subRow = firstRowh.get();
        } else if (subRowType == rowType.second()) {
            subRow = secondRowh.get();
        } else {
            // If the subRowType doesn't match leftType or rightType, then it might be buried deeper.
            subRow = firstRowh.get().subRow(subRowType);
            if (subRow == null) {
                subRow = secondRowh.get().subRow(subRowType);
            }
        }
        return subRow;
    }

    protected ShareHolder<Row> first() {
        return firstRowh;
    }
    
    protected ShareHolder<Row> second() {
        return secondRowh;
    }
    
    protected int firstRowFields() {
        return firstRowFields;
    }
    
    public CompoundRow (CompoundRowType type, Row firstRow, Row secondRow)
    {
        this.rowType = type;
        this.firstRowh.hold(firstRow);
        this.secondRowh.hold(secondRow);
        this.firstRowFields = type.first().nFields();
        this.rowOffset = type.first().nFields();
    }

    private PValueSource nullPValue(int i) {
        return PValueSources.getNullSource(rowType.typeInstanceAt(i));
    }

    // Object state

    private final CompoundRowType rowType;
    private final ShareHolder<Row> firstRowh = new ShareHolder<>();
    private final ShareHolder<Row> secondRowh = new ShareHolder<>();
    private final int firstRowFields;
    protected int rowOffset; 


}
