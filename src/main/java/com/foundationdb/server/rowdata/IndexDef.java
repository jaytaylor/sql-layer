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

package com.foundationdb.server.rowdata;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;

public class IndexDef {
    private final Index index;
    // Identifies fields within the row that form the key part of the index entry.
    private final int[] fields;
    private final RowDef rowDef;

    public IndexDef(RowDef rowDef, Index index)
    {
        this.index = index;
        index.indexDef(this);
        this.rowDef = rowDef;
        this.fields = new int[index.getKeyColumns().size()];
        for (IndexColumn indexColumn : index.getKeyColumns()) {
            int positionInRow = indexColumn.getColumn().getPosition();
            int positionInIndex = indexColumn.getPosition();
            this.fields[positionInIndex] = positionInRow;
        }
    }

    /**
     * @deprecated Use IndexRowComposition or just IndexColumns where appropriate
     * @return Array of index position to table position
     * */
    public int[] getFields() {
        return fields;
    }

    public RowDef getRowDef() {
        return rowDef;
    }

    public int getIndexKeySegmentCount() {
        return fields.length;
    }

    public Index getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return index.toString();
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException("IndexDef deprecated, use Index");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("IndexDef deprecated, use Index");
    }

}
