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

package com.foundationdb.ais.model;

import com.foundationdb.ais.model.validation.AISInvariants;

import com.foundationdb.server.collation.AkCollator;

public class IndexColumn implements Visitable
{
    // IndexColumn interface

    @Override
    public String toString()
    {
        return "IndexColumn(" + column.getName() + ")";
    }

    public Index getIndex()
    {
        return index;
    }

    public Column getColumn()
    {
        return column;
    }

    public Integer getPosition()
    {
        return position;
    }

    public Integer getIndexedLength()
    {
        return indexedLength;
    }

    public Boolean isAscending()
    {
        return ascending;
    }

    /** Can this index column be used as part of a <em>covering</em> index? */
    public boolean isRecoverable() {
        AkCollator collator = column.getCollator();
        if (collator == null)
            return true;
        else
            return collator.isRecoverable();
    }

    // Visitable

    /** Visit this instance. */
    @Override
    public void visit(Visitor visitor) {
        visitor.visit(this);
    }

    public static IndexColumn create(Index index,
                                     Column column,
                                     Integer position,
                                     Boolean ascending,
                                     Integer indexedLength) {
        index.checkMutability();
        AISInvariants.checkNullField(column, "IndexColumn", "column", "Column");
        AISInvariants.checkDuplicateColumnsInIndex(index, column.getColumnar().getName(), column.getName());
        IndexColumn indexColumn = new IndexColumn(index, column, position, ascending, indexedLength);
        index.addColumn(indexColumn);
        return indexColumn;
    }

    /**
     * Create an independent copy of an existing IndexColumn.
     * @param index Destination Index.
     * @param column Associated Column.
     * @param indexColumn IndexColumn to copy.
     * @return The new copy of the IndexColumn.
     */
    public static IndexColumn create(Index index, Column column, IndexColumn indexColumn, int position) {
        return create(index, column, position, indexColumn.ascending, indexColumn.indexedLength);
    }
    
    IndexColumn(Index index, Column column, Integer position, Boolean ascending, Integer indexedLength)
    {
        this.index = index;
        this.column = column;
        this.position = position;
        this.ascending = ascending;
        this.indexedLength = indexedLength;
    }
    
    // State

    private final Index index;
    private final Column column;
    private final Integer position;
    private final Boolean ascending;
    private final Integer indexedLength;
}
