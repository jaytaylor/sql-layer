
package com.akiban.ais.model;

import com.akiban.ais.model.validation.AISInvariants;

import com.akiban.server.collation.AkCollator;

public class IndexColumn
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
