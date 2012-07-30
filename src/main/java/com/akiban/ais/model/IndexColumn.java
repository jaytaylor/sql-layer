/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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

    public static IndexColumn create(Index index, Column column, Integer position, Boolean ascending, Integer indexedLength) {
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
