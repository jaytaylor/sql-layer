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

package com.akiban.qp.rowtype;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;
import com.akiban.server.RowDef;
import com.akiban.util.FilteringIterator;

import java.util.ArrayList;
import java.util.Iterator;

public class UserTableRowType extends RowType
{
    // Object interface

    @Override
    public String toString()
    {
        return table.toString();
    }

    // RowType interface

    @Override
    public int nFields()
    {
        return table.getColumnsIncludingInternal().size();
    }

    @Override
    public boolean ancestorOf(RowType type)
    {
        assert type instanceof UserTableRowType : type;
        // TODO: Something faster
        UserTable ancestor = ((UserTableRowType) type).table;
        while (ancestor != table && ancestor != null) {
            Join join = ancestor.getParentJoin();
            ancestor = join == null ? null : join.getParent();
        }
        return ancestor != null;
    }

    // UserTableRowType interface

    public UserTable userTable()
    {
        return table;
    }

    public IndexRowType indexRowType(Index index)
    {
        return indexRowTypes.get(index.getIndexId());
    }

    public void addIndexRowType(IndexRowType indexRowType)
    {
        Index index = indexRowType.index();
        int requiredEntries = index.getIndexId() + 1;
        while (indexRowTypes.size() < requiredEntries) {
            indexRowTypes.add(null);
        }
        indexRowTypes.set(index.getIndexId(), indexRowType);
    }

    public Iterable<IndexRowType> indexRowTypes() {
        return new Iterable<IndexRowType>() {
            @Override
            public Iterator<IndexRowType> iterator() {
                return new FilteringIterator<IndexRowType>(indexRowTypes.iterator(), false) {
                    @Override
                    protected boolean allow(IndexRowType item) {
                        return item != null;
                    }
                };
            }
        };
    }

    public UserTableRowType(Schema schema, UserTable table)
    {
        super(schema, ((RowDef) table.rowDef()).getOrdinal(), Ancestry.of(table));
        this.table = table;
    }

    // Object state

    private final UserTable table;
    // Type of indexRowTypes is ArrayList, not List, to make it clear that null values are permitted.
    private final ArrayList<IndexRowType> indexRowTypes = new ArrayList<IndexRowType>();
}
