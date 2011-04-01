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

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.qp.physicaloperator.GroupCursor;
import com.akiban.qp.physicaloperator.IndexCursor;
import com.akiban.qp.physicaloperator.StoreAdapter;
import com.akiban.qp.physicaloperator.StoreAdapterRuntimeException;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.IndexDef;
import com.akiban.server.RowDef;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.PersistitStore;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;

public class PersistitAdapter extends StoreAdapter
{
    // StoreAdapter interface

    @Override
    public GroupCursor newGroupCursor(GroupTable groupTable)
    {
        GroupCursor cursor;
        try {
            cursor = new PersistitGroupCursor(this, groupTable);
        } catch (PersistitException e) {
            throw new StoreAdapterRuntimeException(e);
        }
        return cursor;
    }

    @Override
    public IndexCursor newIndexCursor(Index index)
    {
        IndexCursor cursor;
        try {
            cursor = new PersistitIndexCursor(this, schema.indexRowType(index));
        } catch (PersistitException e) {
            throw new StoreAdapterRuntimeException(e);
        }
        return cursor;
    }

    // PersistitAdapter interface

    public PersistitGroupRow newGroupRow()
    {
        // TODO: Pool rows?
        return PersistitGroupRow.newPersistitGroupRow(this);
    }

    public PersistitIndexRow newIndexRow(IndexRowType indexRowType) throws PersistitException
    {
        // TODO: Pool rows?
        return new PersistitIndexRow(this, indexRowType);
    }

    public Exchange takeExchange(GroupTable table) throws PersistitException
    {
        return persistit.getExchange(session, (RowDef) table.rowDef(), null);
    }

    public Exchange takeExchange(Index index) throws PersistitException
    {
        return persistit.getExchange(session, null, (IndexDef) index.indexDef());
    }

    public void returnExchange(Exchange exchange)
    {
        persistit.releaseExchange(session, exchange);
    }

    public PersistitAdapter(Schema schema, PersistitStore persistit, Session session)
    {
        super(schema);
        this.persistit = persistit;
        this.session = session;
    }

    // Object state

    final PersistitStore persistit;
    final Session session;
}
