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
import com.akiban.qp.BTreeAdapter;
import com.akiban.qp.BTreeAdapterRuntimeException;
import com.akiban.qp.Cursor;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.RowDef;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.PersistitStore;
import com.persistit.exception.PersistitException;

public class PersistitAdapter implements BTreeAdapter
{
    // BTreeAdapter interface

    @Override
    public Cursor newCursor(GroupTable table)
    {
        Cursor cursor;
        try {
            cursor = new PersistitCursor(this,
                                         persistit,
                                         persistit.getExchange(session, (RowDef) table.rowDef(), null));
        } catch (PersistitException e) {
            throw new BTreeAdapterRuntimeException(e);
        }
        return cursor;
    }

    // PersistitAdapter interface

    public PersistitAdapter(Schema schema, PersistitStore persistit, Session session)
    {
        this.schema = schema;
        this.persistit = persistit;
        this.session = session;
    }

    // Object state

    final Schema schema;
    final PersistitStore persistit;
    final Session session;
}
