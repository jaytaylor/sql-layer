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
import com.foundationdb.qp.storeadapter.indexrow.SpatialColumnHandler;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.types.TInstance;
import com.persistit.Key;

public abstract class IndexRow extends AbstractRow
{
    // Row interface

    public RowType rowType()
    {
        throw new UnsupportedOperationException();
    }

    public HKey hKey()
    {
        throw new UnsupportedOperationException();
    }

    // IndexRow interface

    public abstract void initialize(RowData rowData, Key hKey, SpatialColumnHandler spatialColumnHandler, long zValue);

    public abstract <S> void append(S source, TInstance type);

    public abstract void close(Session session, Store store, boolean forInsert);

}
