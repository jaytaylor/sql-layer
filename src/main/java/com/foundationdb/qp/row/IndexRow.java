/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.expression.BoundExpressions;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.rowdata.FieldDef;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.PersistitKeyAppender;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types3.TInstance;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

public abstract class IndexRow extends AbstractRow
{
    // BoundExpressions interface

    @Override
    public ValueSource eval(int index)
    {
        throw new UnsupportedOperationException();
    }

    // RowBase interface

    public RowType rowType()
    {
        throw new UnsupportedOperationException();
    }

    public HKey hKey()
    {
        throw new UnsupportedOperationException();
    }

    // IndexRow interface

    public abstract void initialize(RowData rowData, Key hKey);

    public final void append(Column column, ValueSource source)
    {
        append(source, column.getType().akType(), column.tInstance(), column.getCollator());
    }

    public abstract <S> void append(S source, AkType type, TInstance tInstance, AkCollator collator);

    public abstract void close(Session session, Store store, boolean forInsert);

}
