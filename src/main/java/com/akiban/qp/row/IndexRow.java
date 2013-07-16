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

package com.akiban.qp.row;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.PersistitKeyAppender;
import com.akiban.server.store.Store;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.TInstance;
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
