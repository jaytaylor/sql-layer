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

package com.foundationdb.server.store.format.protobuf;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.server.test.it.qp.TestRow;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class ProtobufStorageFormatIT  extends ITBase
{
    private static final String SCHEMA = "test";

    @Test
    public void testSimple() {
        createFromDDL(SCHEMA,
          "CREATE TABLE t1(id INT PRIMARY KEY NOT NULL, s VARCHAR(128)) STORAGE_FORMAT protobuf");
        int t1 = ddl().getTableId(session(), new TableName(SCHEMA, "t1"));

        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        RowType t1Type = schema.tableRowType(getTable(t1));
        StoreAdapter adapter = newStoreAdapter(schema);

        txnService().beginTransaction(session());

        Object[] r1 = { 1L, "Fred" };
        Object[] r2 = { 2L, "Barney" };
        writeRow(t1, r1);
        writeRow(t1, r2);

        Row[] expected = {
            new TestRow(t1Type, r1),
            new TestRow(t1Type, r2)
        };
        compareRows(expected, adapter.newGroupCursor(t1Type.table().getGroup()));

        txnService().commitTransaction(session());
    }
    
}
