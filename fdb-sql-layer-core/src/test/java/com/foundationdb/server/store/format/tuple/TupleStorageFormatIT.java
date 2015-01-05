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

package com.foundationdb.server.store.format.tuple;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.StorageDescriptionInvalidException;
import com.foundationdb.server.test.it.FDBITBase;
import com.foundationdb.server.test.it.qp.TestRow;

import org.junit.Test;

public class TupleStorageFormatIT  extends FDBITBase
{
    private static final String SCHEMA = "test";

    @Test
    public void decimalTypeAllowed() {
        createFromDDL(SCHEMA,
          "CREATE TABLE t1(id INT PRIMARY KEY NOT NULL, d DECIMAL(6,2));" +
          "CREATE INDEX i1 ON t1(d) STORAGE_FORMAT tuple;");
    }

    @Test
    public void groupAllowed() {
        createFromDDL(SCHEMA,
          "CREATE TABLE parent(id INT PRIMARY KEY NOT NULL, s VARCHAR(128)) STORAGE_FORMAT tuple;" +
          "CREATE TABLE child(id INT PRIMARY KEY NOT NULL, pid INT, GROUPING FOREIGN KEY(pid) REFERENCES parent(id))");
    }

    @Test
    public void keyAndRow() {
        createFromDDL(SCHEMA,
          "CREATE TABLE t1(id INT PRIMARY KEY NOT NULL, s VARCHAR(128)) STORAGE_FORMAT tuple");
        int t1 = ddl().getTableId(session(), new TableName(SCHEMA, "t1"));

        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        RowType t1Type = schema.tableRowType(getTable(t1));
        StoreAdapter adapter = newStoreAdapter();

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
    
    @Test
    public void keyOnly() {
        createFromDDL(SCHEMA,
          "CREATE TABLE parent(id INT PRIMARY KEY NOT NULL, s VARCHAR(128)) STORAGE_FORMAT tuple(key_only = true);" +
          "CREATE TABLE child(id INT PRIMARY KEY NOT NULL, pid INT, GROUPING FOREIGN KEY(pid) REFERENCES parent(id), s VARCHAR(128));");
        int parent = ddl().getTableId(session(), new TableName(SCHEMA, "parent"));
        int child = ddl().getTableId(session(), new TableName(SCHEMA, "child"));

        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        RowType parentType = schema.tableRowType(getTable(parent)); 
        RowType childType = schema.tableRowType(getTable(child));
        StoreAdapter adapter = newStoreAdapter();

        txnService().beginTransaction(session());

        Object[] r1 = { 1L, "Margaret" };
        Object[] r1a = { 101L, 1L, "Meg" };
        Object[] r1b = { 102L, 1L, "Jo" };
        Object[] r2 = { 2L, "Josephine" };
        writeRow(parent, r1);
        writeRow(child, r1a);
        writeRow(child, r1b);
        writeRow(parent, r2);

        Row[] expected = {
            new TestRow(parentType, r1),
            new TestRow(childType, r1a),
            new TestRow(childType, r1b),
            new TestRow(parentType, r2)
        };
        compareRows(expected, adapter.newGroupCursor(parentType.table().getGroup()));

        txnService().commitTransaction(session());
    }

}
