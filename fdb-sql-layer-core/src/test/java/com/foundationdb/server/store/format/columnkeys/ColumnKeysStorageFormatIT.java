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

package com.foundationdb.server.store.format.columnkeys;

import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.StorageDescriptionInvalidException;
import com.foundationdb.server.store.FDBTransactionService;
import com.foundationdb.server.store.FDBTransactionService.TransactionState;
import com.foundationdb.server.store.format.FDBStorageDescription;
import com.foundationdb.server.test.it.FDBITBase;
import com.foundationdb.server.test.it.qp.TestRow;
import com.foundationdb.KeyValue;
import com.foundationdb.Range;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple2;

import org.junit.Test;

import static org.junit.Assert.*;

import java.util.*;

// Only tests the basic population of storage. More advanced
// navigation is tested through YAML ITs.
public class ColumnKeysStorageFormatIT  extends FDBITBase
{
    private static final String SCHEMA = "test";

    @Test
    public void decimalsAllowed() {
        createFromDDL(SCHEMA,
          "CREATE TABLE t1(id INT PRIMARY KEY NOT NULL, d DECIMAL(6,2)) STORAGE_FORMAT column_keys;");
    }

    @Test(expected = StorageDescriptionInvalidException.class)
    public void indexNotAllowed() {
        createFromDDL(SCHEMA,
          "CREATE TABLE t1(id INT PRIMARY KEY NOT NULL, n BIGINT);" +
          "CREATE INDEX i1 ON t1(n) STORAGE_FORMAT column_keys;");
    }

    @Test
    public void groupStructure() {
        createFromDDL(SCHEMA,
          "CREATE TABLE t1(id INT PRIMARY KEY NOT NULL, abbrev CHAR(2), name VARCHAR(128)) STORAGE_FORMAT column_keys;" +
          "CREATE TABLE t2(id INT PRIMARY KEY NOT NULL, sid INT, GROUPING FOREIGN KEY(sid) REFERENCES t1(id), name VARCHAR(128));");
        int t1 = ddl().getTableId(session(), new TableName(SCHEMA, "t1"));
        int t2 = ddl().getTableId(session(), new TableName(SCHEMA, "t2"));

        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        RowType t1Type = schema.tableRowType(getTable(t1));
        RowType t2Type = schema.tableRowType(getTable(t2));
        StoreAdapter adapter = newStoreAdapter();

        txnService().beginTransaction(session());

        Object[] r1 = { 1L, "MA", "Massachusetts" };
        Object[] r101 = { 101L, 1L, "Boston" };
        Object[] r102 = { 102L, 1L, "Cambridge" };
        Object[] r2 = { 2L, "NY", "New York" };
        Object[] r201 = { 201L, 2L, "New York" };
        Object[] r202 = { 202L, 2L, "Albany" };
        writeRow(t1, r1);
        writeRow(t2, r101);
        writeRow(t2, r102);
        writeRow(t1, r2);
        writeRow(t2, r201);
        writeRow(t2, r202);

        List<Object> raw = Arrays.<Object>asList(
          Arrays.asList(Arrays.asList(1L, 1L, "abbrev"), Arrays.asList("MA")),
          Arrays.asList(Arrays.asList(1L, 1L, "id"), Arrays.asList(1L)),
          Arrays.asList(Arrays.asList(1L, 1L, "name"), Arrays.asList("Massachusetts")),
          Arrays.asList(Arrays.asList(1L, 1L, 2L, 101L, "id"), Arrays.asList(101L)),
          Arrays.asList(Arrays.asList(1L, 1L, 2L, 101L, "name"), Arrays.asList("Boston")),
          Arrays.asList(Arrays.asList(1L, 1L, 2L, 101L, "sid"), Arrays.asList(1L)),
          Arrays.asList(Arrays.asList(1L, 1L, 2L, 102L, "id"), Arrays.asList(102L)),
          Arrays.asList(Arrays.asList(1L, 1L, 2L, 102L, "name"), Arrays.asList("Cambridge")),
          Arrays.asList(Arrays.asList(1L, 1L, 2L, 102L, "sid"), Arrays.asList(1L)),
          Arrays.asList(Arrays.asList(1L, 2L, "abbrev"), Arrays.asList("NY")),
          Arrays.asList(Arrays.asList(1L, 2L, "id"), Arrays.asList(2L)),
          Arrays.asList(Arrays.asList(1L, 2L, "name"), Arrays.asList("New York")),
          Arrays.asList(Arrays.asList(1L, 2L, 2L, 201L, "id"), Arrays.asList(201L)),
          Arrays.asList(Arrays.asList(1L, 2L, 2L, 201L, "name"), Arrays.asList("New York")),
          Arrays.asList(Arrays.asList(1L, 2L, 2L, 201L, "sid"), Arrays.asList(2L)),
          Arrays.asList(Arrays.asList(1L, 2L, 2L, 202L, "id"), Arrays.asList(202L)),
          Arrays.asList(Arrays.asList(1L, 2L, 2L, 202L, "name"), Arrays.asList("Albany")),
          Arrays.asList(Arrays.asList(1L, 2L, 2L, 202L, "sid"), Arrays.asList(2L))
        );
        assertEquals(raw, treeTuples(t1Type.table().getGroup()));

        Row[] expected = {
            new TestRow(t1Type, r1),
            new TestRow(t2Type, r101),
            new TestRow(t2Type, r102),
            new TestRow(t1Type, r2),
            new TestRow(t2Type, r201),
            new TestRow(t2Type, r202)
        };
        compareRows(expected, adapter.newGroupCursor(t1Type.table().getGroup()));
        txnService().commitTransaction(session());
    }

    protected List<Object> treeTuples(HasStorage object) {
        byte[] prefix = ((FDBStorageDescription)object.getStorageDescription()).getPrefixBytes();
        TransactionState tr = ((FDBTransactionService)txnService()).getTransaction(session());
        List<Object> result = new ArrayList<>();
        for (KeyValue kv : tr.getRangeIterator(Range.startsWith(prefix),Transaction.ROW_LIMIT_UNLIMITED )) {
            byte[] key = kv.getKey();
            byte[] value = kv.getValue();
            key = Arrays.copyOfRange(key, prefix.length, key.length);
            result.add(Arrays.asList(Tuple2.fromBytes(key).getItems(),
                                     Tuple2.fromBytes(value).getItems()));
        }
        return result;
    }

}
