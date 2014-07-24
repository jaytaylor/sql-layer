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

package com.foundationdb.server.test.it.keyupdate;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.service.transaction.TransactionService.CloseableTransaction;
import com.foundationdb.server.store.IndexRecordVisitor;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.util.Exceptions;
import com.foundationdb.util.Strings;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public abstract class GIUpdateITBase extends ITBase {

    @Before
    public final void createTables() {
        c = createTable(SCHEMA, "c", "cid int not null primary key, name varchar(32)");
        o = createTable(SCHEMA, "o", "oid int not null primary key, c_id int, when varchar(32)", akibanFK("c_id", "c", "cid") );
        i = createTable(SCHEMA, "i", "iid int not null primary key, o_id int, sku int", akibanFK("o_id", "o", "oid") );
        h = createTable(SCHEMA, "h", "sid int not null primary key, i_id int, handling_instructions varchar(32)", akibanFK("i_id", "i", "iid") );
        a = createTable(SCHEMA, "a", "oid int not null primary key, c_id int, street varchar(56)", akibanFK("c_id", "c", "cid") );
        groupName = getTable(c).getGroup().getName();
    }

    @After
    public final void forgetTables() {
        int[] ids = { a, h, i, o, c};
        int idIndex = 0;
        for(int i = 5; i >= 0; --i) {
            try {
                while(idIndex < ids.length) {
                    dml().truncateTable(session(), ids[idIndex]);
                    ++idIndex;
                }
                break;
            } catch(Exception e) {
                if(!Exceptions.isRollbackException(e) || i == 0) {
                    throw e;
                }
            }
        }

        Group group = getTable(c).getGroup();
        for (GroupIndex groupIndex : group.getIndexes()) {
            checkIndex(groupIndex);
        }

        c = null;
        o = null;
        i = null;
        h = null;
        a = null;
        groupName = null;
    }

    void writeAndCheck(NewRow row, String... expectedGiEntries) {
        writeRows(row);
        checkIndex(groupIndexName, expectedGiEntries);
    }

    void deleteAndCheck(NewRow row, String... expectedGiEntries) {
        dml().deleteRow(session(), row, false);
        checkIndex(groupIndexName, expectedGiEntries);
    }

    void deleteCascadeAndCheck(NewRow row, String... expectedGiEntries) {
        dml().deleteRow(session(), row, true);
        checkIndex(groupIndexName, expectedGiEntries);
    }

    void updateAndCheck(NewRow oldRow, NewRow newRow, String... expectedGiEntries) {
        dml().updateRow(session(), oldRow, newRow, null);
        checkIndex(groupIndexName, expectedGiEntries);
    }

    void checkIndex(String indexName, String... expected) {
        GroupIndex groupIndex = ddl().getAIS(session()).getGroup(groupName).getIndex(indexName);
        checkIndex(groupIndex, expected);
    }

    private void checkIndex(GroupIndex groupIndex, String... expected) {
        try(CloseableTransaction txn = txnService().beginCloseableTransaction(session())) {
            checkIndexInternal(groupIndex, expected);
            txn.commit();
        }
    }

    private void checkIndexInternal(GroupIndex groupIndex, String... expected) {
        StringsIndexScanner scanner = store().traverse(session(), groupIndex, new StringsIndexScanner(), -1, 0);
        // convert "a, b, c => d" to "[a, b, c] => d"
        for (int i = 0; i < expected.length; ++i) {
            String original = expected[i];
            int arrow = original.indexOf(" => ");
            String keys = original.substring(0, arrow);
            String value = original.substring(arrow + " => ".length());
            expected[i] = String.format("[%s] => %s", keys, value);
        }

        List<String> expectedList = Arrays.asList(expected);
        if (!expectedList.equals(scanner.strings())) {
            assertEquals("scan of " + groupIndex, Strings.join(expectedList), Strings.join(scanner.strings()));
            // just in case
            assertEquals("scan of " + groupIndex, expectedList, scanner.strings());
        }
    }

    String containing(String indexName, int firstTableId, int... tableIds) {
        Set<Table> containingTables = new HashSet<>();
        AkibanInformationSchema ais = ddl().getAIS(session());
        containingTables.add(ais.getTable(firstTableId));
        for (int tableId : tableIds) {
            containingTables.add(ais.getTable(tableId));
        }
        GroupIndex groupIndex = ais.getGroup(groupName).getIndex(indexName);
        if (groupIndex == null)
            throw new RuntimeException("group index undefined: " + indexName);
        long result = 0;
        for(Table table = groupIndex.leafMostTable();
            table != groupIndex.rootMostTable().getParentTable();
            table = table.getParentTable())
        {
            if (containingTables.remove(table)) {
                result |= 1 << table.getDepth();
            }
        }
        if (!containingTables.isEmpty())
            throw new RuntimeException("tables specified not in the branch: " + containingTables);
        assert Long.bitCount(result) == tableIds.length + 1;
        return Long.toBinaryString(result) + " (Long)";
    }

    String containing( int firstTableId, int... tableIds) {
        return containing(groupIndexName, firstTableId, tableIds);
    }

    String groupIndexNamed(String indexName, String... columnNames) {
        createGroupIndex(groupName, indexName, joinType, columnNames);
        return indexName;
    }

    String groupIndex(String... columnNames) {
        return groupIndexNamed(groupIndexName, columnNames);
    }

    GIUpdateITBase(Index.JoinType joinType) {
        this.joinType = joinType;
    }

    private final Index.JoinType joinType;
    private final String groupIndexName = "test_gi";

    TableName groupName;
    Integer c;
    Integer o;
    Integer i;
    Integer h;
    Integer a;

    // consts

    private static final String SCHEMA = "coia";

    // nested class

    private static class StringsIndexScanner extends IndexRecordVisitor {

        // IndexVisitor interface

        @Override
        public boolean groupIndex()
        {
            return true;
        }

        // IndexRecordVisitor interface

        @Override
        public void visit(List<?> key, Object value) {
            final String asString;
            if (value == null) {
                asString = String.format("%s => null", key);
            }
            else {
                final String className;
                if (value instanceof Long) {
                    value = Long.toBinaryString((Long)value);
                    className = "Long";
                }
                else {
                    className = value.getClass().getSimpleName();
                }
                asString = String.format("%s => %s (%s)", key, value, className);
            }
            _strings.add(asString);
        }

        // StringsIndexScanner interface

        public List<String> strings() {
            return _strings;
        }

        // object state

        private final List<String> _strings = new ArrayList<>();
    }
}
