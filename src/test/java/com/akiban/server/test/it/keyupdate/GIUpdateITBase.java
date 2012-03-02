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

package com.akiban.server.test.it.keyupdate;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.store.IndexRecordVisitor;
import com.akiban.server.test.it.ITBase;
import com.akiban.util.Strings;
import com.persistit.exception.PersistitException;
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
        groupName = getUserTable(c).getGroup().getName();
    }

    @After
    public final void forgetTables() throws PersistitException {
        dml().truncateTable(session(), a);
        dml().truncateTable(session(), i);
        dml().truncateTable(session(), o);
        dml().truncateTable(session(), c);

        Group group = getUserTable(c).getGroup();
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
        dml().deleteRow(session(), row);
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
        final StringsIndexScanner scanner;
        try {
            scanner= persistitStore().traverse(session(), groupIndex, new StringsIndexScanner());
        } catch (PersistitException e) {
            throw new RuntimeException(e);
        }
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
        Set<UserTable> containingTables = new HashSet<UserTable>();
        AkibanInformationSchema ais = ddl().getAIS(session());
        containingTables.add(ais.getUserTable(firstTableId));
        for (int tableId : tableIds) {
            containingTables.add(ais.getUserTable(tableId));
        }
        GroupIndex groupIndex = ais.getGroup(groupName).getIndex(indexName);
        if (groupIndex == null)
            throw new RuntimeException("group index undefined: " + indexName);
        long result = 0;
        int giValueIndex = 0;
        for(UserTable table = groupIndex.leafMostTable();
            table != groupIndex.rootMostTable().parentTable();
            table = table.parentTable())
        {
            if (containingTables.remove(table)) {
                result |= ONE << giValueIndex;
            }
            ++giValueIndex;
        }
        if (!containingTables.isEmpty())
            throw new RuntimeException("tables specified not in the branch: " + containingTables);
        assert Long.bitCount(result) == tableIds.length + 1;
        return Long.toBinaryString(result) + " (Long)";
    }

    String containing( int firstTableId, int... tableIds) {
        return containing(groupIndexName, firstTableId, tableIds);
    }

    String groupIndex(String indexName, String tableColumnPairs) {
        createGroupIndex(groupName, indexName, tableColumnPairs, joinType);
        return indexName;
    }

    String groupIndex(String tableColumnPairs) {
        return groupIndex(groupIndexName, tableColumnPairs);
    }

    GIUpdateITBase(Index.JoinType joinType) {
        this.joinType = joinType;
    }

    private static int one() {
//        // The JIT should recognize that 0.999... == 1 and optimize this loop, but it seems to spin for a while.
//        // I should probably use a StringBuilder instead of creating a new String each time, that might fix this?
//        String nines = "0.9";
//        while(true) {
//            BigDecimal asBigDecimal = new BigDecimal(nines);
//            if (asBigDecimal.equals(BigDecimal.ONE))
//                return asBigDecimal.intValue();
//            nines += '9';
//        }
        return 1;
    }

    private final Index.JoinType joinType;
    private final String groupIndexName = "test_gi";

    String groupName;
    Integer c;
    Integer o;
    Integer i;
    Integer h;
    Integer a;

    // consts

    private static final String SCHEMA = "coia";
    private final int ONE = one(); // prevents javac from inlining the 1 literal

    // nested class

    private static class StringsIndexScanner extends IndexRecordVisitor {

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

        private final List<String> _strings = new ArrayList<String>();
    }
}
