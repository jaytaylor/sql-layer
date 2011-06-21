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

import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.server.store.IndexRecordVisitor;
import com.akiban.server.test.it.ITBase;
import com.akiban.util.Strings;
import com.persistit.exception.PersistitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public final class GroupIndexUpdateIT extends ITBase {

    @Test
    public void coiGIsNoOrphan() {
        createGroupIndex(groupName, "name_when_sku", "c.name, o.when, i.sku");
        writeRows(
                createNewRow(c, 1L, "Horton"),
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(i, 101L, 11L, 1111),
                createNewRow(i, 102L, 11L, 2222),
                createNewRow(i, 103L, 11L, 3333),
                createNewRow(o, 12L, 1L, "02-02-2002"),
                createNewRow(a, 10001L, 1L, "Causeway")
        );
        // write
        checkIndex("name_when_sku",
                "Horton, 01-01-2001, 1111, 1, 11, 101",
                "Horton, 01-01-2001, 2222, 1, 11, 102",
                "Horton, 01-01-2001, 3333, 1, 11, 103"
        );
        // update
        dml().updateRow(
                session(),
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(o, 11L, 1L, "01-01-1999"), // party!
                null
        );
        checkIndex("name_when_sku",
                "Horton, 01-01-1999, 1111, 1, 11, 101",
                "Horton, 01-01-1999, 2222, 1, 11, 102",
                "Horton, 01-01-1999, 3333, 1, 11, 103"
        );
        // delete child
        dml().deleteRow(session(), createNewRow(i, 102L, 11L, 2222));
        checkIndex("name_when_sku",
                "Horton, 01-01-1999, 1111, 1, 11, 101",
                "Horton, 01-01-1999, 3333, 1, 11, 103"
        );
        // delete parent
        dml().deleteRow(session(), createNewRow(o, 11L, 1L, "01-01-2001"));
        checkIndex("name_when_sku");
    }


    @Before
    public void createTables() {
        c = createTable(SCHEMA, "c", "cid int key, name varchar(32)");
        o = createTable(SCHEMA, "o", "oid int key, c_id int, when varchar(32)", akibanFK("c_id", "c", "cid") );
        i = createTable(SCHEMA, "i", "iid int key, o_id int, sku int", akibanFK("o_id", "o", "oid") );
        a = createTable(SCHEMA, "a", "oid int key, c_id int, street varchar(56)", akibanFK("c_id", "c", "cid") );
        groupName = getUserTable(c).getGroup().getName();
    }

    @After
    public void forgetTables() throws PersistitException {
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
        a = null;
        groupName = null;
    }

    private void checkIndex(String groupIndexName, String... expected) {
        GroupIndex groupIndex = ddl().getAIS(session()).getGroup(groupName).getIndex(groupIndexName);
        checkIndex(groupIndex, expected);
    }

    private void checkIndex(GroupIndex groupIndex, String... expected) {
        final StringsIndexScanner scanner;
        try {
            scanner= persistitStore().traverse(session(), groupIndex, new StringsIndexScanner());
        } catch (PersistitException e) {
            throw new RuntimeException(e);
        }
        // Add the [] to each expected entry
        for (int i = 0; i < expected.length; ++i) {
            expected[i] = '[' + expected[i] + ']';
        }

        List<String> expectedList = Arrays.asList(expected);
        if (!expectedList.equals(scanner.strings())) {
            assertEquals("scan of " + groupIndex, Strings.join(expectedList), Strings.join(scanner.strings()));
            // just in case
            assertEquals("scan of " + groupIndex, expectedList, scanner.strings());
        }
    }

    private String groupName;
    private Integer c;
    private Integer o;
    private Integer i;
    private Integer a;

    // consts

    private static final String SCHEMA = "coia";

    // nested class

    private static class StringsIndexScanner extends IndexRecordVisitor {

        // IndexRecordVisitor interface

        @Override
        public void visit(List<Object> key) {
            _strings.add(String.valueOf(key));
        }

        // StringsIndexScanner interface

        public List<String> strings() {
            return _strings;
        }

        // object state

        private final List<String> _strings = new ArrayList<String>();
    }
}
