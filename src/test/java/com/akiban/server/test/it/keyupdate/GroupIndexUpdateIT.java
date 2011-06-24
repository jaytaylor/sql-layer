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
import com.akiban.ais.model.UserTable;
import com.akiban.qp.persistitadapter.OperatorStore;
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
import static org.junit.Assert.fail;

public final class GroupIndexUpdateIT extends ITBase {

    @Test
    public void coiGIsNoOrphan() {
        createGroupIndex(groupName, "name_when_sku", "c.name, o.when, i.sku");
        // write write write
        writeRows(createNewRow(c, 1L, "Horton"));
        checkIndex("name_when_sku",
                "Horton, null, null, 1, null, null => " + depthOf(c)
        );
        writeRows(createNewRow(o, 11L, 1L, "01-01-2001"));
        checkIndex("name_when_sku",
                "Horton, 01-01-2001, null, 1, 11, null => " + depthOf(o)
        );
        writeRows(createNewRow(i, 101L, 11L, 1111));
        checkIndex("name_when_sku",
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + depthOf(i)
        );
        writeRows(createNewRow(i, 102L, 11L, 2222));
        // write
        checkIndex("name_when_sku",
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + depthOf(i),
                "Horton, 01-01-2001, 2222, 1, 11, 102 => " + depthOf(i)
        );
        writeRows(createNewRow(i, 103L, 11L, 3333));
        // write
        checkIndex("name_when_sku",
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + depthOf(i),
                "Horton, 01-01-2001, 2222, 1, 11, 102 => " + depthOf(i),
                "Horton, 01-01-2001, 3333, 1, 11, 103 => " + depthOf(i)
        );
        writeRows(createNewRow(o, 12L, 1L, "02-02-2002"));
        writeRows(createNewRow(a, 10001L, 1L, "Causeway"));
        // write
        checkIndex("name_when_sku",
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + depthOf(i),
                "Horton, 01-01-2001, 2222, 1, 11, 102 => " + depthOf(i),
                "Horton, 01-01-2001, 3333, 1, 11, 103 => " + depthOf(i)
        );

        // update parent
        dml().updateRow(
                session(),
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(o, 11L, 1L, "01-01-1999"), // party!
                null
        );
        checkIndex("name_when_sku",
                "Horton, 01-01-1999, 1111, 1, 11, 101 => " + depthOf(i),
                "Horton, 01-01-1999, 2222, 1, 11, 102 => " + depthOf(i),
                "Horton, 01-01-1999, 3333, 1, 11, 103 => " + depthOf(i)
        );
        // update child
        dml().updateRow(
                session(),
                createNewRow(i, 102L, 11L, 2222),
                createNewRow(i, 102L, 11L, 2442),
                null
        );
        checkIndex("name_when_sku",
                "Horton, 01-01-1999, 1111, 1, 11, 101 => " + depthOf(i),
                "Horton, 01-01-1999, 2442, 1, 11, 102 => " + depthOf(i),
                "Horton, 01-01-1999, 3333, 1, 11, 103 => " + depthOf(i)
        );

        // delete child
        dml().deleteRow(session(), createNewRow(i, 102L, 11L, 222211));
        checkIndex("name_when_sku",
                "Horton, 01-01-1999, 1111, 1, 11, 101 => " + depthOf(i),
                "Horton, 01-01-1999, 3333, 1, 11, 103 => " + depthOf(i)
        );
        // delete parent
        dml().deleteRow(session(), createNewRow(o, 11L, 1L, "01-01-2001"));
        checkIndex("name_when_sku");
    }

    @Test
    public void createGIOnPopulatedTables() {
        writeRows(
                createNewRow(c, 1L, "Horton"),
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(i, 101L, 11L, 1111)
        );
        createGroupIndex(groupName, "name_when_sku", "c.name, o.when, i.sku");
        checkIndex("name_when_sku",
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + depthOf(i)
        );
    }

    @Test(expected = OperatorStore.UniqueIndexUnsupportedException.class)
    public void uniqueGI() {
        try {
            createGroupIndex(groupName, "name_when_sku", true, "c.name, o.when, i.sku");
        } catch (UnsupportedOperationException e) {
            // irrelevant
        }
        try {
            writeRows(
                    createNewRow(c, 1L, "Horton")
            );
        } catch (final Exception e) {
            for (Throwable cause = e; cause != null; cause = cause.getCause()) {
                if (cause instanceof OperatorStore.UniqueIndexUnsupportedException) {
                    throw (OperatorStore.UniqueIndexUnsupportedException)cause;
                }
            }
            throw new RuntimeException(e);
        }
        fail("expected an exception of some sort");
    }

    @Test
    public void ihIndexNoOrphans() {
        String indexName = "sku_handling-instructions";
        createGroupIndex(groupName, indexName, "i.sku, h.handling_instructions");
        writeRows(
                createNewRow(c, 1L, "Horton"),
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(i, 101L, 11L, 1111),
                createNewRow(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName, "1111, handle with care, 1, 11, 101, 1001 => " + depthOf(h));

        // delete from root on up
        dml().deleteRow(session(), createNewRow(c, 1L, "Horton"));
        checkIndex(indexName, "1111, handle with care, 1, 11, 101, 1001 => " + depthOf(h));

        dml().deleteRow(session(), createNewRow(o, 11L, 1L, "01-01-2001 => " + depthOf(h)));
        checkIndex(indexName, "1111, handle with care, null, 11, 101, 1001 => " + depthOf(h));

        dml().deleteRow(session(), createNewRow(i, 101L, 11L, 1111));
        checkIndex(indexName);

        dml().deleteRow(session(), createNewRow(h, 1001L, 101L, "handle with care"));
        checkIndex(indexName);
    }

    @Test
    public void ihIndexOIsOrphaned() {
        String indexName = "sku_handling-instructions";
        createGroupIndex(groupName, indexName, "i.sku, h.handling_instructions");
        writeRows(
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(i, 101L, 11L, 1111),
                createNewRow(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName, "1111, handle with care, 1, 11, 101, 1001 => " + depthOf(h));

        // delete from root on up

        dml().deleteRow(session(), createNewRow(o, 11L, 1L, "01-01-2001"));
        checkIndex(indexName, "1111, handle with care, null, 11, 101, 1001 => " + depthOf(h));

        dml().deleteRow(session(), createNewRow(i, 101L, 11L, 1111));
        checkIndex(indexName);

        dml().deleteRow(session(), createNewRow(h, 1001L, 101L, "handle with care"));
        checkIndex(indexName);
    }

    @Test
    public void ihIndexIIsOrphaned() {
        String indexName = "sku_handling-instructions";
        createGroupIndex(groupName, indexName, "i.sku, h.handling_instructions");
        writeRows(
                createNewRow(c, 1L, "Horton"),
                createNewRow(c, -1L, "Notroh"),
                createNewRow(i, 101L, 11L, 1111),
                createNewRow(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName, "1111, handle with care, null, 11, 101, 1001 => " + depthOf(h));

        // delete from root on up

        dml().deleteRow(session(), createNewRow(i, 101L, 11L, 1111));
        checkIndex(indexName);

        dml().deleteRow(session(), createNewRow(h, 1001L, 101L, "handle with care"));
        checkIndex(indexName);
    }

    @Test
    public void ihIndexIIsOrphanedButCExists() {
        String indexName = "sku_handling-instructions";
        createGroupIndex(groupName, indexName, "i.sku, h.handling_instructions");
        writeRows(
                createNewRow(c, 1L, "Horton"),
                createNewRow(i, 101L, 11L, 1111),
                createNewRow(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName, "1111, handle with care, null, 11, 101, 1001 => " + depthOf(h));

        // delete from root on up

        dml().deleteRow(session(), createNewRow(i, 101L, 11L, 1111));
        checkIndex(indexName);

        dml().deleteRow(session(), createNewRow(h, 1001L, 101L, "handle with care"));
        checkIndex(indexName);
    }

    @Test
    public void ihIndexHIsOrphaned() {
        String indexName = "sku_handling-instructions";
        createGroupIndex(groupName, indexName, "i.sku, h.handling_instructions");
        writeRows(
                createNewRow(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName);

        // delete from root on up

        dml().deleteRow(session(), createNewRow(h, 1001L, 101L, "handle with care"));
        checkIndex(indexName);
    }

    @Test
    public void adoptionChangesHKeyNoCustomer() {
        String indexName = "sku_handling-instructions";
        createGroupIndex(groupName, indexName, "i.sku, h.handling_instructions");
        writeRows(
                createNewRow(i, 101L, 11L, 1111),
                createNewRow(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName,
                "1111, handle with care, null, 11, 101, 1001 => " + depthOf(h)
        );

        // bring an o that adopts the i
        dml().writeRow(session(), createNewRow(o, 11L, 1L, "01-01-2001"));
        checkIndex(indexName,
                "1111, handle with care, 1, 11, 101, 1001 => " + depthOf(h)
        );
    }

    @Test
    public void adoptionChangesHKeyWithC() {
        String indexName = "sku_handling-instructions";
        createGroupIndex(groupName, indexName, "i.sku, h.handling_instructions");
        writeRows(
                createNewRow(c, 1L, "Horton"),
                createNewRow(i, 101L, 11L, 1111),
                createNewRow(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName,
                "1111, handle with care, null, 11, 101, 1001 => " + depthOf(h)
        );

        // bring an o that adopts the i
        dml().writeRow(session(), createNewRow(o, 11L, 1L, "01-01-2001"));
        checkIndex(indexName,
                "1111, handle with care, 1, 11, 101, 1001 => " + depthOf(h)
        );
    }

    @Test
    public void testTwoBranches() {
        createGroupIndex(groupName, "when_name", "o.when, c.name");
        createGroupIndex(groupName, "name_street", "c.name, a.street");
        writeRows(
                createNewRow(c, 1L, "Horton"),
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(o, 12L, 1L, "03-03-2003"),
                createNewRow(a, 21L, 1L, "Harrington"),
                createNewRow(a, 22L, 1L, "Causeway"),
                createNewRow(c, 2L, "David"),
                createNewRow(o, 13L, 2L, "02-02-2002"),
                createNewRow(a, 23L, 2L, "Highland")
        );

        checkIndex(
                "when_name",
                "01-01-2001, Horton, 1, 11 => " + depthOf(o),
                "02-02-2002, David, 2, 13 => " + depthOf(o),
                "03-03-2003, Horton, 1, 12 => " + depthOf(o)
        );
        checkIndex(
                "name_street",
                "David, Highland, 2, 23 => " + depthOf(a),
                "Horton, Causeway, 1, 22 => " + depthOf(a),
                "Horton, Harrington, 1, 21 => " + depthOf(a)
        );
    }

    @Test
    public void updateModifiesHKeyWithinBranch() {
        // branch is I-H, we're modifying the hkey of an H
        createGroupIndex(groupName, "sku_handling", "i.sku, h.handling_instructions");
        writeRows(
                createNewRow(c, 1L, "Horton"),
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(i, 101L, 11L, "1111"),
                createNewRow(h, 1001L, 101L, "don't break"),
                createNewRow(c, 2L, "David"),
                createNewRow(o, 12L, 2L, "02-02-2002"),
                createNewRow(i, 102L, 12L, "2222")
        );
        checkIndex(
                "sku_handling",
                "1111, don't break, 1, 11, 101, 1001 => " + depthOf(h)
        );

        dml().updateRow(
                session(),
                createNewRow(h, 1001L, 101L, "don't break"),
                createNewRow(h, 1001L, 102L, "don't break"),
                null
        );

        checkIndex(
                "sku_handling",
                "2222, don't break, 2, 12, 102, 1001 => " + depthOf(h)
        );
    }

    @Test
    public void updateModifiesHKeyDirectlyAboveBranch() {
        // branch is I-H, we're modifying the hkey of an I
        createGroupIndex(groupName, "sku_handling", "i.sku, h.handling_instructions");
        writeRows(
                createNewRow(c, 1L, "Horton"),
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(i, 101L, 11L, "1111"),
                createNewRow(h, 1001L, 101L, "don't break"),
                createNewRow(c, 2L, "David"),
                createNewRow(o, 12L, 2L, "02-02-2002"),
                createNewRow(i, 102L, 12L, "2222")
        );
        checkIndex(
                "sku_handling",
                "1111, don't break, 1, 11, 101, 1001 => " + depthOf(h)
        );

        dml().updateRow(
                session(),
                createNewRow(i, 101L, 11L, "1111"),
                createNewRow(i, 101L, 12L, "1111"),
                null
        );

        checkIndex(
                "sku_handling",
                "1111, don't break, 2, 12, 101, 1001 => " + depthOf(h)
        );
    }

    @Test
    public void updateModifiesHKeyHigherAboveBranch() {
        // branch is I-H, we're modifying the hkey of an O referenced by an I
        createGroupIndex(groupName, "sku_handling", "i.sku, h.handling_instructions");
        writeRows(
                createNewRow(c, 1L, "Horton"),
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(i, 101L, 11L, "1111"),
                createNewRow(h, 1001L, 101L, "don't break"),
                createNewRow(c, 2L, "David"),
                createNewRow(o, 12L, 2L, "02-02-2002"),
                createNewRow(i, 102L, 12L, "2222")
        );
        checkIndex(
                "sku_handling",
                "1111, don't break, 1, 11, 101, 1001 => " + depthOf(h)
        );

        dml().updateRow(
                session(),
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(o, 11L, 2L, "01-01-2001"),
                null
        );

        checkIndex(
                "sku_handling",
                "1111, don't break, 2, 11, 101, 1001 => " + depthOf(h)
        );
    }

    @Test
    public void updateOrphansHKeyWithinBranch() {
        // branch is I-H, we're modifying the hkey of an H
        createGroupIndex(groupName, "sku_handling", "i.sku, h.handling_instructions");
        writeRows(
                createNewRow(c, 1L, "Horton"),
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(i, 101L, 11L, "1111"),
                createNewRow(h, 1001L, 101L, "don't break"),
                createNewRow(c, 2L, "David"),
                createNewRow(o, 12L, 2L, "02-02-2002"),
                createNewRow(i, 102L, 12L, "2222")
        );
        checkIndex(
                "sku_handling",
                "1111, don't break, 1, 11, 101, 1001 => " + depthOf(h)
        );

        dml().updateRow(
                session(),
                createNewRow(h, 1001L, 101L, "don't break"),
                createNewRow(h, 1001L, 666L, "don't break"),
                null
        );

        checkIndex("sku_handling");
    }

    @Test
    public void updateMovesHKeyWithinBranch() {
        // branch is I-H, we're modifying the hkey of an H
        createGroupIndex(groupName, "sku_handling", "i.sku, h.handling_instructions");
        writeRows(
                createNewRow(c, 1L, "Horton"),
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(i, 101L, 11L, "1111"),
                createNewRow(h, 1001L, 101L, "don't break"),
                createNewRow(c, 2L, "David"),
                createNewRow(o, 12L, 2L, "02-02-2002"),
                createNewRow(i, 102L, 12L, "2222"),

                createNewRow(o, 66L, 6L, "03-03-2003"),
                createNewRow(i, 666L, 66L, "6666")
        );
        checkIndex(
                "sku_handling",
                "1111, don't break, 1, 11, 101, 1001 => " + depthOf(h)
        );

        dml().updateRow(
                session(),
                createNewRow(h, 1001L, 101L, "don't break"),
                createNewRow(h, 1001L, 666L, "don't break"),
                null
        );

        checkIndex(
                "sku_handling",
                "6666, don't break, 6, 66, 666, 1001 => " + depthOf(h)
        );
    }

    @Test
    public void updateOrphansHKeyDirectlyAboveBranch() {
        // branch is I-H, we're modifying the hkey of an I
        createGroupIndex(groupName, "sku_handling", "i.sku, h.handling_instructions");
        writeRows(
                createNewRow(c, 1L, "Horton"),
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(i, 101L, 11L, "1111"),
                createNewRow(h, 1001L, 101L, "don't break"),
                createNewRow(c, 2L, "David"),
                createNewRow(o, 12L, 2L, "02-02-2002"),
                createNewRow(i, 102L, 12L, "2222")
        );
        checkIndex(
                "sku_handling",
                "1111, don't break, 1, 11, 101, 1001 => " + depthOf(h)
        );

        dml().updateRow(
                session(),
                createNewRow(i, 101L, 11L, "1111"),
                createNewRow(i, 101L, 66L, "1111"),
                null
        );

        checkIndex(
                "sku_handling",
                "1111, don't break, null, 66, 101, 1001 => " + depthOf(h)
        );
    }

    @Test
    public void updateOrphansHKeyHigherAboveBranch() {
        // branch is I-H, we're modifying the hkey of an O referenced by an I
        createGroupIndex(groupName, "sku_handling", "i.sku, h.handling_instructions");
        writeRows(
                createNewRow(c, 1L, "Horton"),
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(i, 101L, 11L, "1111"),
                createNewRow(h, 1001L, 101L, "don't break"),
                createNewRow(c, 2L, "David"),
                createNewRow(o, 12L, 2L, "02-02-2002"),
                createNewRow(i, 102L, 12L, "2222")
        );
        checkIndex(
                "sku_handling",
                "1111, don't break, 1, 11, 101, 1001 => " + depthOf(h)
        );

        dml().updateRow(
                session(),
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(o, 11L, 6L, "01-01-2001"),
                null
        );

        checkIndex(
                "sku_handling",
                "1111, don't break, 6, 11, 101, 1001 => " + depthOf(h)
        );
    }

    /**
     * Create the endgame of {@linkplain #updateOrphansHKeyHigherAboveBranch} initially, as a santy check
     */
    @Test
    public void originallyOrphansHKeyHigherAboveBranch() {
        createGroupIndex(groupName, "sku_handling", "i.sku, h.handling_instructions");
        writeRows(
                createNewRow(c, 1L, "Horton"),
                createNewRow(o, 11L, 6L, "01-01-2001"),
                createNewRow(i, 101L, 11L, "1111"),
                createNewRow(h, 1001L, 101L, "don't break"),
                createNewRow(c, 2L, "David"),
                createNewRow(o, 12L, 2L, "02-02-2002"),
                createNewRow(i, 102L, 12L, "2222")
        );
        checkIndex(
                "sku_handling",
                "1111, don't break, 6, 11, 101, 1001 => " + depthOf(h)
        );
    }

    @Before
    public void createTables() {
        c = createTable(SCHEMA, "c", "cid int key, name varchar(32)");
        o = createTable(SCHEMA, "o", "oid int key, c_id int, when varchar(32)", akibanFK("c_id", "c", "cid") );
        i = createTable(SCHEMA, "i", "iid int key, o_id int, sku int", akibanFK("o_id", "o", "oid") );
        h = createTable(SCHEMA, "h", "sid int key, i_id int, handling_instructions varchar(32)", akibanFK("i_id", "i", "iid") );
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

    private String depthOf(int tableId) {
        UserTable userTable = ddl().getAIS(session()).getUserTable(tableId);
        return String.format("%d (Integer)", userTable.getDepth());
    }

    private String groupName;
    private Integer c;
    private Integer o;
    private Integer i;
    private Integer h;
    private Integer a;

    // consts

    private static final String SCHEMA = "coia";

    // nested class

    private static class StringsIndexScanner extends IndexRecordVisitor {

        // IndexRecordVisitor interface

        @Override
        public void visit(List<?> key, Object value) {
            String asString = (value == null)
                    ? String.format("%s => null", key)
                    : String.format("%s => %s (%s)", key, value, value.getClass().getSimpleName());
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
