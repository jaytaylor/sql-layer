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
import com.akiban.server.store.IndexRecordVisitor;
import com.akiban.server.test.it.ITBase;
import com.akiban.util.Strings;
import com.persistit.exception.PersistitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public final class NewGiUpdateIT extends ITBase {

    @Test
    public void t() {
        createGroupIndex(groupName, "gi1", "c.name,o.when", Index.JoinType.LEFT);
        writeRow(c, 1L, "John");
        checker()
                .gi("gi1")
                .entry("John, null, 1, null").backedBy(c)
                .gi("gi2")
        .done();
    }

    @Before
    public final void createTables() {
        c = createTable(SCHEMA, "c", "cid int key, name varchar(32)");
        o = createTable(SCHEMA, "o", "oid int key, c_id int, when varchar(32)", akibanFK("c_id", "c", "cid") );
        i = createTable(SCHEMA, "i", "iid int key, o_id int, sku int", akibanFK("o_id", "o", "oid") );
        h = createTable(SCHEMA, "h", "sid int key, i_id int, handling_instructions varchar(32)", akibanFK("i_id", "i", "iid") );
        a = createTable(SCHEMA, "a", "oid int key, c_id int, street varchar(56)", akibanFK("c_id", "c", "cid") );
        groupName = getUserTable(c).getGroup().getName();
    }

    @After
    public final void forgetTables() throws PersistitException {
        dml().truncateTable(session(), a);
        dml().truncateTable(session(), i);
        dml().truncateTable(session(), o);
        dml().truncateTable(session(), c);

        GisChecker emptyChecker = checker();
        for (GroupIndex gi : group().getIndexes()) {
            emptyChecker.gi(gi.getIndexName().getName());
        }
        emptyChecker.done();

        c = null;
        o = null;
        i = null;
        h = null;
        a = null;
        groupName = null;
        assertEquals(Collections.<GisChecker>emptySet(), unfinishedCheckers);
    }

    private GisChecker checker() {
        StackTraceElement callerFrame = Thread.currentThread().getStackTrace()[2];
        GisChecker checker = new GiCheckerImpl(callerFrame);
        boolean added = unfinishedCheckers.add(checker);
        assert added : unfinishedCheckers;
        return checker;
    }

    private Group group() {
        return ddl().getAIS(session()).getGroup(groupName);
    }

    private String groupName;
    private Integer c;
    private Integer o;
    private Integer i;
    private Integer h;
    private Integer a;
    private final Set<GisChecker> unfinishedCheckers = new HashSet<GisChecker>();

    private static final String SCHEMA = "coia";

    // nested classes

    private interface GisChecker {
        GiChecker gi(String giName);
        public void done();
    }

    private interface GiChecker extends GisChecker {
        public GiTablesChecker entry(String key);
    }

    private interface GiTablesChecker {
        GiChecker backedBy(int firstTableId, int... tableIds);
    }

    private class GiCheckerImpl implements GiChecker, GiTablesChecker {

        @Override
        public GiChecker gi(String giName) {
            assertEquals("", scratch.toString());
            giToCheck = group().getIndex(giName);
            assertNotNull("no GI named " + giName, giToCheck);
            List<String> oldList = expectedStrings.put(giToCheck, new ArrayList<String>());
            assertEquals(null, oldList);
            return this;
        }

        @Override
        public GiTablesChecker entry(String key) {
            assertEquals("", scratch.toString());
            scratch.append(key).append(" => ");
            return this;
        }

        @Override
        public GiChecker backedBy(int firstTableId, int... tableIds) {
            String scratchString = scratch.toString();
            assertTrue(scratchString, scratchString.endsWith(" => "));
            assertNotNull(giToCheck);

            Set<UserTable> containingTables = new HashSet<UserTable>();
            AkibanInformationSchema ais = ddl().getAIS(session());
            containingTables.add(ais.getUserTable(firstTableId));
            for (int tableId : tableIds) {
                containingTables.add(ais.getUserTable(tableId));
            }
            long result = 0;
            int giValueIndex = 0;
            for(UserTable table = giToCheck.leafMostTable();
                table != giToCheck.rootMostTable().parentTable();
                table = table.parentTable())
            {
                if (containingTables.remove(table)) {
                    result |= 1 << giValueIndex;
                }
                ++giValueIndex;
            }
            if (!containingTables.isEmpty())
                throw new RuntimeException("tables specified not in the branch: " + containingTables);
            assert Long.bitCount(result) == tableIds.length + 1;
            String valueAsString =  Long.toBinaryString(result) + " (Long)";

            expectedStrings.get(giToCheck).add(scratch.append(valueAsString).toString());
            scratch.setLength(0);

            return this;
        }

        public void done() {
            Collection<GroupIndex> gis = group().getIndexes();
            Set<GroupIndex> uncheckedGis = new HashSet<GroupIndex>(gis);
            if (gis.size() != uncheckedGis.size())
                fail(gis + ".size() != " + uncheckedGis + ".size()");

            for(Map.Entry<GroupIndex,List<String>> checkPair : expectedStrings.entrySet()) {
                GroupIndex gi = checkPair.getKey();
                List<String> expected = checkPair.getValue();
                checkIndex(gi, expected);
                uncheckedGis.remove(gi);
            }

            if (!uncheckedGis.isEmpty()) {
                List<String> uncheckedGiNames = new ArrayList<String>();
                for (GroupIndex gi : uncheckedGis) {
                    uncheckedGiNames.add(gi.getIndexName().getName());
                }
                fail("unchecked GIs: " + uncheckedGiNames.toString());
            }
            unfinishedCheckers.remove(this);
        }

        @Override
        public String toString() {
            return String.format("%s at line %d", frame.getMethodName(), frame.getLineNumber());
        }

        private void checkIndex(GroupIndex groupIndex, List<String> expected) {
            final StringsIndexScanner scanner;
            try {
                scanner= persistitStore().traverse(session(), groupIndex, new StringsIndexScanner());
            } catch (PersistitException e) {
                throw new RuntimeException(e);
            }
            // convert "a, b, c => d" to "[a, b, c] => d"
            for (int i = 0; i < expected.size(); ++i) {
                String original = expected.get(i);
                int arrow = original.indexOf(" => ");
                String keys = original.substring(0, arrow);
                String value = original.substring(arrow + " => ".length());
                expected.set(i, String.format("[%s] => %s", keys, value));
            }

            if (!expected.equals(scanner.strings())) {
                assertEquals("scan of " + groupIndex, Strings.join(expected), Strings.join(scanner.strings()));
                // just in case
                assertEquals("scan of " + groupIndex, expected, scanner.strings());
            }
        }

        private GiCheckerImpl(StackTraceElement frame) {
            this.frame = frame;
            this.scratch = new StringBuilder();
            this.expectedStrings = new HashMap<GroupIndex, List<String>>();
        }

        private final StackTraceElement frame;
        private final Map<GroupIndex,List<String>> expectedStrings;
        private GroupIndex giToCheck;
        private final StringBuilder scratch;
    }

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
