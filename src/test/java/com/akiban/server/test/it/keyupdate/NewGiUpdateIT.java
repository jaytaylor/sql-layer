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

    private void initC() {
        u();
    }

    @Test public void c_add_c() { initC(); u(); }
    @Test public void c_add_o() { initC(); u(); }
    @Test public void c_add_I() { initC(); u(); }
    @Test public void c_add_h() { initC(); u(); }
    @Test public void c_add_a() { initC(); u(); }
    @Test public void c_del_c() { initC(); u(); }

    private void initCC() {
        u();
    }

    @Test public void cC_add_o() { initCC(); u(); }
    @Test public void cC_del_c() { initCC(); u(); }

    private void initO() {
        u();
    }

    @Test public void o_add_c() { initO(); u(); }
    @Test public void o_add_o() { initO(); u(); }
    @Test public void o_add_I() { initO(); u(); }
    @Test public void o_add_h() { initO(); u(); }
    @Test public void o_add_a() { initO(); u(); }
    @Test public void o_del_o() { initO(); u(); }

    private void initOO() {
        u();
    }

    @Test public void oo_add_c() { initOO(); u(); }
    @Test public void oo_add_I() { initOO(); u(); }
    @Test public void oo_add_h() { initOO(); u(); }
    @Test public void oo_del_o() { initOO(); u(); }

    private void initI() {
        u();
    }

    @Test public void I_add_c() { initI(); u(); }
    @Test public void I_add_o() { initI(); u(); }
    @Test public void I_add_I() { initI(); u(); }
    @Test public void I_add_h() { initI(); u(); }
    @Test public void I_add_a() { initI(); u(); }
    @Test public void I_del_I() { initI(); u(); }

    private void initII() {
        u();
    }

    @Test public void ii_add_c() { initII(); u(); }
    @Test public void ii_add_o() { initII(); u(); }
    @Test public void ii_add_h() { initII(); u(); }
    @Test public void ii_del_I() { initII(); u(); }

    private void initH() {
        u();
    }

    @Test public void h_add_c() { initH(); u(); }
    @Test public void h_add_o() { initH(); u(); }
    @Test public void h_add_I() { initH(); u(); }
    @Test public void h_add_h() { initH(); u(); }
    @Test public void h_add_a() { initH(); u(); }

    private void initCO() {
        u();
    }

    @Test public void co_add_o() { initCO(); u(); }
    @Test public void co_add_I() { initCO(); u(); }
    @Test public void co_add_h() { initCO(); u(); }
    @Test public void co_del_c() { initCO(); u(); }
    @Test public void co_del_o() { initCO(); u(); }

    private void initCI() {
        u();
    }


    @Test public void ci_add_o() { initCI(); u(); }
    @Test public void ci_add_h() { initCI(); u(); }

    private void initOI() {
        u();
    }

    @Test public void oi_add_c() { initOI(); u(); }
    @Test public void oi_add_h() { initOI(); u(); }
    @Test public void oi_del_o() { initOI(); u(); }
    @Test public void oi_del_I() { initOI(); u(); }

    private void initIH() {
        u();
    }

    @Test public void ih_add_o() { initIH(); u(); }

    private void initCOI() {
        u();
    }

    @Test public void coi_add_h() { initCOI(); u(); }
    @Test public void coi_add_a() { initCOI(); u(); }

    private void initCOH() {
        u();
    }

    @Test public void coh_add_I() { initCOH(); u(); }

    private void initCOIH() {
        u();
    }

    @Test public void coih_add_h() { initCOIH(); u(); }
    @Test public void coih_add_a() { initCOIH(); u(); }
    @Test public void coih_del_c() { initCOIH(); u(); }
    @Test public void coih_del_o() { initCOIH(); u(); }
    @Test public void coih_del_I() { initCOIH(); u(); }
    @Test public void coih_del_h() { initCOIH(); u(); }

    @Test public void coihOIH_move_c() { u(); }
    @Test public void coihCIH_move_o() { u(); }
    @Test public void coihCOH_move_I() { u(); }
    @Test public void coihCOI_move_h() { u(); }


    @Before
    public final void createTables() {
        c = createTable(SCHEMA, "c", "cid int key, name varchar(32)");
        o = createTable(SCHEMA, "o", "oid int key, c_id int, when varchar(32)", akibanFK("c_id", "c", "cid") );
        i = createTable(SCHEMA, "i", "iid int key, o_id int, sku int", akibanFK("o_id", "o", "oid") );
        h = createTable(SCHEMA, "h", "sid int key, i_id int, handling_instructions varchar(32)", akibanFK("i_id", "i", "iid") );
        a = createTable(SCHEMA, "a", "oid int key, c_id int, street varchar(56)", akibanFK("c_id", "c", "cid") );

        String groupName = group().getName();

        createGroupIndex(groupName, "co_left", "c.name,o.when", Index.JoinType.LEFT);
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
        return getUserTable(c).getGroup();
    }

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

    private static void u() {
        throw new UnsupportedOperationException();
    }
}
