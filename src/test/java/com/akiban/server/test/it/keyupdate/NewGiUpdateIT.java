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

    private GisChecker initC() {
        writeRow(c, 117L, "John");
        return checker().done();
    }

    @Test
    public void c_add_c() {
        GisChecker initState = initC();
        u();
    }

    @Test
    public void c_add_o() {
        GisChecker initState = initC();
        u();
    }

    @Test
    public void c_add_I() {
        GisChecker initState = initC();
        u();
    }

    @Test
    public void c_add_h() {
        GisChecker initState = initC();
        u();
    }

    @Test
    public void c_add_a() {
        GisChecker initState = initC();
        u();
    }

    @Test
    public void c_del_c() {
        GisChecker initState = initC();
        u();
    }

    private GisChecker initCC() {
        throw u();
    }

    @Test
    public void cC_add_o() {
        GisChecker initState = initCC();
        u();
    }

    @Test
    public void cC_del_c() {
        GisChecker initState = initCC();
        u();
    }

    private GisChecker initO() {
        throw u();
    }

    @Test
    public void o_add_c() {
        GisChecker initState = initO();
        u();
    }

    @Test
    public void o_add_o() {
        GisChecker initState = initO();
        u();
    }

    @Test
    public void o_add_I() {
        GisChecker initState = initO();
        u();
    }

    @Test
    public void o_add_h() {
        GisChecker initState = initO();
        u();
    }

    @Test
    public void o_add_a() {
        GisChecker initState = initO();
        u();
    }

    @Test
    public void o_del_o() {
        GisChecker initState = initO();
        u();
    }

    private GisChecker initOO() {
        throw u();
    }

    @Test
    public void oo_add_c() {
        GisChecker initState = initOO();
        u();
    }

    @Test
    public void oo_add_I() {
        GisChecker initState = initOO();
        u();
    }

    @Test
    public void oo_add_h() {
        GisChecker initState = initOO();
        u();
    }

    @Test
    public void oo_del_o() {
        GisChecker initState = initOO();
        u();
    }

    private GisChecker initI() {
        throw u();
    }

    @Test
    public void I_add_c() {
        GisChecker initState = initI();
        u();
    }

    @Test
    public void I_add_o() {
        GisChecker initState = initI();
        u();
    }

    @Test
    public void I_add_I() {
        GisChecker initState = initI();
        u();
    }

    @Test
    public void I_add_h() {
        GisChecker initState = initI();
        u();
    }

    @Test
    public void I_add_a() {
        GisChecker initState = initI();
        u();
    }

    @Test
    public void I_del_I() {
        GisChecker initState = initI();
        u();
    }

    private GisChecker initII() {
        throw u();
    }

    @Test
    public void ii_add_c() {
        GisChecker initState = initII();
        u();
    }

    @Test
    public void ii_add_o() {
        GisChecker initState = initII();
        u();
    }

    @Test
    public void ii_add_h() {
        GisChecker initState = initII();
        u();
    }

    @Test
    public void ii_del_I() {
        GisChecker initState = initII();
        u();
    }

    private GisChecker initH() {
        throw u();
    }

    @Test
    public void h_add_c() {
        GisChecker initState = initH();
        u();
    }

    @Test
    public void h_add_o() {
        GisChecker initState = initH();
        u();
    }

    @Test
    public void h_add_I() {
        GisChecker initState = initH();
        u();
    }

    @Test
    public void h_add_h() {
        GisChecker initState = initH();
        u();
    }

    @Test
    public void h_add_a() {
        GisChecker initState = initH();
        u();
    }

    private GisChecker initCO() {
        throw u();
    }

    @Test
    public void co_add_o() {
        GisChecker initState = initCO();
        u();
    }

    @Test
    public void co_add_I() {
        GisChecker initState = initCO();
        u();
    }

    @Test
    public void co_add_h() {
        GisChecker initState = initCO();
        u();
    }

    @Test
    public void co_del_c() {
        GisChecker initState = initCO();
        u();
    }

    @Test
    public void co_del_o() {
        GisChecker initState = initCO();
        u();
    }

    private GisChecker initCI() {
        throw u();
    }


    @Test
    public void ci_add_o() {
        GisChecker initState = initCI();
        u();
    }

    @Test
    public void ci_add_h() {
        GisChecker initState = initCI();
        u();
    }

    private GisChecker initOI() {
        throw u();
    }

    @Test
    public void oi_add_c() {
        GisChecker initState = initOI();
        u();
    }

    @Test
    public void oi_add_h() {
        GisChecker initState = initOI();
        u();
    }

    @Test
    public void oi_del_o() {
        GisChecker initState = initOI();
        u();
    }

    @Test
    public void oi_del_I() {
        GisChecker initState = initOI();
        u();
    }

    private GisChecker initIH() {
        throw u();
    }

    @Test
    public void ih_add_o() {
        GisChecker initState = initIH();
        u();
    }

    private GisChecker initCOI() {
        throw u();
    }

    @Test
    public void coi_add_h() {
        GisChecker initState = initCOI();
        u();
    }

    @Test
    public void coi_add_a() {
        GisChecker initState = initCOI();
        u();
    }

    private GisChecker initCOH() {
        throw u();
    }

    @Test
    public void coh_add_I() {
        GisChecker initState = initCOH();
        u();
    }

    private GisChecker initCOIH() {
        throw u();
    }

    @Test
    public void coih_add_h() {
        GisChecker initState = initCOIH();
        u();
    }

    @Test
    public void coih_add_a() {
        GisChecker initState = initCOIH();
        u();
    }

    @Test
    public void coih_del_c() {
        GisChecker initState = initCOIH();
        u();
    }

    @Test
    public void coih_del_o() {
        GisChecker initState = initCOIH();
        u();
    }

    @Test
    public void coih_del_I() {
        GisChecker initState = initCOIH();
        u();
    }

    @Test
    public void coih_del_h() {
        GisChecker initState = initCOIH();
        u();
    }

    @Test
    public void coihOIH_move_c() {
        u();
    }

    @Test
    public void coihCIH_move_o() {
        u();
    }

    @Test
    public void coihCOH_move_I() {
        u();
    }

    @Test
    public void coihCOI_move_h() {
        u();
    }


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

        GisCheckBuilder emptyCheckBuilder = checker();
        for (GroupIndex gi : group().getIndexes()) {
            emptyCheckBuilder.gi(gi.getIndexName().getName());
        }
        emptyCheckBuilder.done();

        c = null;
        o = null;
        i = null;
        h = null;
        a = null;
        assertEquals(Collections.<GisCheckBuilder>emptySet(), unfinishedCheckBuilders);
    }

    private GisCheckBuilder checker() {
        StackTraceElement callerFrame = Thread.currentThread().getStackTrace()[2];
        GisCheckBuilder checkBuilder = new GiCheckBuilderImpl(callerFrame);
        boolean added = unfinishedCheckBuilders.add(checkBuilder);
        assert added : unfinishedCheckBuilders;
        return checkBuilder;
    }

    private Group group() {
        return getUserTable(c).getGroup();
    }

    private Integer c;
    private Integer o;
    private Integer i;
    private Integer h;
    private Integer a;
    private final Set<GisCheckBuilder> unfinishedCheckBuilders = new HashSet<GisCheckBuilder>();

    private static final String SCHEMA = "coia";

    // nested classes

    private interface GisChecker {
        public void check();
    }

    private interface GisCheckBuilder {
        GiCheckBuilder gi(String giName);
        public GisChecker done();
    }

    private interface GiCheckBuilder extends GisCheckBuilder {
        public GiTablesChecker entry(String key);
    }

    private interface GiTablesChecker {
        GiCheckBuilder backedBy(int firstTableId, int... tableIds);
    }

    private class GiCheckBuilderImpl implements GiCheckBuilder, GiTablesChecker {

        @Override
        public GiCheckBuilder gi(String giName) {
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
        public GiCheckBuilder backedBy(int firstTableId, int... tableIds) {
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

        public GisChecker done() {
            unfinishedCheckBuilders.remove(this);
            GisChecker checker = new GisCheckerImpl(expectedStrings);
            checker.check();
            return checker;
        }

        @Override
        public String toString() {
            return String.format("%s at line %d", frame.getMethodName(), frame.getLineNumber());
        }

        private GiCheckBuilderImpl(StackTraceElement frame) {
            this.frame = frame;
            this.scratch = new StringBuilder();
            this.expectedStrings = new HashMap<GroupIndex, List<String>>();
        }

        private final StackTraceElement frame;
        private final Map<GroupIndex,List<String>> expectedStrings;
        private GroupIndex giToCheck;
        private final StringBuilder scratch;
    }

    private class GisCheckerImpl implements GisChecker {

        @Override
        public void check() {
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

        private GisCheckerImpl(Map<GroupIndex, List<String>> expectedStrings) {
            this.expectedStrings = new HashMap<GroupIndex, List<String>>(expectedStrings);
        }

        private final Map<GroupIndex,List<String>> expectedStrings;
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

    private static Error u() {
        throw new UnsupportedOperationException();
    }
}
