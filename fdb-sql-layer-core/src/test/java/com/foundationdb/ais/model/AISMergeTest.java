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

package com.foundationdb.ais.model;

import com.foundationdb.ais.AISCloner;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.MultipleIdentityColumnsException;
import com.foundationdb.server.store.format.DummyStorageFormatRegistry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class AISMergeTest {

    private AkibanInformationSchema t;
    private AkibanInformationSchema s;
    private TestAISBuilder b;
    private static final String SCHEMA= "test";
    private static final String TABLE  = "t1";
    private static final TableName TABLENAME = new TableName(SCHEMA,TABLE);
    private AISCloner aisCloner;
    
    @Before
    public void createSchema() throws Exception {
        t = new AkibanInformationSchema();
        s = new AkibanInformationSchema();
        aisCloner = DummyStorageFormatRegistry.aisCloner();
        b = new TestAISBuilder(s, aisCloner.getTypesRegistry());
    }

    @Test
    public void simpleColumnTest () throws Exception {
        b.table(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "MCOMPAT", "INT", false);
        b.column(SCHEMA, TABLE, "c2", 1, "MCOMPAT", "INT", false);
        
        b.basicSchemaIsComplete();
        b.groupingIsComplete();
        
        assertNotNull(s.getTable(SCHEMA, TABLE));
        assertNotNull(s.getTable(SCHEMA, TABLE).getAIS());
        
        AISMerge merge = new AISMerge (aisCloner, t, s.getTable(TABLENAME));
        t = merge.merge().getAIS();
        Table targetTable = t.getTable(TABLENAME);
        Table sourceTable = s.getTable(TABLENAME);

        assertTrue (t.isFrozen());
        assertNotSame (targetTable, sourceTable);
        assertEquals (targetTable.getName(), sourceTable.getName());
        checkColumns (targetTable.getColumns(), "c1", "c2");
        checkColumns (targetTable.getColumnsIncludingInternal(), "c1", "c2", Column.ROW_ID_NAME);

        // hidden primary key gets moved/re-created
        assertEquals (targetTable.getIndexes().size() , sourceTable.getIndexes().size());
        // hidden primary key is not a user visible index. 
        assertEquals (0,targetTable.getIndexes().size());
        assertNull   (targetTable.getPrimaryKey());
        assertNotNull (targetTable.getPrimaryKeyIncludingInternal());
        assertEquals (targetTable.getColumn(Column.ROW_ID_NAME).getPosition(), 
                sourceTable.getColumn(Column.ROW_ID_NAME).getPosition());
        
        // merge will have created a group table for the user table we merged.
        assertEquals (t.getGroups().keySet().size(), 1);
        assertNotNull(t.getGroup(TABLE));
    }

    @Test
    public void simpleIndexTest() throws Exception {
        b.table(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "MCOMPAT", "INT", false);
        b.pk(SCHEMA, TABLE);
        b.indexColumn(SCHEMA, TABLE, Index.PRIMARY, "c1", 0, true, null);
        b.basicSchemaIsComplete();
        AISMerge merge = new AISMerge (aisCloner, t, s.getTable(TABLENAME));
        t = merge.merge().getAIS();
        
        Table targetTable = t.getTable(TABLENAME);
        Table sourceTable = s.getTable(TABLENAME);
        
        assertEquals(targetTable.getIndexes().size(), 1);
        assertEquals(targetTable.getIndexes().size(), sourceTable.getIndexes().size());
        assertNotNull (targetTable.getPrimaryKey());
        checkColumns(targetTable.getPrimaryKey().getColumns(), "c1");
        
        assertNotNull(t.getGroup(TABLE));
    }
    
    @Test
    public void uniqueIndexTest() throws Exception {
        b.table(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "MCOMPAT", "int", false);
        b.unique(SCHEMA, TABLE, "c1");
        b.indexColumn(SCHEMA, TABLE, "c1", "c1", 0, true, null);
        
        AISMerge merge = new AISMerge (aisCloner, t, s.getTable(TABLENAME));
        t = merge.merge().getAIS();
        
        Table targetTable = t.getTable(TABLENAME);
        Table sourceTable = s.getTable(TABLENAME);
        
        assertTrue (t.isFrozen());
        assertEquals (1,targetTable.getIndexes().size());
        assertEquals (targetTable.getIndexes().size(), sourceTable.getIndexes().size());
        assertNotNull (targetTable.getIndex("c1"));
        checkIndexColumns (targetTable.getIndex("c1").getKeyColumns(), "c1");
        assertNotNull(t.getGroup(TABLE));
        assertNull (targetTable.getPrimaryKey());

    }
    
    @Test
    public void testSimpleJoin() throws Exception {
        b.table(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "MCOMPAT", "INT", false);
        b.column(SCHEMA, TABLE, "c2", 1, "MCOMPAT", "INT", false);
        b.pk(SCHEMA, TABLE);
        b.indexColumn(SCHEMA, TABLE, Index.PRIMARY, "c1", 0, true, null);
        b.basicSchemaIsComplete();
        b.createGroup("FRED", SCHEMA);
        b.addTableToGroup("FRED", SCHEMA, TABLE);
        b.groupingIsComplete();
        AISMerge merge = new AISMerge (aisCloner, t, s.getTable(TABLENAME));
        t = merge.merge().getAIS();
        assertTrue (t.isFrozen());
        assertEquals (TABLENAME, t.getTable(TABLENAME).getGroup().getName());

        b.table(SCHEMA, "t2");
        b.column(SCHEMA, "t2", "c1", 0, "MCOMPAT", "INT", false);
        b.column(SCHEMA, "t2", "c2", 1, "MCOMPAT", "INT", true);
        b.joinTables("test/t1/test/t2", SCHEMA, TABLE, SCHEMA, "t2");
        b.joinColumns("test/t1/test/t2", SCHEMA, TABLE, "c1", SCHEMA, "t2", "c1");
        b.basicSchemaIsComplete();
        b.addJoinToGroup("FRED", "test/t1/test/t2", 0);
        b.groupingIsComplete();
        
        merge = new AISMerge (aisCloner, t, s.getTable(SCHEMA, "t2"));
        t = merge.merge().getAIS();
        
        assertEquals (1, t.getJoins().size());
        assertNotNull (t.getTable(SCHEMA, "t2").getParentJoin());
        assertEquals (1, t.getTable(TABLENAME).getChildJoins().size());
        assertNotNull (t.getGroup(TABLE));
        assertEquals (TABLENAME, t.getTable(SCHEMA, "t2").getGroup().getName());
    }
    
    @Test
    public void testTwoJoins() throws Exception {
        b.table(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "MCOMPAT", "INT", false);
        b.column(SCHEMA, TABLE, "c2", 1, "MCOMPAT", "INT", false);
        b.pk(SCHEMA, TABLE);
        b.indexColumn(SCHEMA, TABLE, Index.PRIMARY, "c1", 0, true, null);
        b.basicSchemaIsComplete();
        b.createGroup("FRED", SCHEMA);
        b.addTableToGroup("FRED", SCHEMA, TABLE);
        b.groupingIsComplete();
        AISMerge merge = new AISMerge (aisCloner, t, s.getTable(TABLENAME));
        t = merge.merge().getAIS();
        assertTrue (t.isFrozen());

        b.table(SCHEMA, "t2");
        b.column(SCHEMA, "t2", "c1", 0, "MCOMPAT", "INT", false);
        b.column(SCHEMA, "t2", "c2", 1, "MCOMPAT", "INT", true);
        b.joinTables("test/t1/test/t2", SCHEMA, TABLE, SCHEMA, "t2");
        b.joinColumns("test/t1/test/t2", SCHEMA, TABLE, "c1", SCHEMA, "t2", "c1");
        b.basicSchemaIsComplete();
        b.addJoinToGroup("FRED", "test/t1/test/t2", 0);
        b.groupingIsComplete();
        
        merge = new AISMerge (aisCloner, t, s.getTable(SCHEMA, "t2"));
        t = merge.merge().getAIS();
        assertNotNull (t.getTable(SCHEMA, "t2").getParentJoin());
        assertEquals (1, t.getTable(TABLENAME).getChildJoins().size());
        assertNotNull (t.getGroup(TABLE));

        b.table(SCHEMA, "t3");
        b.column(SCHEMA, "t3", "c1", 0, "MCOMPAT", "INT", false);
        b.column(SCHEMA, "t3", "c2", 1, "MCOMPAT", "INT", false);
        b.joinTables("test/t1/test/t3", SCHEMA, TABLE, SCHEMA, "t3");
        b.joinColumns("test/t1/test/t3", SCHEMA, TABLE, "c1", SCHEMA, "t3", "c1");
        b.basicSchemaIsComplete();
        b.addJoinToGroup("FRED", "test/t1/test/t3", 0);
        b.groupingIsComplete();
        
        merge = new AISMerge (aisCloner, t, s.getTable(SCHEMA, "t3"));
        t = merge.merge().getAIS();
        assertNotNull (t.getTable(SCHEMA, "t3").getParentJoin());
        assertEquals (2, t.getTable(TABLENAME).getChildJoins().size());
        assertNotNull (t.getGroup(TABLE));
        assertEquals (TABLENAME, t.getTable(SCHEMA, "t3").getGroup().getName());
    }

    
    @Test (expected=InvalidOperationException.class)
    public void testJoinToBadParent() throws Exception
    {
        // Table 1
        b.table(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "MCOMPAT", "INT", false);
        b.column(SCHEMA, TABLE, "c2", 1, "MCOMPAT", "INT", false);
        b.pk(SCHEMA, TABLE);
        b.indexColumn(SCHEMA, TABLE, Index.PRIMARY, "c1", 0, true, null);
        b.basicSchemaIsComplete();
        b.createGroup("FRED", SCHEMA);
        b.addTableToGroup("FRED", SCHEMA, TABLE);
        b.groupingIsComplete();
        AISMerge merge = new AISMerge (aisCloner, t, s.getTable(TABLENAME));
        t = merge.merge().getAIS();
        assertTrue (t.isFrozen());
        
        // table 3 : the fake table
        b.table(SCHEMA, "t3");
        b.column(SCHEMA, "t3", "c1", 0, "MCOMPAT", "int", false);
        b.pk(SCHEMA, "t3");
        b.indexColumn(SCHEMA, "t3", Index.PRIMARY, "c1", 0, true, null);
        b.createGroup("DOUG", SCHEMA);
        b.addTableToGroup("DOUG", SCHEMA, "t3");
        // table 2 : join to wrong table. 
        b.table(SCHEMA, "t2");
        b.column(SCHEMA, "t2", "c1", 0, "MCOMPAT", "INT", false);
        b.column(SCHEMA, "t2", "c2", 1, "MCOMPAT", "INT", true);
        b.joinTables("test/t1/test/t2", SCHEMA, "t3", SCHEMA, "t2");
        b.joinColumns("test/t1/test/t2", SCHEMA, "t3", "c1", SCHEMA, "t2", "c1");
        b.basicSchemaIsComplete();
        b.addJoinToGroup("DOUG", "test/t1/test/t2", 0);
        b.groupingIsComplete();
        
        merge = new AISMerge (aisCloner, t, s.getTable(SCHEMA, "t2"));
        t = merge.merge().getAIS();
    }

    @Test (expected=InvalidOperationException.class)
    public void testBadParentJoin () throws Exception
    {
        // Table 1
        b.table(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "MCOMPAT", "INT", false);
        b.column(SCHEMA, TABLE, "c2", 1, "MCOMPAT", "INT", false);
        b.pk(SCHEMA, TABLE);
        b.indexColumn(SCHEMA, TABLE, Index.PRIMARY, "c1", 0, true, null);
        b.basicSchemaIsComplete();
        b.createGroup("FRED", SCHEMA);
        b.addTableToGroup("FRED", SCHEMA, TABLE);
        b.groupingIsComplete();
        AISMerge merge = new AISMerge (aisCloner, t, s.getTable(TABLENAME));
        t = merge.merge().getAIS();
        assertTrue (t.isFrozen());
        
        
        b = new TestAISBuilder(aisCloner.getTypesRegistry());
        // table 3 : the fake table
        b.table(SCHEMA, "t1");
        b.column(SCHEMA, "t1", "c5", 0, "MCOMPAT", "int", false);
        b.pk(SCHEMA, "t1");
        b.indexColumn(SCHEMA, "t1", Index.PRIMARY, "c5", 0, true, null);
        b.createGroup("DOUG", SCHEMA);
        b.addTableToGroup("DOUG", SCHEMA, "t1");
        // table 2 : join to wrong table. 
        b.table(SCHEMA, "t2");
        b.column(SCHEMA, "t2", "c1", 0, "MCOMPAT", "INT", false);
        b.column(SCHEMA, "t2", "c2", 1, "MCOMPAT", "INT", true);
        b.joinTables("test/t1/test/t2", SCHEMA, "t1", SCHEMA, "t2");
        b.joinColumns("test/t1/test/t2", SCHEMA, "t1", "c5", SCHEMA, "t2", "c1");
        b.basicSchemaIsComplete();
        b.addJoinToGroup("DOUG", "test/t1/test/t2", 0);
        b.groupingIsComplete();
        
        merge = new AISMerge (aisCloner, t, b.akibanInformationSchema().getTable(SCHEMA, "t2"));
        t = merge.merge().getAIS();
        
    }

    @Test
    public void testFakeTableJoin() throws Exception
    {
        // Table 1
        b.table(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "MCOMPAT", "INT", false);
        b.column(SCHEMA, TABLE, "c2", 1, "MCOMPAT", "INT", false);
        b.pk(SCHEMA, TABLE);
        b.indexColumn(SCHEMA, TABLE, Index.PRIMARY, "c1", 0, true, null);
        b.basicSchemaIsComplete();
        b.createGroup("FRED", SCHEMA);
        b.addTableToGroup("FRED", SCHEMA, TABLE);
        b.groupingIsComplete();
        AISMerge merge = new AISMerge (aisCloner, t, s.getTable(TABLENAME));
        t = merge.merge().getAIS();
        assertTrue (t.isFrozen());
        
        b = new TestAISBuilder(aisCloner.getTypesRegistry());
        // table 3 : the fake table
        b.table(SCHEMA, "t1");
        b.column(SCHEMA, "t1", "c1", 0, "MCOMPAT", "int", false);
        b.pk(SCHEMA, "t1");
        b.indexColumn(SCHEMA, "t1", Index.PRIMARY, "c1", 0, true, null);
        b.createGroup("DOUG", SCHEMA);
        b.addTableToGroup("DOUG", SCHEMA, "t1");
        // table 2 : join to wrong table. 
        b.table(SCHEMA, "t2");
        b.column(SCHEMA, "t2", "c1", 0, "MCOMPAT", "INT", false);
        b.column(SCHEMA, "t2", "c2", 1, "MCOMPAT", "INT", true);
        b.joinTables("test/t1/test/t2", SCHEMA, "t1", SCHEMA, "t2");
        b.joinColumns("test/t1/test/t2", SCHEMA, "t1", "c1", SCHEMA, "t2", "c1");
        b.basicSchemaIsComplete();
        b.addJoinToGroup("DOUG", "test/t1/test/t2", 0);
        b.groupingIsComplete();
        
        merge = new AISMerge (aisCloner, t, b.akibanInformationSchema().getTable(SCHEMA, "t2"));
        t = merge.merge().getAIS();

        assertNotNull (t.getTable(SCHEMA, "t2"));
        assertNotNull (t.getTable(SCHEMA, "t2").getParentJoin());
        assertEquals (1, t.getTable(TABLENAME).getChildJoins().size());
        assertNotNull (t.getGroup(TABLE));
        assertEquals (TABLENAME, t.getTable(SCHEMA, "t2").getGroup().getName());
        assertNotNull (t.getTable(SCHEMA, TABLE));
        assertEquals (TABLENAME, t.getTable(SCHEMA, TABLE).getGroup().getName());

    }

    @Test
    public void joinOfDifferingIntTypes() throws Exception {
        b.table(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "MCOMPAT", "BIGINT", false);
        b.pk(SCHEMA, TABLE);
        b.indexColumn(SCHEMA, TABLE, Index.PRIMARY, "c1", 0, true, null);
        b.basicSchemaIsComplete();
        b.createGroup(TABLE, SCHEMA);
        b.addTableToGroup(TABLE, SCHEMA, TABLE);
        b.groupingIsComplete();
        
        AISMerge merge = new AISMerge(aisCloner, t, s.getTable(TABLENAME));
        t = merge.merge().getAIS();
        assertTrue(t.isFrozen());
        assertEquals(TABLENAME, t.getTable(TABLENAME).getGroup().getName());

        b.table(SCHEMA, "t2");
        b.column(SCHEMA, "t2", "c1", 0, "MCOMPAT", "INT", false);
        b.column(SCHEMA, "t2", "parentid", 1, "MCOMPAT", "INT", false);

        // join bigint->int
        b.joinTables("test/t1/test/t2", SCHEMA, TABLE, SCHEMA, "t2");
        b.joinColumns("test/t1/test/t2", SCHEMA, TABLE, "c1", SCHEMA, "t2", "parentid");
        b.basicSchemaIsComplete();
        b.addJoinToGroup(TABLE, "test/t1/test/t2", 0);
        b.groupingIsComplete();

        merge = new AISMerge(aisCloner, t, s.getTable(SCHEMA, "t2"));
        t = merge.merge().getAIS();

        assertEquals(1, t.getJoins().size());
        assertNotNull(t.getTable(SCHEMA, "t2").getParentJoin());
        assertEquals(1, t.getTable(TABLENAME).getChildJoins().size());
        assertNotNull(t.getGroup(TABLE));
        assertEquals(TABLENAME, t.getTable(SCHEMA, "t2").getGroup().getName());
    }

    @Test(expected= InvalidOperationException.class)
    public void joinDifferentTypes() throws Exception {
        b.table(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "MCOMPAT", "BIGINT", false);
        b.pk(SCHEMA, TABLE);
        b.indexColumn(SCHEMA, TABLE, Index.PRIMARY, "c1", 0, true, null);
        b.basicSchemaIsComplete();
        b.createGroup(TABLE, SCHEMA);
        b.addTableToGroup(TABLE, SCHEMA, TABLE);
        b.groupingIsComplete();

        AISMerge merge = new AISMerge(aisCloner, t, s.getTable(TABLENAME));
        t = merge.merge().getAIS();
        assertTrue(t.isFrozen());
        assertEquals(TABLENAME, t.getTable(TABLENAME).getGroup().getName());

        b.table(SCHEMA, "t2");
        b.column(SCHEMA, "t2", "c1", 0, "MCOMPAT", "INT", false);
        b.column(SCHEMA, "t2", "parentid", 1, "MCOMPAT", "varchar", 32L, null, false);

        // join bigint->varchar
        b.joinTables("test/t1/test/t2", SCHEMA, TABLE, SCHEMA, "t2");
        b.joinColumns("test/t1/test/t2", SCHEMA, TABLE, "c1", SCHEMA, "t2", "parentid");
        b.basicSchemaIsComplete();
        b.addJoinToGroup(TABLE, "test/t1/test/t2", 0);
        b.groupingIsComplete();

        merge = new AISMerge(aisCloner, t, s.getTable(SCHEMA, "t2"));
        t = merge.merge().getAIS();
    }
    
    @Test(expected= MultipleIdentityColumnsException.class)
    public void columnIdentityNoPK () {
        b.table(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "MCOMPAT", "INT", false);
        b.column(SCHEMA, TABLE, "c2", 1, "MCOMPAT", "INT", false);
        b.sequence(SCHEMA, "seq-1", 5, 2, 0, 1000, false);
        b.columnAsIdentity(SCHEMA, TABLE, "c1", "seq-1", true);
        b.basicSchemaIsComplete();

        b.createGroup("FRED", SCHEMA);
        b.addTableToGroup("FRED", SCHEMA, TABLE);
        b.groupingIsComplete();
        AISMerge merge = new AISMerge (aisCloner, t, s.getTable(TABLENAME));
        t = merge.merge().getAIS();
    }

    @Test
    public void columnIdentityToPK () {
        b.table(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "MCOMPAT", "INT", false);
        b.index(SCHEMA, TABLE, Index.PRIMARY, true, true, TableName.create(SCHEMA, Index.PRIMARY));
        b.indexColumn(SCHEMA, TABLE, Index.PRIMARY, "c1", 0, true, null);
        b.column(SCHEMA, TABLE, "c2", 1, "MCOMPAT", "INT", false);
        b.sequence(SCHEMA, "seq-1", 5, 2, 0, 1000, false);
        b.columnAsIdentity(SCHEMA, TABLE, "c1", "seq-1", true);
        b.basicSchemaIsComplete();

        b.createGroup("FRED", SCHEMA);
        b.addTableToGroup("FRED", SCHEMA, TABLE);
        b.groupingIsComplete();
        AISMerge merge = new AISMerge (aisCloner, t, s.getTable(TABLENAME));
        t = merge.merge().getAIS();
        
        assertNotNull (t.getTable(TABLENAME).getColumn(0).getIdentityGenerator());
        Sequence identityGenerator = t.getTable(TABLENAME).getColumn(0).getIdentityGenerator();
        assertEquals (5, identityGenerator.getStartsWith());
        assertEquals (2, identityGenerator.getIncrement());
        assertEquals (1000, identityGenerator.getMaxValue());
        assertNotNull (identityGenerator.getStorageUniqueKey());
    }

    @Test
    public void columnIdentityToNotPK () {
        b.table(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "MCOMPAT", "INT", false);
        b.index(SCHEMA, TABLE, Index.PRIMARY, true, true, TableName.create(SCHEMA, Index.PRIMARY));
        b.indexColumn(SCHEMA, TABLE, Index.PRIMARY, "c1", 0, true, null);
        b.column(SCHEMA, TABLE, "c2", 1, "MCOMPAT", "INT", false);
        b.sequence(SCHEMA, "seq-1", 5, 2, 0, 1000, false);
        b.columnAsIdentity(SCHEMA, TABLE, "c2", "seq-1", true);
        b.basicSchemaIsComplete();

        b.createGroup("FRED", SCHEMA);
        b.addTableToGroup("FRED", SCHEMA, TABLE);
        b.groupingIsComplete();
        AISMerge merge = new AISMerge (aisCloner, t, s.getTable(TABLENAME));
        t = merge.merge().getAIS();
        
        assertNotNull (t.getTable(TABLENAME).getColumn(1).getIdentityGenerator());
        Sequence identityGenerator = t.getTable(TABLENAME).getColumn(1).getIdentityGenerator();
        assertEquals (5, identityGenerator.getStartsWith());
        assertEquals (2, identityGenerator.getIncrement());
        assertEquals (1000, identityGenerator.getMaxValue());
        assertNotNull (identityGenerator.getStorageUniqueKey());
    }

    
    /*
     * Not really behavior that needs preserved, but at least one data set observed
     * to contain IDs outside of what we assume the valid Table range to be.
     */
    @Test
    public void tableIDUniqueIncludingIS() {
        /*
         * Set up this scenario:
         * table id -> table_name
         * x   -> test.bar
         * x+1 -> test._akiban_bar
         *   (x+2 = invalid next user table id)
         * x+2 -> i_s.foo
         * x+3 -> i_s._akiban_foo
         */
        TestAISBuilder tb = new TestAISBuilder(t, aisCloner.getTypesRegistry());

        tb.table(SCHEMA, "bar");
        tb.column(SCHEMA, "bar", "id", 0, "MCOMPAT", "INT", false);
        tb.createGroup("bar", SCHEMA);
        tb.addTableToGroup("bar", SCHEMA, "bar");

        tb.table(TableName.INFORMATION_SCHEMA, "foo");
        tb.column(TableName.INFORMATION_SCHEMA, "foo", "id", 0, "MCOMPAT", "INT", false);
        tb.createGroup("foo", TableName.INFORMATION_SCHEMA);
        tb.addTableToGroup("foo", TableName.INFORMATION_SCHEMA, "foo");

        tb.basicSchemaIsComplete();
        tb.groupingIsComplete();
        t.freeze();

        assertNotNull("bar table", tb.akibanInformationSchema().getTable(SCHEMA, "bar"));
        assertNotNull("foo table", t.getTable(TableName.INFORMATION_SCHEMA, "foo"));

        b.table(SCHEMA, "zap");
        b.column(SCHEMA, "zap", "id", 0, "MCOMPAT", "INT", false);
        
        AISMerge merge = new AISMerge(aisCloner, t, s.getTable(SCHEMA, "zap"));
        t = merge.merge().getAIS();
    }

    @Test
    public void groupTableIDUniqueIncludingIS() {
        /*
         * Set up this scenario:
         * table id -> table_name
         * x   -> i_s.foo
         * x+1 -> i_s._akiban_foo
         *    (x+2 = valid next i_s table ID)
         *    (x+3 = invalid next i_s group table id)
         * x+3 -> test.bar
         * x+4 -> test._akiban_bar
         */
        final String I_S = TableName.INFORMATION_SCHEMA;
        TestAISBuilder tb = new TestAISBuilder(t, aisCloner.getTypesRegistry());

        tb.table(I_S, "foo");
        tb.column(I_S, "foo", "id", 0, "MCOMPAT", "INT", false);
        tb.createGroup("foo", I_S);
        tb.addTableToGroup("foo", I_S, "foo");

        tb.table(SCHEMA, "bar");
        tb.column(SCHEMA, "bar", "id", 0, "MCOMPAT", "INT", false);
        tb.createGroup("bar", SCHEMA);
        tb.addTableToGroup("bar", SCHEMA, "bar");

        tb.basicSchemaIsComplete();
        tb.groupingIsComplete();
        t.freeze();

        assertNotNull("bar table", tb.akibanInformationSchema().getTable(SCHEMA, "bar"));
        assertNotNull("foo table", t.getTable(I_S, "foo"));

        b.table(I_S, "zap");
        b.column(I_S, "zap", "id", 0, "MCOMPAT", "INT", false);

        AISMerge merge = new AISMerge(aisCloner, t, s.getTable(I_S, "zap"));
        t = merge.merge().getAIS();
    }

    private void checkColumns(List<Column> actual, String ... expected)
    {
        assertEquals(expected.length, actual.size());
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual.get(i).getName());
        }
    }
    
    private void checkIndexColumns(List<IndexColumn> actual, String ... expected)
    {
        assertEquals (expected.length, actual.size());
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual.get(i).getColumn().getName());
        }
    }
}
