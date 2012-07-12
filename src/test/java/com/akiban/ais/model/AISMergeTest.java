/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.ais.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.akiban.server.error.InvalidOperationException;

public class AISMergeTest {

    private AkibanInformationSchema t;
    private AkibanInformationSchema s;
    private AISBuilder b;
    private static final String SCHEMA= "test";
    private static final String TABLE  = "t1";
    private static final TableName TABLENAME = new TableName(SCHEMA,TABLE);
    
    @Before
    public void createSchema() throws Exception {
        t = new AkibanInformationSchema();
        s = new AkibanInformationSchema();
        b = new AISBuilder(s);
    }

    @Test
    public void simpleColumnTest () throws Exception {
        b.userTable(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "INT", (long)0, (long)0, false, false, null, null);
        b.column(SCHEMA, TABLE, "c2", 1, "INT", (long)0, (long)0, false, false, null, null);
        
        b.basicSchemaIsComplete();
        
        assertNotNull(s.getUserTable(SCHEMA, TABLE));
        assertNotNull(s.getUserTable(SCHEMA, TABLE).getAIS());
        
        AISMerge merge = new AISMerge (t, s.getUserTable(TABLENAME));
        t = merge.merge().getAIS();
        UserTable targetTable = t.getUserTable(TABLENAME);
        UserTable sourceTable = s.getUserTable(TABLENAME);

        assertTrue (t.isFrozen());
        assertNotSame (targetTable, sourceTable);
        assertEquals (targetTable.getName(), sourceTable.getName());
        checkColumns (targetTable.getColumns(), "c1", "c2");
        checkColumns (targetTable.getColumnsIncludingInternal(), "c1", "c2", Column.AKIBAN_PK_NAME);
        assertEquals (targetTable.getEngine(), sourceTable.getEngine());
        
        // hidden primary key gets moved/re-created
        assertEquals (targetTable.getIndexes().size() , sourceTable.getIndexes().size());
        // hidden primary key is not a user visible index. 
        assertEquals (0,targetTable.getIndexes().size());
        assertNull   (targetTable.getPrimaryKey());
        assertNotNull (targetTable.getPrimaryKeyIncludingInternal());
        assertEquals (targetTable.getColumn(Column.AKIBAN_PK_NAME).getPosition(), 
                sourceTable.getColumn(Column.AKIBAN_PK_NAME).getPosition());
        
        // merge will have created a group table for the user table we merged.
        assertEquals (t.getGroups().keySet().size(), 1);
        assertNotNull(t.getGroup(TABLE));
        checkColumns (t.getGroup(TABLE).getGroupTable().getColumns(), "t1$c1", "t1$c2", "t1$"+Column.AKIBAN_PK_NAME);
    }

    @Test
    public void simpleIndexTest() throws Exception {
        b.userTable(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "INT", (long)0, (long)0, false, false, null, null);
        b.index(SCHEMA, TABLE, "PRIMARY", true, Index.PRIMARY_KEY_CONSTRAINT);
        b.indexColumn(SCHEMA, TABLE, "PRIMARY", "c1", 0, true, null);
        b.basicSchemaIsComplete();
        AISMerge merge = new AISMerge (t,s.getUserTable(TABLENAME));
        t = merge.merge().getAIS();
        
        UserTable targetTable = t.getUserTable(TABLENAME);
        UserTable sourceTable = s.getUserTable(TABLENAME);
        
        assertEquals(targetTable.getIndexes().size(), 1);
        assertEquals(targetTable.getIndexes().size(), sourceTable.getIndexes().size());
        assertNotNull (targetTable.getPrimaryKey());
        checkColumns(targetTable.getPrimaryKey().getColumns(), "c1");
        
        assertNotNull(t.getGroup(TABLE));
        assertNotNull(t.getGroup(TABLE).getGroupTable().getIndex("t1$PRIMARY"));
    }
    
    @Test
    public void uniqueIndexTest() throws Exception {
        b.userTable(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "int", (long)0, (long)0, false, false, null, null);
        b.index(SCHEMA, TABLE, "c1", true, Index.UNIQUE_KEY_CONSTRAINT);
        b.indexColumn(SCHEMA, TABLE, "c1", "c1", 0, true, null);
        
        AISMerge merge = new AISMerge (t,s.getUserTable(TABLENAME));
        t = merge.merge().getAIS();
        
        UserTable targetTable = t.getUserTable(TABLENAME);
        UserTable sourceTable = s.getUserTable(TABLENAME);
        
        assertTrue (t.isFrozen());
        assertEquals (1,targetTable.getIndexes().size());
        assertEquals (targetTable.getIndexes().size(), sourceTable.getIndexes().size());
        assertNotNull (targetTable.getIndex("c1"));
        checkIndexColumns (targetTable.getIndex("c1").getKeyColumns(), "c1");
        assertNotNull(t.getGroup(TABLE));
        assertNotNull(t.getGroup(TABLE).getGroupTable().getIndex("t1$c1"));
        assertNull (targetTable.getPrimaryKey());

    }
    
    @Test
    public void testSimpleJoin() throws Exception {
        b.userTable(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "INT", (long)0, (long)0, false, false, null, null);
        b.column(SCHEMA, TABLE, "c2", 1, "INT", (long)0, (long)0, false, false, null, null);
        b.index(SCHEMA, TABLE, "PK", true, Index.PRIMARY_KEY_CONSTRAINT);
        b.indexColumn(SCHEMA, TABLE, "PK", "c1", 0, true, null);
        b.basicSchemaIsComplete();
        b.createGroup("FRED", SCHEMA, "_akiban_t1");
        b.addTableToGroup("FRED", SCHEMA, TABLE);
        b.groupingIsComplete();
        AISMerge merge = new AISMerge (t,s.getUserTable(TABLENAME));
        t = merge.merge().getAIS();
        assertTrue (t.isFrozen());
        assertEquals (TABLE, t.getUserTable(TABLENAME).getGroup().getName());
        assertEquals ("test._akiban_t1", t.getUserTable(TABLENAME).getGroup().getGroupTable().getName().toString());
        
        b.userTable(SCHEMA, "t2");
        b.column(SCHEMA, "t2", "c1", 0, "INT", (long)0, (long)0, false, false, null, null);
        b.column(SCHEMA, "t2", "c2", 1, "INT", (long)0, (long)0, true, false, null, null);
        b.joinTables("test/t1/test/t2", SCHEMA, TABLE, SCHEMA, "t2");
        b.joinColumns("test/t1/test/t2", SCHEMA, TABLE, "c1", SCHEMA, "t2", "c1");
        b.basicSchemaIsComplete();
        b.addJoinToGroup("FRED", "test/t1/test/t2", 0);
        b.groupingIsComplete();
        
        merge = new AISMerge (t, s.getUserTable(SCHEMA, "t2"));
        t = merge.merge().getAIS();
        
        assertEquals (1, t.getJoins().size());
        assertNotNull (t.getUserTable(SCHEMA, "t2").getParentJoin());
        assertEquals (1, t.getUserTable(TABLENAME).getChildJoins().size());
        assertNotNull (t.getGroup(TABLE));
        assertEquals (TABLE, t.getUserTable(SCHEMA, "t2").getGroup().getName());
        assertEquals (5, t.getGroupTable(SCHEMA, "_akiban_t1").getColumnsIncludingInternal().size());
    }
    
    @Test
    public void testTwoJoins() throws Exception {
        b.userTable(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "INT", (long)0, (long)0, false, false, null, null);
        b.column(SCHEMA, TABLE, "c2", 1, "INT", (long)0, (long)0, false, false, null, null);
        b.index(SCHEMA, TABLE, "PK", true, Index.PRIMARY_KEY_CONSTRAINT);
        b.indexColumn(SCHEMA, TABLE, "PK", "c1", 0, true, null);
        b.basicSchemaIsComplete();
        b.createGroup("FRED", SCHEMA, "_akiban_t1");
        b.addTableToGroup("FRED", SCHEMA, TABLE);
        b.groupingIsComplete();
        AISMerge merge = new AISMerge (t,s.getUserTable(TABLENAME));
        t = merge.merge().getAIS();
        assertTrue (t.isFrozen());

        b.userTable(SCHEMA, "t2");
        b.column(SCHEMA, "t2", "c1", 0, "INT", (long)0, (long)0, false, false, null, null);
        b.column(SCHEMA, "t2", "c2", 1, "INT", (long)0, (long)0, true, false, null, null);
        b.joinTables("test/t1/test/t2", SCHEMA, TABLE, SCHEMA, "t2");
        b.joinColumns("test/t1/test/t2", SCHEMA, TABLE, "c1", SCHEMA, "t2", "c1");
        b.basicSchemaIsComplete();
        b.addJoinToGroup("FRED", "test/t1/test/t2", 0);
        b.groupingIsComplete();
        
        merge = new AISMerge (t, s.getUserTable(SCHEMA, "t2"));
        t = merge.merge().getAIS();
        assertNotNull (t.getUserTable(SCHEMA, "t2").getParentJoin());
        assertEquals (1, t.getUserTable(TABLENAME).getChildJoins().size());
        assertNotNull (t.getGroup(TABLE));

        b.userTable(SCHEMA, "t3");
        b.column(SCHEMA, "t3", "c1", 0, "INT", (long)0, (long)0, false, false, null, null);
        b.column(SCHEMA, "t3", "c2", 1, "Int", (long)0, (long)0, false, false, null, null);
        b.joinTables("test/t1/test/t3", SCHEMA, TABLE, SCHEMA, "t3");
        b.joinColumns("test/t1/test/t3", SCHEMA, TABLE, "c1", SCHEMA, "t3", "c1");
        b.basicSchemaIsComplete();
        b.addJoinToGroup("FRED", "test/t1/test/t3", 0);
        b.groupingIsComplete();
        
        merge = new AISMerge (t, s.getUserTable(SCHEMA, "t3"));
        t = merge.merge().getAIS();
        assertNotNull (t.getUserTable(SCHEMA, "t3").getParentJoin());
        assertEquals (2, t.getUserTable(TABLENAME).getChildJoins().size());
        assertNotNull (t.getGroup(TABLE));
        assertEquals (TABLE, t.getUserTable(SCHEMA, "t3").getGroup().getName());
        assertEquals (8, t.getGroupTable(SCHEMA, "_akiban_t1").getColumnsIncludingInternal().size());
    }

    
    @Test (expected=InvalidOperationException.class)
    public void testJoinToBadParent() throws Exception
    {
        // Table 1
        b.userTable(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "INT", (long)0, (long)0, false, false, null, null);
        b.column(SCHEMA, TABLE, "c2", 1, "INT", (long)0, (long)0, false, false, null, null);
        b.index(SCHEMA, TABLE, "PK", true, Index.PRIMARY_KEY_CONSTRAINT);
        b.indexColumn(SCHEMA, TABLE, "PK", "c1", 0, true, null);
        b.basicSchemaIsComplete();
        b.createGroup("FRED", SCHEMA, "_akiban_t1");
        b.addTableToGroup("FRED", SCHEMA, TABLE);
        b.groupingIsComplete();
        AISMerge merge = new AISMerge (t,s.getUserTable(TABLENAME));
        t = merge.merge().getAIS();
        assertTrue (t.isFrozen());
        
        // table 3 : the fake table
        b.userTable(SCHEMA, "t3");
        b.column(SCHEMA, "t3", "c1", 0, "int", 0L, 0L, false, false, null, null);
        b.index(SCHEMA, "t3", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        b.indexColumn(SCHEMA, "t3", "pk", "c1", 0, true, null);
        b.createGroup("DOUG", SCHEMA, "_akiban_t3");
        b.addTableToGroup("DOUG", SCHEMA, "t3");
        // table 2 : join to wrong table. 
        b.userTable(SCHEMA, "t2");
        b.column(SCHEMA, "t2", "c1", 0, "INT", (long)0, (long)0, false, false, null, null);
        b.column(SCHEMA, "t2", "c2", 1, "INT", (long)0, (long)0, true, false, null, null);
        b.joinTables("test/t1/test/t2", SCHEMA, "t3", SCHEMA, "t2");
        b.joinColumns("test/t1/test/t2", SCHEMA, "t3", "c1", SCHEMA, "t2", "c1");
        b.basicSchemaIsComplete();
        b.addJoinToGroup("DOUG", "test/t1/test/t2", 0);
        b.groupingIsComplete();
        
        merge = new AISMerge (t, s.getUserTable(SCHEMA, "t2"));
        t = merge.merge().getAIS();
    }

    @Test (expected=InvalidOperationException.class)
    public void testBadParentJoin () throws Exception
    {
        // Table 1
        b.userTable(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "INT", (long)0, (long)0, false, false, null, null);
        b.column(SCHEMA, TABLE, "c2", 1, "INT", (long)0, (long)0, false, false, null, null);
        b.index(SCHEMA, TABLE, "PK", true, Index.PRIMARY_KEY_CONSTRAINT);
        b.indexColumn(SCHEMA, TABLE, "PK", "c1", 0, true, null);
        b.basicSchemaIsComplete();
        b.createGroup("FRED", SCHEMA, "_akiban_t1");
        b.addTableToGroup("FRED", SCHEMA, TABLE);
        b.groupingIsComplete();
        AISMerge merge = new AISMerge (t,s.getUserTable(TABLENAME));
        t = merge.merge().getAIS();
        assertTrue (t.isFrozen());
        
        
        b = new AISBuilder();
        // table 3 : the fake table
        b.userTable(SCHEMA, "t1");
        b.column(SCHEMA, "t1", "c5", 0, "int", 0L, 0L, false, false, null, null);
        b.index(SCHEMA, "t1", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        b.indexColumn(SCHEMA, "t1", "pk", "c5", 0, true, null);
        b.createGroup("DOUG", SCHEMA, "_akiban_t1");
        b.addTableToGroup("DOUG", SCHEMA, "t1");
        // table 2 : join to wrong table. 
        b.userTable(SCHEMA, "t2");
        b.column(SCHEMA, "t2", "c1", 0, "INT", (long)0, (long)0, false, false, null, null);
        b.column(SCHEMA, "t2", "c2", 1, "INT", (long)0, (long)0, true, false, null, null);
        b.joinTables("test/t1/test/t2", SCHEMA, "t1", SCHEMA, "t2");
        b.joinColumns("test/t1/test/t2", SCHEMA, "t1", "c5", SCHEMA, "t2", "c1");
        b.basicSchemaIsComplete();
        b.addJoinToGroup("DOUG", "test/t1/test/t2", 0);
        b.groupingIsComplete();
        
        merge = new AISMerge (t, b.akibanInformationSchema().getUserTable(SCHEMA, "t2"));
        t = merge.merge().getAIS();
        
    }

    @Test
    public void testFakeTableJoin() throws Exception
    {
        // Table 1
        b.userTable(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "INT", (long)0, (long)0, false, false, null, null);
        b.column(SCHEMA, TABLE, "c2", 1, "INT", (long)0, (long)0, false, false, null, null);
        b.index(SCHEMA, TABLE, "PK", true, Index.PRIMARY_KEY_CONSTRAINT);
        b.indexColumn(SCHEMA, TABLE, "PK", "c1", 0, true, null);
        b.basicSchemaIsComplete();
        b.createGroup("FRED", SCHEMA, "_akiban_t1");
        b.addTableToGroup("FRED", SCHEMA, TABLE);
        b.groupingIsComplete();
        AISMerge merge = new AISMerge (t,s.getUserTable(TABLENAME));
        t = merge.merge().getAIS();
        assertTrue (t.isFrozen());
        
        b = new AISBuilder();
        // table 3 : the fake table
        b.userTable(SCHEMA, "t1");
        b.column(SCHEMA, "t1", "c1", 0, "int", 0L, 0L, false, false, null, null);
        b.index(SCHEMA, "t1", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        b.indexColumn(SCHEMA, "t1", "pk", "c1", 0, true, null);
        b.createGroup("DOUG", SCHEMA, "_akiban_t1");
        b.addTableToGroup("DOUG", SCHEMA, "t1");
        // table 2 : join to wrong table. 
        b.userTable(SCHEMA, "t2");
        b.column(SCHEMA, "t2", "c1", 0, "INT", (long)0, (long)0, false, false, null, null);
        b.column(SCHEMA, "t2", "c2", 1, "INT", (long)0, (long)0, true, false, null, null);
        b.joinTables("test/t1/test/t2", SCHEMA, "t1", SCHEMA, "t2");
        b.joinColumns("test/t1/test/t2", SCHEMA, "t1", "c1", SCHEMA, "t2", "c1");
        b.basicSchemaIsComplete();
        b.addJoinToGroup("DOUG", "test/t1/test/t2", 0);
        b.groupingIsComplete();
        
        merge = new AISMerge (t, b.akibanInformationSchema().getUserTable(SCHEMA, "t2"));
        t = merge.merge().getAIS();

        assertNotNull (t.getUserTable(SCHEMA, "t2"));
        assertNotNull (t.getUserTable(SCHEMA, "t2").getParentJoin());
        assertEquals (1, t.getUserTable(TABLENAME).getChildJoins().size());
        assertNotNull (t.getGroup(TABLE));
        assertEquals (TABLE, t.getUserTable(SCHEMA, "t2").getGroup().getName());
        assertNotNull (t.getUserTable(SCHEMA, TABLE));
        assertEquals (TABLE, t.getUserTable(SCHEMA, TABLE).getGroup().getName());

    }

    @Test
    public void joinOfDifferingIntTypes() throws Exception {
        b.userTable(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "BIGINT", 0L, 0L, false, false, null, null);
        b.index(SCHEMA, TABLE, Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        b.indexColumn(SCHEMA, TABLE, Index.PRIMARY_KEY_CONSTRAINT, "c1", 0, true, null);
        b.basicSchemaIsComplete();
        b.createGroup(TABLE, SCHEMA, "_akiban_t1");
        b.addTableToGroup(TABLE, SCHEMA, TABLE);
        b.groupingIsComplete();
        
        AISMerge merge = new AISMerge(t, s.getUserTable(TABLENAME));
        t = merge.merge().getAIS();
        assertTrue(t.isFrozen());
        assertEquals(TABLE, t.getUserTable(TABLENAME).getGroup().getName());
        assertEquals("test._akiban_t1", t.getUserTable(TABLENAME).getGroup().getGroupTable().getName().toString());

        b.userTable(SCHEMA, "t2");
        b.column(SCHEMA, "t2", "c1", 0, "INT", 0L, 0L, false, false, null, null);
        b.column(SCHEMA, "t2", "parentid", 1, "INT", 0L, 0L, false, false, null, null);

        // join bigint->int
        b.joinTables("test/t1/test/t2", SCHEMA, TABLE, SCHEMA, "t2");
        b.joinColumns("test/t1/test/t2", SCHEMA, TABLE, "c1", SCHEMA, "t2", "parentid");
        b.basicSchemaIsComplete();
        b.addJoinToGroup(TABLE, "test/t1/test/t2", 0);
        b.groupingIsComplete();

        merge = new AISMerge(t, s.getUserTable(SCHEMA, "t2"));
        t = merge.merge().getAIS();

        assertEquals(1, t.getJoins().size());
        assertNotNull(t.getUserTable(SCHEMA, "t2").getParentJoin());
        assertEquals(1, t.getUserTable(TABLENAME).getChildJoins().size());
        assertNotNull(t.getGroup(TABLE));
        assertEquals(TABLE, t.getUserTable(SCHEMA, "t2").getGroup().getName());
    }

    @Test(expected= InvalidOperationException.class)
    public void joinDifferentTypes() throws Exception {
        b.userTable(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "BIGINT", 0L, 0L, false, false, null, null);
        b.index(SCHEMA, TABLE, Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        b.indexColumn(SCHEMA, TABLE, Index.PRIMARY_KEY_CONSTRAINT, "c1", 0, true, null);
        b.basicSchemaIsComplete();
        b.createGroup(TABLE, SCHEMA, "_akiban_t1");
        b.addTableToGroup(TABLE, SCHEMA, TABLE);
        b.groupingIsComplete();

        AISMerge merge = new AISMerge(t, s.getUserTable(TABLENAME));
        t = merge.merge().getAIS();
        assertTrue(t.isFrozen());
        assertEquals(TABLE, t.getUserTable(TABLENAME).getGroup().getName());
        assertEquals("test._akiban_t1", t.getUserTable(TABLENAME).getGroup().getGroupTable().getName().toString());

        b.userTable(SCHEMA, "t2");
        b.column(SCHEMA, "t2", "c1", 0, "INT", 0L, 0L, false, false, null, null);
        b.column(SCHEMA, "t2", "parentid", 1, "varchar", 32L, 0L, false, false, null, null);

        // join bigint->varchar
        b.joinTables("test/t1/test/t2", SCHEMA, TABLE, SCHEMA, "t2");
        b.joinColumns("test/t1/test/t2", SCHEMA, TABLE, "c1", SCHEMA, "t2", "parentid");
        b.basicSchemaIsComplete();
        b.addJoinToGroup(TABLE, "test/t1/test/t2", 0);
        b.groupingIsComplete();

        merge = new AISMerge(t, s.getUserTable(SCHEMA, "t2"));
        t = merge.merge().getAIS();
    }
    
    @Test
    public void columnIdentity () {
        b.userTable(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "INT", (long)0, (long)0, false, false, null, null);
        b.column(SCHEMA, TABLE, "c2", 1, "INT", (long)0, (long)0, false, false, null, null);
        b.sequence(SCHEMA, "seq-1", 5, 2, 0, 1000, false);
        b.columnAsIdentity(SCHEMA, TABLE, "c1", "seq-1", true);
        b.basicSchemaIsComplete();

        b.createGroup("FRED", SCHEMA, "_akiban_t1");
        b.addTableToGroup("FRED", SCHEMA, TABLE);
        b.groupingIsComplete();
        AISMerge merge = new AISMerge (t,s.getUserTable(TABLENAME));
        t = merge.merge().getAIS();
        
        assertNotNull (t.getTable(TABLENAME).getColumn(0).getIdentityGenerator());
        Sequence identityGenerator = t.getTable(TABLENAME).getColumn(0).getIdentityGenerator();
        //assertEquals(t.getTable(TABLENAME).getTreeName(), identityGenerator.getTreeName());
        assertEquals (5, identityGenerator.getStartsWith());
        assertEquals (2, identityGenerator.getIncrement());
        assertEquals (1000, identityGenerator.getMaxValue());
    }

    /*
     * Not really behavior that needs preserved, but at least one data set observed
     * to contain IDs outside of what we assume the valid UserTable range to be.
     */
    @Test
    public void userTableIDUniqueIncludingIS() {
        AISBuilder tb = new AISBuilder(t);
        tb.setTableIdOffset(AISMerge.AIS_TABLE_ID_OFFSET);

        tb.userTable(SCHEMA, "bar");
        tb.column(SCHEMA, "bar", "id", 0, "INT", null, null, false, false, null, null);
        tb.createGroup("bar", SCHEMA, "akiban_bar");
        tb.addTableToGroup("bar", SCHEMA, "bar");

        tb.userTable(TableName.INFORMATION_SCHEMA, "foo");
        tb.column(TableName.INFORMATION_SCHEMA, "foo", "id", 0, "INT", null, null, false, false, null, null);
        tb.createGroup("foo", TableName.INFORMATION_SCHEMA, "akiban_foo");
        tb.addTableToGroup("foo", TableName.INFORMATION_SCHEMA, "foo");

        tb.basicSchemaIsComplete();
        tb.groupingIsComplete();
        t.freeze();

        UserTable barTable = tb.akibanInformationSchema().getUserTable(SCHEMA, "bar");
        assertNotNull("bar table", barTable);
        UserTable fooTable = t.getUserTable(TableName.INFORMATION_SCHEMA, "foo");
        assertNotNull("foo table", fooTable);

        b.userTable(SCHEMA, "zap");
        b.column(SCHEMA, "zap", "id", 0, "INT", null, null, false, false, null, null);
        
        AISMerge merge = new AISMerge(t, s.getUserTable(SCHEMA, "zap"));
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
