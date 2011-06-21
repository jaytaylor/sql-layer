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

package com.akiban.ais.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

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
        AISMerge merge = new AISMerge (t, s.getUserTable(TABLENAME));
        t = merge.validate().merge().getAIS();
        UserTable targetTable = t.getUserTable(TABLENAME);
        UserTable sourceTable = s.getUserTable(TABLENAME);

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
        
        t.checkIntegrity();
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
        b.indexColumn(SCHEMA, TABLE, "PRIMARY", "c1", 0, true, 0);
        b.basicSchemaIsComplete();
        AISMerge merge = new AISMerge (t,s.getUserTable(TABLENAME));
        t = merge.validate().merge().getAIS();
        
        UserTable targetTable = t.getUserTable(TABLENAME);
        UserTable sourceTable = s.getUserTable(TABLENAME);
        
        assertEquals(targetTable.getIndexes().size(), 1);
        assertEquals(targetTable.getIndexes().size(), sourceTable.getIndexes().size());
        assertNotNull (targetTable.getPrimaryKey());
        checkColumns(targetTable.getPrimaryKey().getColumns(), "c1");
        
        t.checkIntegrity();
        assertNotNull(t.getGroup(TABLE));
        assertNotNull(t.getGroup(TABLE).getGroupTable().getIndex("t1$PRIMARY"));
    }
    
    @Test
    public void uniqueIndexTest() throws Exception {
        b.userTable(SCHEMA, TABLE);
        b.column(SCHEMA, TABLE, "c1", 0, "int", (long)0, (long)0, false, false, null, null);
        b.index(SCHEMA, TABLE, "c1", true, Index.UNIQUE_KEY_CONSTRAINT);
        b.indexColumn(SCHEMA, TABLE, "c1", "c1", 0, true, 0);
        
        AISMerge merge = new AISMerge (t,s.getUserTable(TABLENAME));
        t = merge.validate().merge().getAIS();
        
        UserTable targetTable = t.getUserTable(TABLENAME);
        UserTable sourceTable = s.getUserTable(TABLENAME);
        
        assertEquals (targetTable.getIndexes().size(),1);
        assertEquals (targetTable.getIndexes().size(), sourceTable.getIndexes().size());
        assertNotNull (targetTable.getIndex("c1"));
        checkIndexColumns (targetTable.getIndex("c1").getColumns(), "c1");
        assertNotNull(t.getGroup(TABLE));
        assertNotNull(t.getGroup(TABLE).getGroupTable().getIndex("t1$c1"));
        
        
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