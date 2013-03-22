/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.ais.model.validation;

import com.akiban.server.error.DuplicateIndexColumnException;
import org.junit.Test;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.Index;
import com.akiban.server.error.InvalidOperationException;

public class AISInvariantsTest {

    private AISBuilder builder;
    
    @Test (expected=InvalidOperationException.class)
    public void testDupicateTables1() {
        builder = new AISBuilder();
        builder.userTable("test", "t1");
        builder.column("test", "t1", "c1", 0, "INT", (long)0, (long)0, false, true, null, null);
        
        builder.userTable("test", "t1");
    }

    @Test (expected=InvalidOperationException.class)
    public void testDuplicateColumns() {
        builder = new AISBuilder();
        
        builder.userTable("test", "t1");
        builder.column("test", "t1", "c1", 0, "int", (long)0, (long)0, false, true, null, null);
        builder.column("test", "t1", "c1", 1, "INT", (long)0, (long)0, false, false, null, null);

    }
    
    //@Test (expected=InvalidOperationException.class)
    public void testDuplicateColumnPos() {
        builder = new AISBuilder();
        
        builder.userTable("test", "t1");
        builder.column("test", "t1", "c1", 0, "int", (long)0, (long)0, false, true, null, null);
        builder.column("test", "t1", "c2", 0, "int", (long)0, (long)0, false, false, null, null);
    }
    
    @Test (expected=InvalidOperationException.class)
    public void testDuplicateIndexes() {
        builder = new AISBuilder();
        builder.userTable("test", "t1");
        builder.column("test", "t1", "c1", 0, "int", (long)0, (long)0, false, true, null, null);
        builder.column("test", "t1", "c2", 1, "int", (long)0, (long)0, false, false, null, null);
        
        builder.index("test", "t1", "PRIMARY", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("test", "t1", "PRIMARY", "c1", 0, true, null);
        builder.index("test", "t1", "PRIMARY", true, Index.PRIMARY_KEY_CONSTRAINT);
    }
    
    @Test (expected=InvalidOperationException.class)
    public void testDuplicateGroup() {
        builder = new AISBuilder();
        builder.createGroup("test", "test");
        builder.createGroup("test", "test");
    }

    @Test (expected=DuplicateIndexColumnException.class)
    public void testDuplicateColumnsTableIndex() {
        builder = new AISBuilder();
        builder.userTable("test", "t1");
        builder.column("test", "t1", "c1", 0, "INT", null, null, false, false, null, null);
        builder.index("test", "t1", "c1_index", false, Index.KEY_CONSTRAINT);
        builder.indexColumn("test", "t1", "c1_index", "c1", 0, true, null);
        builder.indexColumn("test", "t1", "c1_index", "c1", 1, true, null);
    }

    @Test (expected=DuplicateIndexColumnException.class)
    public void testDuplicateColumnsGroupIndex() {
        builder = createSimpleValidGroup();
        builder.groupIndex("t1", "y_x", false, Index.JoinType.LEFT);
        builder.groupIndexColumn("t1", "y_x", "test", "t2", "y", 0);
        builder.groupIndexColumn("t1", "y_x", "test", "t2", "y", 1);
    }

    @Test
    public void testDuplicateColumnNamesButValidGroupIndex() {
        builder = createSimpleValidGroup();
        builder.groupIndex("t1", "y_y", false, Index.JoinType.LEFT);
        builder.groupIndexColumn("t1", "y_y", "test", "t2", "y", 0);
        builder.groupIndexColumn("t1", "y_y", "test", "t1", "y", 1);
    }

    private static AISBuilder createSimpleValidGroup() {
        AISBuilder builder = new AISBuilder();
        builder.userTable("test", "t1");
        builder.column("test", "t1", "c1", 0, "INT", null, null, false, false, null, null);
        builder.column("test", "t1", "x", 1, "INT", null, null, false, false, null, null);
        builder.column("test", "t1", "y", 2, "INT", null, null, false, false, null, null);
        builder.index("test", "t1", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("test", "t1", Index.PRIMARY_KEY_CONSTRAINT, "c1", 0, true, null);
        builder.userTable("test", "t2");
        builder.column("test", "t2", "c1", 0, "INT", null, null, false, false, null, null);
        builder.column("test", "t2", "c2", 1, "INT", null, null, false, false, null, null);
        builder.column("test", "t2", "y", 2, "INT", null, null, false, false, null, null);
        builder.basicSchemaIsComplete();

        builder.createGroup("t1", "test");
        builder.addTableToGroup("t1", "test", "t1");
        builder.joinTables("t2/t1", "test", "t1", "test", "t2");
        builder.joinColumns("t2/t1", "test", "t2", "c1", "test", "t2", "c2");
        builder.addJoinToGroup("t1", "t2/t1", 0);
        builder.groupingIsComplete();
        return builder;
    }
}
