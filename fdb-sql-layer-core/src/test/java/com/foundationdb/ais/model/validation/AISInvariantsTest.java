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

package com.foundationdb.ais.model.validation;

import com.foundationdb.server.error.DuplicateIndexColumnException;
import org.junit.Test;

import com.foundationdb.ais.model.AISBuilder;
import com.foundationdb.ais.model.Index;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.service.TestTypesRegistry;

public class AISInvariantsTest {
    private AISBuilder builder;
    
    private TInstance intType = TestTypesRegistry.MCOMPAT
        .getTypeClass("MCOMPAT", "INT").instance(false);

    @Test (expected=InvalidOperationException.class)
    public void testDuplicateTables() {
        builder = new AISBuilder();
        builder.table("test", "t1");
        builder.column("test", "t1", "c1", 0, intType, true, null, null);
        
        builder.table("test", "t1");
    }

    @Test (expected=InvalidOperationException.class)
    public void testDuplicateColumns() {
        builder = new AISBuilder();
        
        builder.table("test", "t1");
        builder.column("test", "t1", "c1", 0, intType, true, null, null);
        builder.column("test", "t1", "c1", 1, intType, false, null, null);

    }
    
    //@Test (expected=InvalidOperationException.class)
    public void testDuplicateColumnPos() {
        builder = new AISBuilder();
        
        builder.table("test", "t1");
        builder.column("test", "t1", "c1", 0, intType, true, null, null);
        builder.column("test", "t1", "c2", 0, intType, false, null, null);
    }
    
    @Test (expected=InvalidOperationException.class)
    public void testDuplicateIndexes() {
        builder = new AISBuilder();
        builder.table("test", "t1");
        builder.column("test", "t1", "c1", 0, intType, true, null, null);
        builder.column("test", "t1", "c2", 1, intType, false, null, null);
        
        builder.pk("test", "t1");
        builder.indexColumn("test", "t1", Index.PRIMARY, "c1", 0, true, null);
        builder.pk("test", "t1");
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
        builder.table("test", "t1");
        builder.column("test", "t1", "c1", 0, intType, false, null, null);
        builder.index("test", "t1", "c1_index");
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

    private AISBuilder createSimpleValidGroup() {
        AISBuilder builder = new AISBuilder();
        builder.table("test", "t1");
        builder.column("test", "t1", "c1", 0, intType, false, null, null);
        builder.column("test", "t1", "x", 1, intType, false, null, null);
        builder.column("test", "t1", "y", 2, intType, false, null, null);
        builder.pk("test", "t1");
        builder.indexColumn("test", "t1", Index.PRIMARY, "c1", 0, true, null);
        builder.table("test", "t2");
        builder.column("test", "t2", "c1", 0, intType, false, null, null);
        builder.column("test", "t2", "c2", 1, intType, false, null, null);
        builder.column("test", "t2", "y", 2, intType, false, null, null);
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
