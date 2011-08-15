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
package com.akiban.ais.model.validation;

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
    public void testDuplicateTables2() {
        builder = new AISBuilder();
        builder.userTable("test", "t1");
        builder.createGroup("test", "test", "t1");
    }
    
    @Test (expected=InvalidOperationException.class)
    public void testDuplicateTables3() {
        builder = new AISBuilder();
        builder.createGroup("test", "test", "t1");
        builder.userTable("test", "t1");
    }
    
    @Test (expected=InvalidOperationException.class)
    public void testDuplicateTables4() {
        builder = new AISBuilder();
        builder.createGroup ("test", "test", "t1");
        builder.createGroup ("fred", "test", "t1");
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
        builder.createGroup("test", "test", "t1");
        builder.createGroup("test", "test", "t2");
    }
}
