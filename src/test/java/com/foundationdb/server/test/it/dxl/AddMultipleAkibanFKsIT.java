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

package com.foundationdb.server.test.it.dxl;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

// Inspired by bug 874459. These DDL steps simulate MySQL running ALTER TABLE statements which add Akiban FKs.

public class AddMultipleAkibanFKsIT extends ITBase
{
    @Test
    public void createRenameCreate()
    {
        createTable("schema", "root", "id int not null, primary key(id)");
        // Create children
        createTable("schema", "child1", "id int not null, rid int, primary key(id)");
        createTable("schema", "child2", "id int not null, rid int, primary key(id)");
        createTable("schema", "child3", "id int not null, rid int, primary key(id)");
        // Add Akiban FK to child1
        createTable("schema", "TEMP", "id int not null, rid int, primary key(id)", akibanFK("rid", "root", "id"));
        ddl().renameTable(session(), tableName("schema", "child1"), tableName("schema", "TEMP2"));
        ddl().renameTable(session(), tableName("schema", "TEMP"), tableName("schema", "child1"));
        ddl().dropTable(session(), tableName("schema", "TEMP2"));
        // Add Akiban FK to child2
        createTable("schema", "TEMP", "id int not null, rid int, primary key(id)", akibanFK("rid", "root", "id"));
        ddl().renameTable(session(), tableName("schema", "child2"), tableName("schema", "TEMP2"));
        ddl().renameTable(session(), tableName("schema", "TEMP"), tableName("schema", "child2"));
        ddl().dropTable(session(), tableName("schema", "TEMP2"));
        // Add Akiban FK to child3
        createTable("schema", "TEMP", "id int not null, rid int, primary key(id)", akibanFK("rid", "root", "id"));
        ddl().renameTable(session(), tableName("schema", "child3"), tableName("schema", "TEMP2"));
        ddl().renameTable(session(), tableName("schema", "TEMP"), tableName("schema", "child3"));
        ddl().dropTable(session(), tableName("schema", "TEMP2"));
        AkibanInformationSchema ais = ddl().getAIS(session());
        Table root = ais.getTable("schema", "root");
        int check = 0;
        for (Join join : root.getChildJoins()) {
            Table child = join.getChild();
            assertEquals(root, join.getParent());
            assertEquals(join, child.getParentJoin());
            String childName = child.getName().getTableName();
            if (childName.equals("child1")) {
                check |= 0x1;
            } else if (childName.equals("child2")) {
                check |= 0x2;
            } else if (childName.equals("child3")) {
                check |= 0x4;
            } else {
                fail();
            }
        }
        assertEquals(0x7, check);
        for (Table table : ais.getTables().values()) {
            assertTrue(!table.getName().getTableName().startsWith("TEMP"));
        }
    }
}
