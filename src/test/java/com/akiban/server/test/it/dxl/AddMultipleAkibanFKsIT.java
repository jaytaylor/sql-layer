
package com.akiban.server.test.it.dxl;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;
import com.akiban.server.test.it.ITBase;
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
        UserTable root = ais.getUserTable("schema", "root");
        int check = 0;
        for (Join join : root.getChildJoins()) {
            UserTable child = join.getChild();
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
        for (UserTable userTable : ais.getUserTables().values()) {
            assertTrue(!userTable.getName().getTableName().startsWith("TEMP"));
        }
    }
}
