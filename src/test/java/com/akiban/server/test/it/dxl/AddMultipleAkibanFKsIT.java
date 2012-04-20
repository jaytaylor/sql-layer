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

package com.akiban.server.test.it.dxl;

import com.akiban.ais.model.*;
import com.akiban.ais.model.validation.AISValidation;
import com.akiban.ais.model.validation.AISValidationFailure;
import com.akiban.ais.model.validation.AISValidationOutput;
import com.akiban.ais.model.validation.AISValidationResults;
import com.akiban.server.error.DuplicateIndexTreeNamesException;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
