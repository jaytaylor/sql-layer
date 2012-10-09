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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.akiban.ais.model.UserTable;
import com.akiban.ais.util.DDLGenerator;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.NoSuchIndexException;
import com.akiban.server.error.ProtectedIndexException;

import com.akiban.server.test.it.ITBase;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

public final class DropIndexesIT extends ITBase {
    private void checkDDL(Integer tableId, String expected) {
        final UserTable table = getUserTable(tableId);
        DDLGenerator gen = new DDLGenerator();
        String actual = gen.createTable(table);
        assertEquals(table.getName() + "'s create statement", expected, actual);
    }

    
    @Test
    public void emptyIndexList() throws InvalidOperationException {
        int tid = createTable("test", "t", "id int not null primary key");
        ddl().dropTableIndexes(session(), tableName(tid), Collections.<String>emptyList());
    }
    
    @Test(expected=NoSuchTableException.class)
    public void unknownTable() throws InvalidOperationException {
        ddl().dropTableIndexes(session(), tableName("test", "bar"), Arrays.asList("bar"));
    }
    
    @Test(expected=NoSuchIndexException.class)
    public void unknownIndex() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, name varchar(255)");
        ddl().dropTableIndexes(session(), tableName(tId), Arrays.asList("name"));
    }

    @Test(expected=NoSuchIndexException.class)
    public void hiddenPrimaryKey() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int, name varchar(255)");
        ddl().dropTableIndexes(session(), tableName(tId), Arrays.asList("PRIMARY"));
    }
    
    @Test(expected=ProtectedIndexException.class)
    public void declaredPrimaryKey() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, name varchar(255)");
        ddl().dropTableIndexes(session(), tableName(tId), Arrays.asList("PRIMARY"));
    }

    @Test
    public void basicConfirmNotInAIS() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, name varchar(255)");
        createIndex("test", "t", "name", "name");
        ddl().dropTableIndexes(session(), tableName(tId), Arrays.asList("name"));

        // Index should be gone from UserTable
        UserTable uTable = getUserTable("test", "t");
        assertNotNull(uTable);
        assertNull(uTable.getIndex("name"));
    }
    
    @Test
    public void nonUniqueVarchar() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, name varchar(255)");
        createIndex("test", "t", "name", "name");
        dml().writeRow(session(), createNewRow(tId, 1, "bob"));
        dml().writeRow(session(), createNewRow(tId, 2, "jim"));
        ddl().dropTableIndexes(session(), tableName(tId), Arrays.asList("name"));
        updateAISGeneration();

        checkDDL(tId, "create table `test`.`t`(`id` int NOT NULL, `name` varchar(255), PRIMARY KEY(`id`)) engine=akibandb DEFAULT CHARSET=utf8 COLLATE=utf8_bin");

        List<NewRow> rows = scanAll(scanAllRequest(tId));
        assertEquals("rows from table scan", 2, rows.size());
    }
    
    @Test
    public void nonUniqueVarcharMiddleOfGroup() throws InvalidOperationException {
        int cId = createTable("coi", "c", "cid int not null primary key, name varchar(32)");
        int oId = createTable("coi", "o", "oid int not null primary key, c_id int, tag varchar(32), GROUPING FOREIGN KEY (c_id) REFERENCES c(cid)");
        createIndex("coi", "o", "tag", "tag");
        createGroupingFKIndex("coi", "o", "__akiban_fk_c", "c_id");
        int iId = createTable("coi", "i", "iid int not null primary key, o_id int, idesc varchar(32), GROUPING FOREIGN KEY (o_id) REFERENCES o(oid)");

        // One customer, two orders, 5 items
        dml().writeRow(session(), createNewRow(cId, 1, "bob"));
        dml().writeRow(session(), createNewRow(oId, 1, 1, "supplies"));
        dml().writeRow(session(), createNewRow(oId, 2, 1, "random"));
        dml().writeRow(session(), createNewRow(iId, 1, 1, "foo"));
        dml().writeRow(session(), createNewRow(iId, 2, 1, "bar"));
        dml().writeRow(session(), createNewRow(iId, 3, 2, "zap"));
        dml().writeRow(session(), createNewRow(iId, 4, 2, "fob"));
        dml().writeRow(session(), createNewRow(iId, 5, 2, "baz"));
        
        ddl().dropTableIndexes(session(), tableName(oId), Arrays.asList("tag"));
        updateAISGeneration();
        
        checkDDL(oId, "create table `coi`.`o`(`oid` int NOT NULL, `c_id` int, `tag` varchar(32), PRIMARY KEY(`oid`), "+
                      "CONSTRAINT `__akiban_fk_c` FOREIGN KEY `__akiban_fk_c`(`c_id`) REFERENCES `c`(`cid`)) engine=akibandb DEFAULT CHARSET=utf8 COLLATE=utf8_bin");

        List<NewRow> rows = scanAll(scanAllRequest(cId));
        assertEquals("customers from table scan", 1, rows.size());
        rows = scanAll(scanAllRequest(oId));
        assertEquals("orders from table scan", 2, rows.size());
        rows = scanAll(scanAllRequest(iId));
        assertEquals("items from table scan", 5, rows.size());
    }
    
    @Test
    public void nonUniqueCompoundVarcharVarchar() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, \"first\" varchar(255), \"last\" varchar(255)");
        createIndex("test", "t", "name", "\"first\"", "\"last\"");
        dml().writeRow(session(), createNewRow(tId, 1, "foo", "bar"));
        dml().writeRow(session(), createNewRow(tId, 2, "zap", "snap"));
        dml().writeRow(session(), createNewRow(tId, 3, "baz", "fob"));
        ddl().dropTableIndexes(session(), tableName(tId), Arrays.asList("name"));
        updateAISGeneration();
        
        checkDDL(tId, "create table `test`.`t`(`id` int NOT NULL, `first` varchar(255), `last` varchar(255), PRIMARY KEY(`id`)) engine=akibandb DEFAULT CHARSET=utf8 COLLATE=utf8_bin");

        List<NewRow> rows = scanAll(scanAllRequest(tId));
        assertEquals("rows from table scan", 3, rows.size());
    }
    
    @Test
    public void uniqueChar() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, state char(2), unique(state)");
        dml().writeRow(session(), createNewRow(tId, 1, "IA"));
        dml().writeRow(session(), createNewRow(tId, 2, "WA"));
        dml().writeRow(session(), createNewRow(tId, 3, "MA"));
        
        ddl().dropTableIndexes(session(), tableName(tId), Arrays.asList("state"));
        updateAISGeneration();
        
        checkDDL(tId, "create table `test`.`t`(`id` int NOT NULL, `state` char(2), PRIMARY KEY(`id`)) engine=akibandb DEFAULT CHARSET=utf8 COLLATE=utf8_bin");

        List<NewRow> rows = scanAll(scanAllRequest(tId));
        assertEquals("rows from table scan", 3, rows.size());
    }
    
    @Test
    public void uniqueIntNonUniqueDecimal() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, otherid int, price decimal(10,2), unique(otherid)");
        createIndex("test", "t", "price", "price");
        dml().writeRow(session(), createNewRow(tId, 1, 1337, "10.50"));
        dml().writeRow(session(), createNewRow(tId, 2, 5000, "10.50"));
        dml().writeRow(session(), createNewRow(tId, 3, 47000, "9.99"));
        
        ddl().dropTableIndexes(session(), tableName(tId), Arrays.asList("otherId", "price"));
        updateAISGeneration();
        
        checkDDL(tId, "create table `test`.`t`(`id` int NOT NULL, `otherid` int, `price` decimal(10, 2), PRIMARY KEY(`id`)) engine=akibandb DEFAULT CHARSET=utf8 COLLATE=utf8_bin");

        List<NewRow> rows = scanAll(scanAllRequest(tId));
        assertEquals("rows from table scan", 3, rows.size());
    }
}
