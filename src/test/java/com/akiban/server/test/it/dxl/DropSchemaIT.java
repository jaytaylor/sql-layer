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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.View;
import com.akiban.server.error.ForeignConstraintDDLException;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.NoSuchSchemaException;
import com.akiban.server.error.ViewReferencesExist;
import com.akiban.server.test.it.ITBase;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public final class DropSchemaIT extends ITBase {
    private void expectTables(String schemaName, String... tableNames) {
        final AkibanInformationSchema ais = ddl().getAIS(session());
        for (String name : tableNames) {
            final UserTable userTable = ais.getUserTable(schemaName, name);
            assertNotNull(schemaName + " " + name + " doesn't exist", userTable);
        }
    }

    private void expectNotTables(String schemaName, String... tableNames) {
        final AkibanInformationSchema ais = ddl().getAIS(session());
        for (String name : tableNames) {
            final UserTable userTable = ais.getUserTable(schemaName, name);
            assertNull(schemaName + " " + name + " still exists", userTable);
        }
    }

    private void expectSequences (String schemaName, String... sequenceNames) {
        final AkibanInformationSchema ais = ddl().getAIS(session());
        for (String name : sequenceNames) {
            final Sequence sequence = ais.getSequence(new TableName(schemaName, name));
            assertNotNull (schemaName + "." + name + " doesn't exist", sequence);
        }
        
    }
    private void expectNotSequence (String schemaName, String... sequenceNames) {
        final AkibanInformationSchema ais = ddl().getAIS(session());
        for (String name : sequenceNames) {
            final Sequence sequence = ais.getSequence(new TableName(schemaName, name));
            assertNull (schemaName + "." + name + " still exists", sequence);
        }
    }

    private void expectViews(String schemaName, String... viewNames) {
        final AkibanInformationSchema ais = ddl().getAIS(session());
        for (String name : viewNames) {
            final View view = ais.getView(new TableName(schemaName, name));
            assertNotNull (schemaName + "." + name + " doesn't exist", view);
        }
        
    }
    private void expectNotViews(String schemaName, String... viewNames) {
        final AkibanInformationSchema ais = ddl().getAIS(session());
        for (String name : viewNames) {
            final View view = ais.getView(new TableName(schemaName, name));
            assertNull (schemaName + "." + name + " still exists", view);
        }
    }

    @Test
    public void unknownSchemaIsNoOp() throws InvalidOperationException {
        createTable("one", "t", "id int not null primary key");
        ddl().dropSchema(session(), "not_a_real_schema");
        expectTables("one", "t");
    }

    @Test
    public void singleTable() throws InvalidOperationException {
        createTable("one", "t", "id int not null primary key");
        ddl().dropSchema(session(), "one");
        expectNotTables("one", "t");
    }

    @Test
    public void singleTableCheckData() throws InvalidOperationException {
        final int tid1 = createTable("one", "t", "id int not null primary key");
        writeRows(createNewRow(tid1, 1L), createNewRow(tid1, 2L));
        ddl().dropSchema(session(), "one");
        expectNotTables("one", "t");
        // Check for lingering data
        final int tid2 = createTable("one", "t", "id int not null primary key");
        expectRowCount(tid2, 0);
        assertEquals("scanned rows", 0, scanFull(scanAllRequest(tid2)).size());
    }

    @Test
    public void multipleTables() throws InvalidOperationException {
        createTable("one", "a", "id int not null primary key");
        createTable("one", "b", "id int not null primary key");
        createTable("two", "a", "id int not null primary key");
        createTable("two", "b", "id int not null primary key");
        ddl().dropSchema(session(), "one");
        expectNotTables("one", "a", "b");
        expectTables("two", "a", "b");
    }

    @Test
    public void groupedTables() throws InvalidOperationException {
        createTable("one", "c", "id int not null primary key");
        createTable("one", "o", "id int not null primary key, cid int, grouping foreign key(cid) references c(id)");
        createTable("one", "i", "id int not null primary key, oid int, grouping foreign key(oid) references o(id)");
        createTable("one", "t", "id int not null primary key");
        createTable("two", "c", "id int not null primary key");
        ddl().dropSchema(session(), "one");
        expectNotTables("one", "c", "o", "i", "t");
        expectTables("two", "c");
    }

    @Test
    public void crossSchemaGroupInvalid() throws InvalidOperationException {
        createTable("one", "c", "id int not null primary key");
        createTable("one", "o", "id int not null primary key, cid int, grouping foreign key(cid) references c(id)");
        createTable("two", "i", "id int not null primary key, oid int, grouping foreign key(oid) references one.o(id)");
        try {
            ddl().dropSchema(session(), "one");
            Assert.fail("ForeignConstraintDDLException expected");
        } catch(ForeignConstraintDDLException e) {
            // expected
        }
        expectTables("one", "c", "o");
        expectTables("two", "i");
    }

    @Test
    public void crossSchemaGroupValid() throws InvalidOperationException {
        createTable("one", "c", "id int not null primary key");
        createTable("one", "o", "id int not null primary key, cid int, grouping foreign key(cid) references c(id)");
        createTable("two", "i", "id int not null primary key, oid int, grouping foreign key(oid) references one.o(id)");
        ddl().dropSchema(session(), "two");
        expectTables("one", "c", "o");
        expectNotTables("two", "i");
    }
    
    @Test
    public void dropSchemaSequence() throws InvalidOperationException {
        createTable("one", "o", "id int not null generated by default as identity (start with 1)");
        createSequence("one", "seq1", "Start with 1 increment by 1 no cycle");
        ddl().dropSchema(session(), "one");
        expectNotSequence("one", "seq1");
        expectNotTables("one", "o");
    }
    
    @Test
    public void dropSchemaOtherSequence() throws InvalidOperationException {
        createSequence("one", "seq1", "Start with 1 increment by 1 no cycle");
        createSequence("two", "seq2", "Start with 2 increment by 2 no cycle");
        ddl().dropSchema(session(), "one");
        expectNotSequence("one", "seq1");
        expectNotTables("one", "o");
        expectSequences("two", "seq2");
        
    }

    @Test
    public void dropViewValidInSchema() throws Exception {
        createTable("one", "t1",
                    "id int not null primary key", "name varchar(128)");
        createTable("two", "t2",
                    "id int not null primary key", "name varchar(128)");
        createView("one", "v1",
                   "SELECT * FROM t1");
        createView("two", "v2",
                   "SELECT * FROM t2");
        ddl().dropSchema(session(), "one");
        expectNotTables("one", "t1");
        expectNotViews("one", "v1");
        expectTables("two", "t2");
        expectViews("two", "v2");
    }

    @Test
    public void dropViewInvalidOutsideSchema() throws Exception {
        createTable("one", "t1",
                    "id int not null primary key", "name varchar(128)");
        createTable("two", "t2",
                    "id int not null primary key", "name varchar(128)");
        createView("one", "crossview",
                   "SELECT t1.id,t1.name,t2.name AS name2 FROM one.t1 t1, two.t2 t2 WHERE t1.id = t2.id");
        try {
            ddl().dropSchema(session(), "one");
        } catch (ViewReferencesExist ex) {
            // expected
        }
        expectTables("one", "t1");
        expectTables("two", "t2");
        expectViews("one", "crossview");
    }

    @Test
    public void dropViewsInOrder() throws Exception {
        createTable("test", "t1",
                    "id int not null primary key", "name varchar(128)");
        createView("test", "v1",
                   "SELECT * FROM t1");
        createView("test", "v3",
                   "SELECT * FROM v1");
        createView("test", "v2",
                   "SELECT * FROM v3");
        ddl().dropSchema(session(), "test");
        expectNotViews("test", "v1", "v2", "v3");
    }

    @Test
    public void createDropRecreateDropAndRestart() throws Exception {
        createTable("test2", "customer", "id int not null primary key");
        createTable("test2", "order", "name varchar(32)");
        createTable("test2", "item", "cost int");

        ddl().dropSchema(session(), "test2");
        expectNotTables("test2", "customer", "order", "item");

        createTable("test2", "order", "id int not null primary key");
        createTable("test2", "customer", "id int not null primary key, oid int, grouping foreign key(oid) references \"order\"(id)");

        ddl().dropSchema(session(), "test2");
        expectNotTables("test2", "customer", "order");

        safeRestartTestServices();
        expectNotTables("test2", "customer", "order");
    }
}
