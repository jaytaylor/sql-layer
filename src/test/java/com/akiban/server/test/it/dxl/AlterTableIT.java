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

import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.util.OperatorBasedTableCopier;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.service.dxl.DXLReadWriteLockHook;
import com.akiban.server.test.it.ITBase;
import com.akiban.server.test.it.qp.TestRow;
import com.akiban.sql.StandardException;
import com.akiban.sql.aisddl.AlterTableDDL;
import com.akiban.sql.parser.AlterTableNode;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.StatementNode;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

// TODO: Cleanup once ALTER exists on DDLFunctions directly
public class AlterTableIT extends ITBase {
    private final String SCHEMA = "test";

    private void runAlter(String sql) throws StandardException {
        SQLParser parser = new SQLParser();
        StatementNode node = parser.parseStatement(sql);
        assertTrue("is alter node", node instanceof AlterTableNode);
        OperatorBasedTableCopier copier = new OperatorBasedTableCopier(configService(), treeService(), session(), store());
        AlterTableDDL.alterTable(DXLReadWriteLockHook.only(), ddl(), dml(), session(), copier, SCHEMA, (AlterTableNode)node);
        updateAISGeneration();
    }

    private RowBase testRow(RowType type, Object... fields) {
        return new TestRow(type, fields);
    }

    @Test
    public void addSingleColumnSingleTableGroup() throws StandardException {
        int cid = createTable(SCHEMA, "c", "id int not null primary key, v varchar(32)");
        writeRows(
                createNewRow(cid, 1L, "a"),
                createNewRow(cid, 2L, "b"),
                createNewRow(cid, 3L, "asdf"),
                createNewRow(cid, 10L, "asdfasdfasdf")
        );

        runAlter("ALTER TABLE c ADD COLUMN c1 INT NULL");

        expectFullRows(
                cid,
                createNewRow(cid, 1L, "a", null),
                createNewRow(cid, 2L, "b", null),
                createNewRow(cid, 3L, "asdf", null),
                createNewRow(cid, 10L, "asdfasdfasdf", null)
        );
    }

    @Test
    public void dropSingleColumnSingleTableGroup() throws StandardException {
        int cid = createTable(SCHEMA, "c", "id int not null primary key, v varchar(32)");
        writeRows(
                createNewRow(cid, 1L, "a"),
                createNewRow(cid, 2L, "b"),
                createNewRow(cid, 3L, "asdf"),
                createNewRow(cid, 10L, "asdfasdfasdf")
        );

        runAlter("ALTER TABLE c DROP COLUMN v");

        expectFullRows(
                cid,
                createNewRow(cid, 1L),
                createNewRow(cid, 2L),
                createNewRow(cid, 3L),
                createNewRow(cid, 10L)
        );
    }

    @Test
    public void simpleAddGroupingForeignKey() throws StandardException {
        int cid = createTable(SCHEMA, "c", "id int not null primary key, v varchar(32)");
        int oid = createTable(SCHEMA, "o", "id int not null primary key, cid int, tag char(1), grouping foreign key(cid) references c(id)");
        int iid = createTable(SCHEMA, "i", "id int not null primary key, spare_id int, tag2 char(1)");

        writeRows(
                createNewRow(cid, 1, "asdf"),
                createNewRow(cid, 5, "qwer"),
                createNewRow(cid, 10, "zxcv")
        );
        writeRows(
                createNewRow(oid, 10, 1, "a"),
                createNewRow(oid, 11, 1, "b"),
                createNewRow(oid, 60, 6, "c")
        );
        writeRows(
                createNewRow(iid, 100, 10, "d"),
                createNewRow(iid, 101, 10, "e"),
                createNewRow(iid, 102, 10, "f"),
                createNewRow(iid, 200, 20, "d")
        );

        runAlter("ALTER TABLE i ADD GROUPING FOREIGN KEY(spare_id) REFERENCES o(id)");
        updateAISGeneration();

        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        RowType cType = schema.userTableRowType(getUserTable(SCHEMA, "c"));
        RowType oType = schema.userTableRowType(getUserTable(SCHEMA, "o"));
        RowType iType = schema.userTableRowType(getUserTable(SCHEMA, "i"));

        StoreAdapter adapter = new PersistitAdapter(schema, store(), treeService(), session(), configService());
        compareRows(
                new RowBase[] {
                        // null c
                            // no o20
                                testRow(iType, 200, 20, "d"),
                        testRow(cType, 1L, "asdf"),
                            testRow(oType, 10, 1, "a"),
                                testRow(iType, 100, 10, "d"),
                                testRow(iType, 101, 10, "e"),
                                testRow(iType, 102, 10, "f"),
                            testRow(oType, 11, 1, "b"),
                        testRow(cType, 5, "qwer"),
                        // no c6
                            testRow(oType, 60, 6, "c"),
                        testRow(cType, 10, "zxcv")

                },
                adapter.newGroupCursor(cType.userTable().getGroup().getGroupTable())
        );
    }

    @Test
    public void simpleDropGroupingForeignKey() throws StandardException {
        int cid = createTable(SCHEMA, "c", "id int not null primary key, v varchar(32)");
        int oid = createTable(SCHEMA, "o", "id int not null primary key, cid int, tag char(1), grouping foreign key(cid) references c(id)");

        writeRows(
                createNewRow(cid, 1, "asdf"),
                createNewRow(cid, 5, "qwer"),
                createNewRow(cid, 10, "zxcv")
        );
        writeRows(
                createNewRow(oid, 10, 1, "a"),
                createNewRow(oid, 11, 1, "b"),
                createNewRow(oid, 60, 6, "c")
        );

        runAlter("ALTER TABLE o DROP GROUPING FOREIGN KEY");
        updateAISGeneration();

        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        RowType cType = schema.userTableRowType(getUserTable(SCHEMA, "c"));
        RowType oType = schema.userTableRowType(getUserTable(SCHEMA, "o"));

        StoreAdapter adapter = new PersistitAdapter(schema, store(), treeService(), session(), configService());
        compareRows(
                new RowBase[] {
                        testRow(cType, 1L, "asdf"),
                        testRow(cType, 5, "qwer"),
                        testRow(cType, 10, "zxcv")
                },
                adapter.newGroupCursor(cType.userTable().getGroup().getGroupTable())
        );
        compareRows(
                new RowBase[] {
                        testRow(oType, 10, 1, "a"),
                        testRow(oType, 11, 1, "b"),
                        testRow(oType, 60, 6, "c"),
                },
                adapter.newGroupCursor(oType.userTable().getGroup().getGroupTable())
        );
    }
}
