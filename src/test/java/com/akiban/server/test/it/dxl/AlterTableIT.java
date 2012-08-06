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

import com.akiban.ais.AISCloner;
import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.util.TableChange;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.util.OperatorBasedTableCopier;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.error.NotNullViolationException;
import com.akiban.server.service.dxl.DXLReadWriteLockHook;
import com.akiban.server.test.it.ITBase;
import com.akiban.server.test.it.qp.TestRow;
import com.akiban.server.types3.Types3Switch;
import com.akiban.sql.StandardException;
import com.akiban.sql.aisddl.AlterTableDDL;
import com.akiban.sql.parser.AlterTableNode;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.StatementNode;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AlterTableIT extends ITBase {
    private final String SCHEMA = "test";
    private int cid;
    private int oid;
    private int iid;

    private void runAlter(String sql) throws StandardException {
        SQLParser parser = new SQLParser();
        StatementNode node = parser.parseStatement(sql);
        assertTrue("is alter node", node instanceof AlterTableNode);
        OperatorBasedTableCopier copier = new OperatorBasedTableCopier(configService(), treeService(), session(), store(), Types3Switch.ON);
        AlterTableDDL.alterTable(DXLReadWriteLockHook.only(), ddl(), dml(), session(), copier, SCHEMA, (AlterTableNode)node);
        updateAISGeneration();
    }

    private RowBase testRow(RowType type, Object... fields) {
        return new TestRow(type, fields);
    }

    private void createAndLoadSingleTableGroup() {
        cid = createTable(SCHEMA, "c", "id int not null primary key, c1 char(5)");
        writeRows(
                createNewRow(cid, 1L, "10"),
                createNewRow(cid, 2L, "20"),
                createNewRow(cid, 3L, "30")
        );
    }

    private void createAndLoadCOI() {
        cid = createTable(SCHEMA, "c", "id int not null primary key, c1 char(1)");
        oid = createTable(SCHEMA, "o", "id int not null primary key, cid int, o1 int, grouping foreign key(cid) references c(id)");
        iid = createTable(SCHEMA, "i", "id int not null primary key, oid int, i1 int, grouping foreign key(oid) references o(id)");
        writeRows(
                createNewRow(cid, 1L, "a"),
                    createNewRow(oid, 10L, 1L, 11L),
                        createNewRow(iid, 100L, 10L, 110L),
                        createNewRow(iid, 101L, 10L, 111L),
                    createNewRow(oid, 11L, 1L, 12L),
                        createNewRow(iid, 111L, 11L, 122L),
                createNewRow(cid, 2L, "b"),
                // no 3L
                    createNewRow(oid, 30L, 3L, 33L),
                        createNewRow(iid, 300L, 30L, 330L)
        );
    }

    @Test
    public void cannotAddNotNullColumn() throws StandardException {
        createAndLoadSingleTableGroup();

        // Needs updated after default values are supported
        try {
            runAlter("ALTER TABLE c ADD COLUMN c2 INT NOT NULL DEFAULT 0");
            fail("Expected NotNullViolationException");
        } catch(NotNullViolationException e) {
            // Expected
        }

        // For now, check that the ALTER schema change was rolled back correctly
        updateAISGeneration();
        expectFullRows(
                cid,
                createNewRow(cid, 1L, "10"),
                createNewRow(cid, 2L, "20"),
                createNewRow(cid, 3L, "30")
        );
    }

    @Test
    public void addSingleColumnSingleTableGroup() throws StandardException {
        createAndLoadSingleTableGroup();
        runAlter("ALTER TABLE c ADD COLUMN c2 INT NULL");
        expectFullRows(
                cid,
                createNewRow(cid, 1L, "10", null),
                createNewRow(cid, 2L, "20", null),
                createNewRow(cid, 3L, "30", null)
        );
    }

    @Test
    public void addSingleColumnRootOfGroup() throws StandardException {
        createAndLoadCOI();
        runAlter("ALTER TABLE c ADD COLUMN c2 INT NULL");
        expectFullRows(
                cid,
                createNewRow(cid, 1L, "a", null),
                createNewRow(cid, 2L, "b", null)
        );
    }

    @Test
    public void addSingleColumnMiddleOfGroup() throws StandardException {
        createAndLoadCOI();
        runAlter("ALTER TABLE o ADD COLUMN o2 INT NULL");
        expectFullRows(
                oid,
                createNewRow(oid, 10L, 1L, 11L, null),
                createNewRow(oid, 11L, 1L, 12L, null),
                createNewRow(oid, 30L, 3L, 33L, null)
        );
    }

    @Test
    public void addSingleColumnLeafOfGroup() throws StandardException {
        createAndLoadCOI();
        runAlter("ALTER TABLE i ADD COLUMN i2 INT NULL");
        expectFullRows(
                iid,
                createNewRow(iid, 100L, 10L, 110L, null),
                createNewRow(iid, 101L, 10L, 111L, null),
                createNewRow(iid, 111L, 11L, 122L, null),
                createNewRow(iid, 300L, 30L, 330L, null)
        );
    }

    @Test
    public void dropSingleColumnSingleTableGroup() throws StandardException {
        createAndLoadSingleTableGroup();
        runAlter("ALTER TABLE c DROP COLUMN c1");
        expectFullRows(
                cid,
                createNewRow(cid, 1L),
                createNewRow(cid, 2L),
                createNewRow(cid, 3L)
        );
    }

    @Test
    public void dropSingleColumnRootOfGroup() throws StandardException {
        createAndLoadCOI();
        runAlter("ALTER TABLE c DROP COLUMN c1");
        expectFullRows(
                cid,
                createNewRow(cid, 1L),
                createNewRow(cid, 2L)
        );
    }

    @Test
    public void dropSingleColumnMiddleOfGroup() throws StandardException {
        createAndLoadCOI();
        runAlter("ALTER TABLE o DROP COLUMN o1");
        expectFullRows(
                oid,
                createNewRow(oid, 10L, 1L),
                createNewRow(oid, 11L, 1L),
                createNewRow(oid, 30L, 3L)
        );
    }

    @Test
    public void dropSingleColumnLeafOfGroup() throws StandardException {
        createAndLoadCOI();
        runAlter("ALTER TABLE i DROP COLUMN i1");
        expectFullRows(
                iid,
                createNewRow(iid, 100L, 10L),
                createNewRow(iid, 101L, 10L),
                createNewRow(iid, 111L, 11L),
                createNewRow(iid, 300L, 30L)
        );
    }

    @Test
    public void dropSingleColumnOfSingleColumnIndex() throws StandardException {
        createAndLoadSingleTableGroup();
        createIndex(SCHEMA, "c", "c1", "c1");

        UserTable origTable = getUserTable(cid);
        runAlter("ALTER TABLE c DROP COLUMN c1");

        expectIndexes(cid, "PRIMARY");
        assertEquals("index tree exists", false, treeService().treeExists(SCHEMA, origTable.getIndex("c1").getTreeName()));
        expectFullRows(
                cid,
                createNewRow(store(), cid,  1L),
                createNewRow(store(), cid,  2L),
                createNewRow(store(), cid,  3L)
        );
    }

    @Test
    public void dropSingleColumnOfMultiColumnIndex() throws StandardException {
        int cid = createTable(SCHEMA, "c", "id int not null primary key, c1 int, c2 int");
        createIndex(SCHEMA, "c", "c1_c2", "c1", "c2");
        writeRows(
                createNewRow(cid,  1L,  11L,  12L),
                createNewRow(cid,  2L,  21L,  22L),
                createNewRow(cid,  3L,  31L,  32L),
                createNewRow(cid, 10L, 101L, 102L)
        );

        runAlter("ALTER TABLE c DROP COLUMN c1");

        expectRows(
                scanAllIndexRequest(getUserTable(SCHEMA, "c").getIndex("c1_c2")),
                createNewRow(store(), cid, UNDEF, 12L),
                createNewRow(store(), cid, UNDEF, 22L),
                createNewRow(store(), cid, UNDEF, 32L),
                createNewRow(store(), cid, UNDEF, 102L)
        );
    }

    @Test
    public void dropSingleColumnOfMultiColumnGroupIndex() throws StandardException {
        createAndLoadCOI();
        createGroupIndex("c", "c1_o1_o2", "c.c1,o.o1,i.i1");

        runAlter("ALTER TABLE o DROP COLUMN o1");

        AkibanInformationSchema ais = ddl().getAIS(session());
        Index index = ais.getGroup("c").getIndex("c1_o1_o2");
        assertNotNull("Index still exists", index);
        assertEquals("Index column count", 2, index.getKeyColumns().size());

        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        IndexRowType indexRowType = schema.indexRowType(index);

        StoreAdapter adapter = new PersistitAdapter(schema, store(), treeService(), session(), configService());
        compareRows(
                new RowBase[] {
                        testRow(indexRowType, "a", 110L, 1L, 10L, 100L),
                        testRow(indexRowType, "a", 111L, 1L, 10L, 101L),
                        testRow(indexRowType, "a", 122L, 1L, 11L, 111L),
                },
                API.cursor(
                        API.indexScan_Default(indexRowType, false, IndexKeyRange.unbounded(indexRowType)),
                        new SimpleQueryContext(adapter)
                )
        );
    }

    @Test
    public void changeDataTypeSingleColumnSingleTableGroup() throws StandardException {
        createAndLoadSingleTableGroup();
        runAlter("ALTER TABLE c ALTER COLUMN c1 SET DATA TYPE int");
        expectFullRows(
                cid,
                createNewRow(cid, 1L, 10L),
                createNewRow(cid, 2L, 20L),
                createNewRow(cid, 3L, 30L)
        );
    }

    @Test
    public void addDropAndAlterColumnSingleTableGroup() throws StandardException {
        int cid = createTable(SCHEMA, "c", "c1 int not null primary key, c2 char(5), c3 int, c4 char(1)");
        writeRows(
                createNewRow(cid, 1L, "one", 10, "A"),
                createNewRow(cid, 2L, "two", 20, "B"),
                createNewRow(cid, 3L, "three", 30, "C"),
                createNewRow(cid, 10L, "ten", 100, "D")
        );

        // Our parser doesn't (yet) support multi-action alters, manually build parameters
        // ALTER TABLE c ADD COLUMN c5 INT, DROP COLUMN c2, ALTER COLUMN c3 SET DATA TYPE char(3)
        AISBuilder builder = new AISBuilder();
        builder.userTable(SCHEMA, "c");
        builder.column(SCHEMA, "c", "c1", 0, "int", null, null, false, false, null, null);
        builder.column(SCHEMA, "c", "c3", 1, "char", 3L, null, true, false, null, null);
        builder.column(SCHEMA, "c", "c4", 2, "char", 1L, null, true, false, null, null);
        builder.column(SCHEMA, "c", "c5", 3, "int", null, null, true, false, null, null);
        builder.index(SCHEMA, "c", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn(SCHEMA, "c", Index.PRIMARY_KEY_CONSTRAINT, "c1", 0, true, null);
        builder.basicSchemaIsComplete();
        UserTable newTable = builder.akibanInformationSchema().getUserTable(SCHEMA, "c");

        List<TableChange> changes = new ArrayList<TableChange>();
        changes.add(TableChange.createAdd("c5"));
        changes.add(TableChange.createDrop("c2"));
        changes.add(TableChange.createModify("c3", "c3"));

        ddl().alterTable(session(), new TableName(SCHEMA, "c"), newTable, changes, Collections.<TableChange>emptyList());
        updateAISGeneration();

        expectFullRows(
                cid,
                createNewRow(cid, 1L, "10", "A", null),
                createNewRow(cid, 2L, "20", "B", null),
                createNewRow(cid, 3L, "30", "C", null),
                createNewRow(cid, 10L, "100", "D", null)
        );
    }

    @Test
    public void addUniqueKeyExistingColumn() throws StandardException {
        createAndLoadSingleTableGroup();
        runAlter("ALTER TABLE c ADD UNIQUE(c1)");
        expectIndexes(cid, "PRIMARY", "c1");
        expectRows(
                scanAllIndexRequest(getUserTable(SCHEMA, "c").getIndex("c1")),
                createNewRow(store(), cid, UNDEF, "10"),
                createNewRow(store(), cid, UNDEF, "20"),
                createNewRow(store(), cid, UNDEF, "30")
        );
    }

    @Test
    public void dropUniqueAddIndexSameName() {
        cid = createTable(SCHEMA, "c", "id int not null primary key, c1 char(1), c2 char(1), constraint foo unique(c1)");
        writeRows(
                createNewRow(cid, 1L, "A", "3"),
                createNewRow(cid, 2L, "B", "2"),
                createNewRow(cid, 3L, "C", "1")
        );

        AkibanInformationSchema ais = AISCloner.clone(ddl().getAIS(session()));
        UserTable table = ais.getUserTable(SCHEMA, "c");
        table.removeIndexes(Collections.singleton(table.getIndex("foo")));
        AISBuilder builder = new AISBuilder(ais);
        builder.index(SCHEMA, "c", "foo", false, Index.KEY_CONSTRAINT);
        builder.indexColumn(SCHEMA, "c", "foo", "c2", 0, true, null);

        List<TableChange> changes = new ArrayList<TableChange>();
        changes.add(TableChange.createDrop("foo"));
        changes.add(TableChange.createAdd("foo"));

        ddl().alterTable(session(), new TableName(SCHEMA, "c"), table, Collections.<TableChange>emptyList(), changes);
        updateAISGeneration();

        expectIndexes(cid, "foo", "PRIMARY");
        expectRows(
                scanAllIndexRequest(getUserTable(SCHEMA, "c").getIndex("foo")),
                createNewRow(store(), cid, UNDEF, UNDEF, "1"),
                createNewRow(store(), cid, UNDEF, UNDEF, "2"),
                createNewRow(store(), cid, UNDEF, UNDEF, "3")
        );
    }

    @Test
    public void modifyIndex() {
        cid = createTable(SCHEMA, "c", "id int not null primary key, c1 char(1), c2 char(1)");
        createIndex(SCHEMA, "c", "foo", "c1");
        writeRows(
                createNewRow(cid, 1L, "A", "3"),
                createNewRow(cid, 2L, "B", "2"),
                createNewRow(cid, 3L, "C", "1")
        );

        AkibanInformationSchema ais = AISCloner.clone(ddl().getAIS(session()));
        UserTable table = ais.getUserTable(SCHEMA, "c");
        table.removeIndexes(Collections.singleton(table.getIndex("foo")));
        AISBuilder builder = new AISBuilder(ais);
        builder.index(SCHEMA, "c", "foo", false, Index.KEY_CONSTRAINT);
        builder.indexColumn(SCHEMA, "c", "foo", "c2", 0, true, null);
        builder.indexColumn(SCHEMA, "c", "foo", "c1", 1, true, null);

        List<TableChange> changes = new ArrayList<TableChange>();
        changes.add(TableChange.createModify("foo", "foo"));

        ddl().alterTable(session(), new TableName(SCHEMA, "c"), table, Collections.<TableChange>emptyList(), changes);
        updateAISGeneration();

        expectIndexes(cid, "foo", "PRIMARY");
        expectRows(
                scanAllIndexRequest(getUserTable(SCHEMA, "c").getIndex("foo")),
                createNewRow(store(), cid, UNDEF, "C", "1"),
                createNewRow(store(), cid, UNDEF, "B", "2"),
                createNewRow(store(), cid, UNDEF, "A", "3")
        );
    }

    @Test
     public void addColumnDropColumnAddIndexOldNewMiddleOfGroup() throws StandardException {
        createAndLoadCOI();

        // ALTER TABLE o DROP COLUMN o1, ADD COLUMN o1 INT, ADD INDEX x(o1), ADD INDEX y(cid)
        AkibanInformationSchema ais = AISCloner.clone(ddl().getAIS(session()));
        UserTable table = ais.getUserTable(SCHEMA, "o");
        table.dropColumn("o1");
        AISBuilder builder = new AISBuilder(ais);
        builder.column(SCHEMA, "o", "o1", 2, "int", null, null, true, false, null, null);
        builder.index(SCHEMA, "o", "x", false, Index.KEY_CONSTRAINT);
        builder.indexColumn(SCHEMA, "o", "x", "o1", 0, true, null);
        builder.index(SCHEMA, "o", "y", false, Index.KEY_CONSTRAINT);
        builder.indexColumn(SCHEMA, "o", "y", "cid", 0, true, null);

        List<TableChange> columnChanges = new ArrayList<TableChange>();
        columnChanges.add(TableChange.createDrop("o1"));
        columnChanges.add(TableChange.createAdd("o1"));
        List<TableChange> indexChanges = new ArrayList<TableChange>();
        indexChanges.add(TableChange.createAdd("x"));
        indexChanges.add(TableChange.createAdd("y"));

        ddl().alterTable(session(), new TableName(SCHEMA, "o"), table, columnChanges, indexChanges);
        updateAISGeneration();

        expectFullRows(
                oid,
                createNewRow(oid, 10L, 1L, null),
                createNewRow(oid, 11L, 1L, null),
                createNewRow(oid, 30L, 3L, null)
        );

        expectIndexes(oid, "PRIMARY", "x", "y");

        expectRows(
                scanAllIndexRequest(getUserTable(SCHEMA, "o").getIndex("x")),
                createNewRow(store(), oid, UNDEF, UNDEF, null),
                createNewRow(store(), oid, UNDEF, UNDEF, null),
                createNewRow(store(), oid, UNDEF, UNDEF, null)
        );

        expectRows(
                scanAllIndexRequest(getUserTable(SCHEMA, "o").getIndex("y")),
                createNewRow(store(), oid, UNDEF, 1L, UNDEF),
                createNewRow(store(), oid, UNDEF, 1L, UNDEF),
                createNewRow(store(), oid, UNDEF, 3L, UNDEF)
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
