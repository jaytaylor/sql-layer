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
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
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
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.error.InvalidAlterException;
import com.akiban.server.error.NotNullViolationException;
import com.akiban.sql.StandardException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.akiban.ais.util.TableChangeValidator.ChangeLevel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class AlterTableBasicIT extends AlterTableITBase {
    private static final Logger LOG = LoggerFactory.getLogger(AlterTableBasicIT.class.getName());

    private int cid;
    private int oid;
    private int iid;

    private void createAndLoadSingleTableGroup() {
        cid = createTable(SCHEMA, "c", "id int not null primary key, c1 char(5)");
        writeRows(
                createNewRow(cid, 1L, "10"),
                createNewRow(cid, 2L, "20"),
                createNewRow(cid, 3L, "30")
        );
    }

    private void createAndLoadCOI() {
        createAndLoadCOI(SCHEMA);
    }

    private void createAndLoadCOI(String schema) {
        cid = createTable(schema, "c", "id int not null primary key, c1 char(1)");
        oid = createTable(schema, "o", "id int not null primary key, cid int, o1 int, grouping foreign key(cid) references c(id)");
        iid = createTable(schema, "i", "id int not null primary key, oid int, i1 int, grouping foreign key(oid) references o(id)");
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

    private IndexRowType indexRowType(Index index) {
        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        return schema.indexRowType(index);
    }

    private void scanAndCheckIndex(IndexRowType type, RowBase... expectedRows) {
        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        StoreAdapter adapter = new PersistitAdapter(schema, store(), treeService(), session(), configService());
        compareRows(
                expectedRows,
                API.cursor(
                        API.indexScan_Default(type, false, IndexKeyRange.unbounded(type)),
                        new SimpleQueryContext(adapter)
                )
        );
    }



    @Test(expected=InvalidAlterException.class)
    public void unspecifiedColumnChange() {
        NewAISBuilder builder = AISBBasedBuilder.create();
        builder.userTable(SCHEMA, "c").colLong("c1").pk("c1");
        UserTable table = builder.ais().getUserTable(SCHEMA, "c");

        ddl().createTable(session(),  table);
        updateAISGeneration();

        builder = AISBBasedBuilder.create();
        builder.userTable(SCHEMA, "c").colLong("c1").colLong("c2").colLong("c3").pk("c1");
        table = builder.ais().getUserTable(SCHEMA, "c");

        ddl().alterTable(session(), table.getName(), table,
                         Arrays.asList(TableChange.createAdd("c2")), NO_CHANGES,
                         null);
    }

    @Test
    public void dropSingleColumnFromMultiColumnPK() throws StandardException {
        cid = createTable(SCHEMA, "c", "c1 int not null, c2 char(1), c3 int not null, primary key(c1,c3)");
        writeRows(
                createNewRow(cid, 1L, "A", 50L),
                createNewRow(cid, 2L, "B", 20L),
                createNewRow(cid, 5L, "C", 10L)
        );
        runAlter(ChangeLevel.GROUP, "ALTER TABLE c DROP COLUMN c1");
        expectFullRows(
                cid,
                createNewRow(cid, "C", 10L),
                createNewRow(cid, "B", 20L),
                createNewRow(cid, "A", 50L)
        );
        expectRows(
                scanAllIndexRequest(getUserTable(SCHEMA, "c").getIndex(Index.PRIMARY_KEY_CONSTRAINT)),
                createNewRow(store(), cid, UNDEF, 10L),
                createNewRow(store(), cid, UNDEF, 20L),
                createNewRow(store(), cid, UNDEF, 50L)
        );
    }

    @Test
    public void dropPrimaryKeyMiddleOfGroup() throws StandardException {
        createAndLoadCOI();

        // Will yield 2 groups: C-O and I
        runAlter(ChangeLevel.GROUP, "ALTER TABLE o DROP PRIMARY KEY");

        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        RowType cType = schema.userTableRowType(getUserTable(SCHEMA, "c"));
        RowType oType = schema.userTableRowType(getUserTable(SCHEMA, "o"));
        RowType iType = schema.userTableRowType(getUserTable(SCHEMA, "i"));
        StoreAdapter adapter = new PersistitAdapter(schema, store(), treeService(), session(), configService());
        long pk = 1;
        compareRows(
                new RowBase[]{
                        testRow(cType, 1L, "a"),
                        testRow(oType, 10L, 1L, 11L, pk++),
                        testRow(oType, 11L, 1L, 12L, pk++),
                        testRow(cType, 2L, "b"),
                        testRow(oType, 30L, 3L, 33L, pk++),
                },
                adapter.newGroupCursor(cType.userTable().getGroup())
        );
        compareRows(
                new RowBase[]{
                        testRow(iType, 100L, 10L, 110L),
                        testRow(iType, 101L, 10L, 111L),
                        testRow(iType, 111L, 11L, 122L),
                        testRow(iType, 300L, 30L, 330L)
                },
                adapter.newGroupCursor(iType.userTable().getGroup())
        );
    }

    @Test
    public void cannotAddNotNullWithNoDefault() throws StandardException {
        createAndLoadSingleTableGroup();

        try {
            runAlter("ALTER TABLE c ADD COLUMN c2 INT NOT NULL DEFAULT NULL");
            fail("Expected NotNullViolationException");
        } catch(NotNullViolationException e) {
            // Expected
        }

        // Check that schema change was rolled back correctly
        updateAISGeneration();
        expectFullRows(
                cid,
                createNewRow(cid, 1L, "10"),
                createNewRow(cid, 2L, "20"),
                createNewRow(cid, 3L, "30")
        );
    }

    @Test
    public void addNotNullColumnDefault() throws StandardException {
        createAndLoadSingleTableGroup();
        runAlter("ALTER TABLE c ADD COLUMN c2 INT NOT NULL DEFAULT 0");
        expectFullRows(
                cid,
                createNewRow(cid, 1L, "10", 0L),
                createNewRow(cid, 2L, "20", 0L),
                createNewRow(cid, 3L, "30", 0L)
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
    public void addColumnIndexSingleTableNoPrimaryKey() throws StandardException {
        TableName cName = tableName(SCHEMA, "c");
        NewAISBuilder builder = AISBBasedBuilder.create();
        builder.userTable(cName).colLong("c1", true).colLong("c2", true).colLong("c3", true);

        ddl().createTable(session(), builder.unvalidatedAIS().getUserTable(cName));
        updateAISGeneration();

        // Note: Not using standard id due to null index contents
        int tableId = tableId(cName);
        writeRows(
                createNewRow(tableId, 1, 2, 3),
                createNewRow(tableId, 4, 5, 6),
                createNewRow(tableId, 7, 8, 9)
        );

        builder = AISBBasedBuilder.create();
        builder.userTable(cName).colLong("c1", true).colLong("c2", true).colLong("c3", true).colLong("c4", true).key("c4", "c4");
        List<TableChange> changes = new ArrayList<TableChange>();
        changes.add(TableChange.createAdd("c4"));

        ddl().alterTable(session(), cName, builder.unvalidatedAIS().getUserTable(cName), changes, changes, queryContext());
        updateAISGeneration();

        expectFullRows(
                tableId,
                createNewRow(tableId, 1L, 2L, 3L, null),
                createNewRow(tableId, 4L, 5L, 6L, null),
                createNewRow(tableId, 7L, 8L, 9L, null)
        );

        expectRows(
                scanAllIndexRequest(getUserTable(tableId).getIndex("c4")),
                createNewRow(store(), tableId, UNDEF, UNDEF, UNDEF, null),
                createNewRow(store(), tableId, UNDEF, UNDEF, UNDEF, null),
                createNewRow(store(), tableId, UNDEF, UNDEF, UNDEF, null)
        );

        ddl().dropTable(session(), cName);
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
        cid = createTable(SCHEMA, "c", "id int not null primary key, c1 int, c2 int");
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
    public void dropGroupingForeignKeyTableInGroupIndex() throws StandardException {
        createAndLoadCOI();
        createGroupIndex("c", "c1_o1_i1", "c.c1,o.o1,i.i1");

        runAlter(ChangeLevel.GROUP, "ALTER TABLE o DROP GROUPING FOREIGN KEY");

        AkibanInformationSchema ais = ddl().getAIS(session());
        Index index = ais.getGroup("c").getIndex("c1_o1_i1");
        assertNull("Index should not exist on c group", index);
        index = ais.getGroup("o").getIndex("c1_o1_i1");
        assertNull("Index should not exist on o group", index);
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
    public void changeDataTypeSingleColumnInIndex() throws StandardException {
        createAndLoadSingleTableGroup();
        createIndex(SCHEMA, "c", "c1", "c1");
        runAlter("ALTER TABLE c ALTER COLUMN c1 SET DATA TYPE int");
        expectFullRows(
                cid,
                createNewRow(cid, 1L, 10L),
                createNewRow(cid, 2L, 20L),
                createNewRow(cid, 3L, 30L)
        );
        expectRows(
                scanAllIndexRequest(getUserTable(SCHEMA, "c").getIndex("c1")),
                createNewRow(store(), cid, UNDEF, 10L),
                createNewRow(store(), cid, UNDEF, 20L),
                createNewRow(store(), cid, UNDEF, 30L)
        );
    }

    @Test
    public void addDropAndAlterColumnSingleTableGroup() throws StandardException {
        cid = createTable(SCHEMA, "c", "c1 int not null primary key, c2 char(5), c3 int, c4 char(1)");
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
        builder.createGroup("c", SCHEMA);
        builder.addTableToGroup("c", SCHEMA, "c");
        builder.groupingIsComplete();
        UserTable newTable = builder.akibanInformationSchema().getUserTable(SCHEMA, "c");

        List<TableChange> changes = new ArrayList<TableChange>();
        changes.add(TableChange.createAdd("c5"));
        changes.add(TableChange.createDrop("c2"));
        changes.add(TableChange.createModify("c3", "c3"));

        ddl().alterTable(session(), new TableName(SCHEMA, "c"), newTable, changes, NO_CHANGES, queryContext());
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
        runAlter(ChangeLevel.INDEX, "ALTER TABLE c ADD UNIQUE(c1)");
        expectIndexes(cid, "PRIMARY", "c1");
        expectRows(
                scanAllIndexRequest(getUserTable(SCHEMA, "c").getIndex("c1")),
                createNewRow(store(), cid, UNDEF, "10"),
                createNewRow(store(), cid, UNDEF, "20"),
                createNewRow(store(), cid, UNDEF, "30")
        );
    }

    @Test
    public void alterColumnNotNullToNull() throws StandardException {
        cid = createTable(SCHEMA, "c", "id int not null primary key, c1 char(5) not null");
        writeRows(
                createNewRow(cid, 1L, "10"),
                createNewRow(cid, 2L, "20"),
                createNewRow(cid, 3L, "30")
        );
        runAlter(ChangeLevel.METADATA, "ALTER TABLE c ALTER COLUMN c1 NULL");
        // Just check metadata
        // Insert needs more plumbing (e.g. Insert_Default), checked in test-alter-nullability.yaml
        UserTable table = getUserTable(SCHEMA, "c");
        assertEquals("c1 nullable", true, table.getColumn("c1").getNullable());
    }

    @Test
    public void alterColumnNullToNotNull() throws StandardException {
        cid = createTable(SCHEMA, "c", "id int not null primary key, c1 char(5) null");
        writeRows(
                createNewRow(cid, 1L, "10"),
                createNewRow(cid, 2L, "20"),
                createNewRow(cid, 3L, "30")
        );
        runAlter(ChangeLevel.METADATA_NOT_NULL, "ALTER TABLE c ALTER COLUMN c1 NOT NULL");
        // Just check metadata
        // Insert needs more plumbing (e.g. Insert_Default), checked in test-alter-nullability.yaml
        UserTable table = getUserTable(SCHEMA, "c");
        assertEquals("c1 nullable", false, table.getColumn("c1").getNullable());
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

        ddl().alterTable(session(), new TableName(SCHEMA, "c"), table, NO_CHANGES, changes, queryContext());
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

        ddl().alterTable(session(), new TableName(SCHEMA, "c"), table, NO_CHANGES, changes, queryContext());
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

        ddl().alterTable(session(), new TableName(SCHEMA, "o"), table, columnChanges, indexChanges, queryContext());
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
    public void addGroupingForeignSingleToTwoTableGroup() throws StandardException {
        cid = createTable(SCHEMA, "c", "id int not null primary key, v varchar(32)");
        oid = createTable(SCHEMA, "o", "id int not null primary key, cid int, tag char(1), grouping foreign key(cid) references c(id)");
        iid = createTable(SCHEMA, "i", "id int not null primary key, spare_id int, tag2 char(1)");

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

        runAlter(ChangeLevel.GROUP, "ALTER TABLE i ADD GROUPING FOREIGN KEY(spare_id) REFERENCES o(id)");

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
                adapter.newGroupCursor(cType.userTable().getGroup())
        );
    }

    @Test
    public void simpleDropGroupingForeignKey() throws StandardException {
        cid = createTable(SCHEMA, "c", "id int not null primary key, v varchar(32)");
        oid = createTable(SCHEMA, "o", "id int not null primary key, cid int, tag char(1), grouping foreign key(cid) references c(id)");

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

        runAlter(ChangeLevel.GROUP, "ALTER TABLE o DROP GROUPING FOREIGN KEY");

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
                adapter.newGroupCursor(cType.userTable().getGroup())
        );
        compareRows(
                new RowBase[] {
                        testRow(oType, 10, 1, "a"),
                        testRow(oType, 11, 1, "b"),
                        testRow(oType, 60, 6, "c"),
                },
                adapter.newGroupCursor(oType.userTable().getGroup())
        );
    }

    @Test
    public void addGroupingForeignKeyToExistingParent() throws StandardException {
        cid = createTable(SCHEMA, "c", "id int not null primary key, v varchar(32)");
        oid = createTable(SCHEMA, "o", "id int not null primary key, cid int, tag char(1)");
        iid = createTable(SCHEMA, "i", "id int not null primary key, spare_id int, tag2 char(1), grouping foreign key(spare_id) references o(id)");

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

        runAlter(ChangeLevel.GROUP, "ALTER TABLE o ADD GROUPING FOREIGN KEY(cid) REFERENCES c(id)");

        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        RowType cType = schema.userTableRowType(getUserTable(SCHEMA, "c"));
        RowType oType = schema.userTableRowType(getUserTable(SCHEMA, "o"));
        RowType iType = schema.userTableRowType(getUserTable(SCHEMA, "i"));

        StoreAdapter adapter = new PersistitAdapter(schema, store(), treeService(), session(), configService());
        compareRows(
                new RowBase[] {
                        // ?
                            // null
                                testRow(iType, 200, 20, "d"),
                        testRow(cType, 1, "asdf"),
                            testRow(oType, 10, 1, "a"),
                                testRow(iType, 100, 10, "d"),
                                testRow(iType, 101, 10, "e"),
                                testRow(iType, 102, 10, "f"),
                            testRow(oType, 11, 1, "b"),
                        testRow(cType, 5, "qwer"),
                        // null
                            testRow(oType, 60, 6, "c"),
                        testRow(cType, 10, "zxcv"),
                },
                adapter.newGroupCursor(cType.userTable().getGroup())
        );
    }

    @Test
    public void dropGroupingForeignKeyMiddleOfGroup() throws StandardException {
        cid = createTable(SCHEMA, "c", "id int not null primary key, v varchar(32)");
        oid = createTable(SCHEMA, "o", "id int not null primary key, cid int, tag char(1), grouping foreign key(cid) references c(id)");
        iid = createTable(SCHEMA, "i", "id int not null primary key, spare_id int, tag2 char(1), grouping foreign key(spare_id) references o(id)");

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

        runAlter(ChangeLevel.GROUP, "ALTER TABLE o DROP GROUPING FOREIGN KEY");

        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        RowType cType = schema.userTableRowType(getUserTable(SCHEMA, "c"));
        RowType oType = schema.userTableRowType(getUserTable(SCHEMA, "o"));
        RowType iType = schema.userTableRowType(getUserTable(SCHEMA, "i"));

        StoreAdapter adapter = new PersistitAdapter(schema, store(), treeService(), session(), configService());
        compareRows(
                new RowBase[] {
                        testRow(cType, 1, "asdf"),
                        testRow(cType, 5, "qwer"),
                        testRow(cType, 10, "zxcv")
                },
                adapter.newGroupCursor(cType.userTable().getGroup())
        );

        compareRows(
                new RowBase[] {
                        testRow(oType, 10, 1, "a"),
                            testRow(iType, 100, 10, "d"),
                            testRow(iType, 101, 10, "e"),
                            testRow(iType, 102, 10, "f"),
                        testRow(oType, 11, 1, "b"),
                        // none
                            testRow(iType, 200, 20, "d"),
                        testRow(oType, 60, 6, "c"),
                },
                adapter.newGroupCursor(oType.userTable().getGroup())
        );
    }

    // bug1037308, part 1
    @Test
    public void dropColumnConflatedPKFKOnLeaf() {
        createTable(
                SCHEMA, "customers",
                "cid INT NOT NULL PRIMARY KEY",
                "name VARCHAR(32) NOT NULL"
        );
        createTable(
                SCHEMA, "orders",
                "oid INT NOT NULL PRIMARY KEY",
                "cid INT NOT NULL",
                "GROUPING FOREIGN KEY(cid) REFERENCES customers(cid)",
                "order_date DATE NOT NULL"
        );
        createTable(
                SCHEMA, "items",
                "iid INT NOT NULL PRIMARY KEY",
                "oid INT NOT NULL",
                "GROUPING FOREIGN KEY(oid) REFERENCES orders(oid)",
                "sku VARCHAR(32) NOT NULL",
                "quan INT NOT NULL"
        );
        createTable(
                SCHEMA, "item_details",
                "iid INT NOT NULL PRIMARY KEY",
                "GROUPING FOREIGN KEY(iid) REFERENCES items(iid)",
                "details VARCHAR(1024)"
        );
        // Hit assert in sort index size validation
        runAlter(ChangeLevel.GROUP, "ALTER TABLE item_details DROP COLUMN iid");
    }

    // bug1037308, part 2
    @Test
    public void dropColumnCascadingKeyFromMiddleOfGroup() {
        createTable(
                SCHEMA, "customers",
                "cid INT NOT NULL PRIMARY KEY",
                "name VARCHAR(32) NOT NULL"
        );
        createTable(
                SCHEMA, "orders",
                "oid INT NOT NULL",
                "cid INT NOT NULL",
                "PRIMARY KEY(cid, oid)",
                "GROUPING FOREIGN KEY(cid) REFERENCES customers(cid)",
                "order_date DATE NOT NULL"
        );
        createTable(
                SCHEMA, "items",
                "iid INT NOT NULL",
                "oid INT NOT NULL",
                "cid INT NOT NULL",
                "PRIMARY KEY(cid, oid, iid)",
                "GROUPING FOREIGN KEY(cid,oid) REFERENCES orders(cid,oid)",
                "sku VARCHAR(32) NOT NULL",
                "quan INT NOT NULL"
        );
        // Hit assert in index size validation
        runAlter(ChangeLevel.GROUP, "ALTER TABLE orders DROP COLUMN cid");
    }

    // bug1037387
    @Test
    public void alterTableWithDefaults() {
        createTable(
                SCHEMA, C_TABLE,
                "cid int not null generated by default as identity primary key",
                "c1 varchar(32) default 'bob'",
                "c2 int default 42",
                "c3 decimal(5,2) default 0.0",
                "c4 char(1) default 'N'"
        );
        LOG.warn("Expecting message for NO_CHANGE alter:");
        // First example of the failure in the bug
        runAlter(ChangeLevel.NONE, "ALTER TABLE c ALTER COLUMN cid NOT NULL");
        // Exception from validator due to defaults incorrectly changing
        runAlter("ALTER TABLE c ADD family_size int");
        UserTable table = getUserTable(C_NAME);
        assertEquals("cid default identity", true, table.getColumn("cid").getDefaultIdentity());
        assertEquals("c1 default", "bob", table.getColumn("c1").getDefaultValue());
        assertEquals("c2 default", "42", table.getColumn("c2").getDefaultValue());
        assertEquals("c3 default", "0.0", table.getColumn("c3").getDefaultValue());
        assertEquals("c4 default", "N", table.getColumn("c4").getDefaultValue());
        assertEquals("family_size default", null, table.getColumn("family_size").getDefaultValue());
    }

    // bug1037387
    @Test
    public void modifyColumnPosition() {
        createTable(SCHEMA, C_TABLE, "c1 int not null primary key, c2 int");
        createIndex(SCHEMA, C_TABLE, "c2", "c2");

        int cid = tableId(C_NAME);
        writeRows(
                createNewRow(cid, 1, 10),
                createNewRow(cid, 2, 20),
                createNewRow(cid, 3, 30)
        );

        AISBuilder builder = new AISBuilder();
        builder.userTable(SCHEMA, C_TABLE);
        builder.column(SCHEMA, C_TABLE, "c2", 0, "int", null, null, true, false, null, null);
        builder.column(SCHEMA, C_TABLE, "c1", 1, "int", null, null, false, false, null, null);
        builder.index(SCHEMA, C_TABLE, Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn(SCHEMA, C_TABLE, Index.PRIMARY_KEY_CONSTRAINT, "c1", 0, true, null);
        builder.index(SCHEMA, C_TABLE, "c2", true, Index.KEY_CONSTRAINT);
        builder.indexColumn(SCHEMA, C_TABLE, "c2", "c2", 0, true, null);
        builder.basicSchemaIsComplete();
        builder.createGroup(C_TABLE, SCHEMA);
        builder.addTableToGroup(C_TABLE, SCHEMA, C_TABLE);
        builder.groupingIsComplete();

        runAlter(ChangeLevel.TABLE,
                 C_NAME, builder.akibanInformationSchema().getUserTable(C_NAME),
                 Arrays.asList(TableChange.createModify("c1", "c1"), TableChange.createModify("c2", "c2")),
                 NO_CHANGES);

        expectFullRows(
                cid,
                createNewRow(cid, 10L, 1L),
                createNewRow(cid, 20L, 2L),
                createNewRow(cid, 30L, 3L)
        );

        // Let base class check index contents
        checkIndexesInstead(C_NAME, "PRIMARY", "c2");
    }

    // bug1038212
    @Test
    public void extendVarcharColumnWithIndex() {
        cid = createTable(SCHEMA, C_TABLE, "id int not null primary key, state varchar(2)");
        createIndex(SCHEMA, C_TABLE, "state", "state");
        NewRow[] rows = {
                createNewRow(cid, 1L, "AZ"),
                createNewRow(cid, 2L, "NY"),
                createNewRow(cid, 3L, "MA"),
                createNewRow(cid, 4L, "WA")
        };
        writeRows(rows);
        runAlter(ChangeLevel.TABLE, "ALTER TABLE c ALTER COLUMN state SET DATA TYPE varchar(3)");
        expectFullRows(cid, rows);
        checkIndexesInstead(C_NAME, "PRIMARY", "state");
    }

    // bug1040347
    @Test
    public void changeDataTypeDropsSequence() {
        final int id = createTable(SCHEMA, C_TABLE,
                                   "id INT NOT NULL PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY",
                                   "name VARCHAR(255) NOT NULL");
        Sequence seq = getUserTable(id).getColumn("id").getIdentityGenerator();
        assertNotNull("id column has sequence", seq);
        writeRows(
                createNewRow(id, 1, "1"),
                createNewRow(id, 2, "2"),
                createNewRow(id, 3, "3")
        );
        runAlter(ChangeLevel.GROUP, "ALTER TABLE c ALTER COLUMN id SET DATA TYPE varchar(10)");
        assertNull("sequence was dropped", ddl().getAIS(session()).getSequence(seq.getSequenceName()));
        assertNull("id column has no sequence", getUserTable(id).getColumn("id").getIdentityGenerator());
        expectFullRows(
                id,
                createNewRow(id, "1", "1"),
                createNewRow(id, "2", "2"),
                createNewRow(id, "3", "3")
        );
        checkIndexesInstead(C_NAME, "PRIMARY");
    }

    // bug1040347
    @Test
    public void dropColumnDropsSequence() {
        final int id = createTable(SCHEMA, C_TABLE,
                                   "id INT NOT NULL PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY",
                                   "name VARCHAR(255) NOT NULL");
        Sequence seq = getUserTable(id).getColumn("id").getIdentityGenerator();
        assertNotNull("id column has sequence", seq);
        writeRows(
                createNewRow(id, 1, "1"),
                createNewRow(id, 2, "2"),
                createNewRow(id, 3, "3")
        );
        runAlter(ChangeLevel.GROUP, "ALTER TABLE c DROP COLUMN id");
        assertNull("sequence was dropped", ddl().getAIS(session()).getSequence(seq.getSequenceName()));
        assertNull("id column does not exist", getUserTable(id).getColumn("id"));
        expectFullRows(
                id,
                createNewRow(id, "1"),
                createNewRow(id, "2"),
                createNewRow(id, "3")
        );
        checkIndexesInstead(C_NAME);
    }

    // bug1046793
    @Test
    public void dropColumnAutoDropsGroupIndex() {
        createAndLoadCOI();
        createGroupIndex("c", "c1_01", "c.c1,o.o1", Index.JoinType.LEFT);
        runAlter(ChangeLevel.TABLE, "ALTER TABLE o DROP COLUMN o1");
        assertEquals("Remaining group indexes", "[]", ddl().getAIS(session()).getGroup("c").getIndexes().toString());
        checkIndexesInstead(C_NAME, "PRIMARY");
        checkIndexesInstead(O_NAME, "PRIMARY");
        checkIndexesInstead(I_NAME, "PRIMARY");
    }

    // bug1047090
    @Test
    public void changeColumnAffectGroupIndexMultiRootTablesSameName() {
        final String schema1 = "test1";
        final String schema2 = "test2";
        createAndLoadCOI(schema1);
        createAndLoadCOI(schema2);
        String groupName = getUserTable(schema2, "c").getGroup().getName();
        createGroupIndex(groupName, "c1_01", "c.c1,o.o1", Index.JoinType.LEFT);

        runAlter(ChangeLevel.TABLE, "ALTER TABLE test2.o ALTER COLUMN o1 SET DATA TYPE bigint");

        Index gi = getUserTable(schema2, "c").getGroup().getIndex("c1_01");
        assertNotNull("GI still exists", gi);

        IndexRowType type = indexRowType(gi);
        scanAndCheckIndex(type,
                testRow(type, "a", 11L, 1L, 10L),
                testRow(type, "a", 12L, 1L, 11L)
        );
    }

    public void changeColumnInGICommon(String table, Runnable alterRunnable) {
        String giName = "c1_o1_i1";
        createAndLoadCOI();
        String groupName = getUserTable(SCHEMA, table).getGroup().getName();
        createGroupIndex(groupName, giName, "c.c1,o.o1,i.i1", Index.JoinType.LEFT);

        alterRunnable.run();

        Index gi = getUserTable(SCHEMA, table).getGroup().getIndex(giName);
        assertNotNull("GI still exists", gi);

        IndexRowType type = indexRowType(gi);
        scanAndCheckIndex(type,
                          testRow(type, "a", 11L, 110L, 1L, 10L, 100L),
                          testRow(type, "a", 11L, 111L, 1L, 10L, 101L),
                          testRow(type, "a", 12L, 122L, 1L, 11L, 111L)
        );
    }

    public void changeColumnTypeInGI(final String table, final String column, final String newType) {
        changeColumnInGICommon(table, new Runnable() {
            @Override
            public void run() {
                runAlter(ChangeLevel.TABLE,
                         String.format("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s", table, column, newType));
            }
        });
    }

    public void changeColumnNameInGI(final String table, final String column, final String newName) {
        changeColumnInGICommon(table, new Runnable() {
            @Override
            public void run() {
                runRenameColumn(new TableName(SCHEMA, table), column, newName);
            }
        });
    }

    @Test
    public void changeColumnTypeInGI_C() {
        changeColumnTypeInGI("c", "c1", "char(10)");
    }

    @Test
    public void changeColumnTypeInGI_O() {
        changeColumnTypeInGI("o", "o1", "bigint");
    }

    @Test
    public void changeColumnTypeInGI_I() {
        changeColumnTypeInGI("i", "i1", "bigint");
    }

    @Test
    public void changeColumnNameInGI_C() {
        changeColumnNameInGI("c", "c1", "c1_new");
    }

    @Test
    public void changeColumnNameInGI_O() {
        changeColumnNameInGI("o", "o1", "o1_new");
    }

    @Test
    public void changeColumnNameInGI_I() {
        changeColumnNameInGI("i", "i1", "i1_new");
    }
}
