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

import com.foundationdb.ais.model.AISBuilder;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.TestAISBuilder;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.ais.util.TableChange;

import com.foundationdb.ais.util.TableChangeValidatorException.UndeclaredColumnChangeException;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.error.NoColumnsInTableException;
import com.foundationdb.server.error.NoSuchColumnException;
import com.foundationdb.server.error.NotNullViolationException;
import com.foundationdb.sql.StandardException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.foundationdb.ais.util.TableChangeValidator.ChangeLevel;
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
                createNewRow(cid, 1, "10"),
                createNewRow(cid, 2, "20"),
                createNewRow(cid, 3, "30")
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
                    createNewRow(oid, 10, 1, 11),
                        createNewRow(iid, 100, 10, 110),
                        createNewRow(iid, 101, 10, 111),
                    createNewRow(oid, 11, 1, 12),
                        createNewRow(iid, 111, 11, 122),
                createNewRow(cid, 2L, "b"),
                // no 3L
                    createNewRow(oid, 30, 3, 33),
                        createNewRow(iid, 300, 30, 330)
        );
    }

    private IndexRowType indexRowType(Index index) {
        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        return schema.indexRowType(index);
    }

    private void scanAndCheckIndex(IndexRowType type, Row... expectedRows) {
        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        StoreAdapter adapter = newStoreAdapter(schema);
        QueryContext queryContext = new SimpleQueryContext(adapter);
        QueryBindings queryBindings = queryContext.createBindings();
        compareRows(
                expectedRows,
                API.cursor(
                        API.indexScan_Default(type, false, IndexKeyRange.unbounded(type)),
                        queryContext, queryBindings
                )
        );
    }



    @Test(expected=UndeclaredColumnChangeException.class)
    public void unspecifiedColumnChange() {
        NewAISBuilder builder = AISBBasedBuilder.create(ddl().getTypesTranslator());
        builder.table(SCHEMA, "c").colInt("c1").pk("c1");
        Table table = builder.ais().getTable(SCHEMA, "c");

        ddl().createTable(session(),  table);
        updateAISGeneration();

        builder = AISBBasedBuilder.create(ddl().getTypesTranslator());
        builder.table(SCHEMA, "c").colInt("c1").colInt("c2").colInt("c3").pk("c1");
        table = builder.ais().getTable(SCHEMA, "c");

        ddl().alterTable(session(), table.getName(), table,
                         Arrays.asList(TableChange.createAdd("c2")), NO_CHANGES,
                         null);
    }

    @Test
    public void dropSingleColumnFromMultiColumnPK() throws StandardException {
        cid = createTable(SCHEMA, "c", "c1 int not null, c2 char(1), c3 int not null, primary key(c1,c3)");
        writeRows(
                createNewRow(cid, 1, "A", 50L),
                createNewRow(cid, 2, "B", 20L),
                createNewRow(cid, 5, "C", 10L)
        );
        runAlter(ChangeLevel.GROUP, "ALTER TABLE c DROP COLUMN c1");
        expectFullRows(
                cid,
                createNewRow(cid, "C", 10),
                createNewRow(cid, "B", 20),
                createNewRow(cid, "A", 50)
        );
        expectRows(
                scanAllIndexRequest(getTable(SCHEMA, "c").getIndex(Index.PRIMARY)),
                createNewRow(cid, UNDEF, 10),
                createNewRow(cid, UNDEF, 20),
                createNewRow(cid, UNDEF, 50)
        );
    }

    @Test
    public void dropPrimaryKeyMiddleOfGroup() throws StandardException {
        createAndLoadCOI();

        // Will yield 2 groups: C-O and I
        runAlter(ChangeLevel.GROUP, "ALTER TABLE o DROP PRIMARY KEY");

        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        RowType cType = schema.tableRowType(getTable(SCHEMA, "c"));
        RowType oType = schema.tableRowType(getTable(SCHEMA, "o"));
        RowType iType = schema.tableRowType(getTable(SCHEMA, "i"));
        StoreAdapter adapter = newStoreAdapter(schema);
        int pk = 1;
        compareRows(
                new Row[]{
                        testRow(cType, 1, "a"),
                        testRow(oType, 10, 1, 11, pk++),
                        testRow(oType, 11, 1, 12, pk++),
                        testRow(cType, 2, "b"),
                        testRow(oType, 30, 3, 33, pk++),
                },
                adapter.newGroupCursor(cType.table().getGroup())
        );
        compareRows(
                new Row[]{
                        testRow(iType, 100, 10, 110),
                        testRow(iType, 101, 10, 111),
                        testRow(iType, 111, 11, 122),
                        testRow(iType, 300, 30, 330)
                },
                adapter.newGroupCursor(iType.table().getGroup())
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
                createNewRow(cid, 1, "10"),
                createNewRow(cid, 2, "20"),
                createNewRow(cid, 3, "30")
        );
    }

    @Test
    public void addNotNullColumnDefault() throws StandardException {
        createAndLoadSingleTableGroup();
        runAlter("ALTER TABLE c ADD COLUMN c2 INT NOT NULL DEFAULT 0");
        expectFullRows(
                cid,
                createNewRow(cid, 1, "10", 0),
                createNewRow(cid, 2, "20", 0),
                createNewRow(cid, 3, "30", 0)
        );
    }

    @Test
    public void addSingleColumnSingleTableGroup() throws StandardException {
        createAndLoadSingleTableGroup();
        runAlter("ALTER TABLE c ADD COLUMN c2 INT NULL");
        expectFullRows(
                cid,
                createNewRow(cid, 1, "10", null),
                createNewRow(cid, 2, "20", null),
                createNewRow(cid, 3, "30", null)
        );
    }

    @Test
    public void addColumnIndexSingleTableNoPrimaryKey() throws StandardException {
        TableName cName = tableName(SCHEMA, "c");
        NewAISBuilder builder = AISBBasedBuilder.create(ddl().getTypesTranslator());
        builder.table(cName).colInt("c1", true).colInt("c2", true).colInt("c3", true);

        ddl().createTable(session(), builder.unvalidatedAIS().getTable(cName));
        updateAISGeneration();

        // Note: Not using standard id due to null index contents
        int tableId = tableId(cName);
        writeRows(
                createNewRow(tableId, 1, 2, 3),
                createNewRow(tableId, 4, 5, 6),
                createNewRow(tableId, 7, 8, 9)
        );

        builder = AISBBasedBuilder.create(ddl().getTypesTranslator());
        builder.table(cName).colInt("c1", true).colInt("c2", true).colInt("c3", true).colInt("c4", true).key("c4", "c4");
        List<TableChange> changes = new ArrayList<>();
        changes.add(TableChange.createAdd("c4"));

        ddl().alterTable(session(), cName, builder.unvalidatedAIS().getTable(cName), changes, changes, queryContext());
        updateAISGeneration();

        expectFullRows(
                tableId,
                createNewRow(tableId, 1, 2, 3, null),
                createNewRow(tableId, 4, 5, 6, null),
                createNewRow(tableId, 7, 8, 9, null)
        );

        expectRows(
                scanAllIndexRequest(getTable(tableId).getIndex("c4")),
                createNewRow(tableId, UNDEF, UNDEF, UNDEF, null),
                createNewRow(tableId, UNDEF, UNDEF, UNDEF, null),
                createNewRow(tableId, UNDEF, UNDEF, UNDEF, null)
        );

        ddl().dropTable(session(), cName);
    }

    @Test
    public void addSingleColumnRootOfGroup() throws StandardException {
        createAndLoadCOI();
        runAlter("ALTER TABLE c ADD COLUMN c2 INT NULL");
        expectFullRows(
                cid,
                createNewRow(cid, 1, "a", null),
                createNewRow(cid, 2, "b", null)
        );
    }

    @Test
    public void addSingleColumnMiddleOfGroup() throws StandardException {
        createAndLoadCOI();
        runAlter("ALTER TABLE o ADD COLUMN o2 INT NULL");
        expectFullRows(
                oid,
                createNewRow(oid, 10, 1, 11, null),
                createNewRow(oid, 11, 1, 12, null),
                createNewRow(oid, 30, 3, 33, null)
        );
    }

    @Test
    public void addSingleColumnLeafOfGroup() throws StandardException {
        createAndLoadCOI();
        runAlter("ALTER TABLE i ADD COLUMN i2 INT NULL");
        expectFullRows(
                iid,
                createNewRow(iid, 100, 10, 110, null),
                createNewRow(iid, 101, 10, 111, null),
                createNewRow(iid, 111, 11, 122, null),
                createNewRow(iid, 300, 30, 330, null)
        );
    }

    @Test
    public void dropSingleColumnSingleTableGroup() throws StandardException {
        createAndLoadSingleTableGroup();
        runAlter("ALTER TABLE c DROP COLUMN c1");
        expectFullRows(
                cid,
                createNewRow(cid, 1),
                createNewRow(cid, 2),
                createNewRow(cid, 3)
        );
    }

    @Test
    public void dropSingleColumnRootOfGroup() throws StandardException {
        createAndLoadCOI();
        runAlter("ALTER TABLE c DROP COLUMN c1");
        expectFullRows(
                cid,
                createNewRow(cid, 1),
                createNewRow(cid, 2)
        );
    }

    @Test
    public void dropSingleColumnMiddleOfGroup() throws StandardException {
        createAndLoadCOI();
        runAlter("ALTER TABLE o DROP COLUMN o1");
        expectFullRows(
                oid,
                createNewRow(oid, 10, 1),
                createNewRow(oid, 11, 1),
                createNewRow(oid, 30, 3)
        );
    }

    @Test
    public void dropSingleColumnLeafOfGroup() throws StandardException {
        createAndLoadCOI();
        runAlter("ALTER TABLE i DROP COLUMN i1");
        expectFullRows(
                iid,
                createNewRow(iid, 100, 10),
                createNewRow(iid, 101, 10),
                createNewRow(iid, 111, 11),
                createNewRow(iid, 300, 30)
        );
    }

    @Test
    public void dropSingleColumnOfSingleColumnIndex() throws StandardException {
        createAndLoadSingleTableGroup();
        createIndex(SCHEMA, "c", "c1", "c1");

        runAlter("ALTER TABLE c DROP COLUMN c1");

        expectIndexes(cid, "PRIMARY");
        expectFullRows(
                cid,
                createNewRow(cid,  1),
                createNewRow(cid,  2),
                createNewRow(cid,  3)
        );
    }

    @Test
    public void dropSingleColumnOfMultiColumnIndex() throws StandardException {
        cid = createTable(SCHEMA, "c", "id int not null primary key, c1 int, c2 int");
        createIndex(SCHEMA, "c", "c1_c2", "c1", "c2");
        writeRows(
                createNewRow(cid,  1,  11,  12),
                createNewRow(cid,  2,  21,  22),
                createNewRow(cid,  3,  31,  32),
                createNewRow(cid, 10, 101, 102)
        );

        runAlter("ALTER TABLE c DROP COLUMN c1");

        expectRows(
                scanAllIndexRequest(getTable(SCHEMA, "c").getIndex("c1_c2")),
                createNewRow(cid, UNDEF, 12),
                createNewRow(cid, UNDEF, 22),
                createNewRow(cid, UNDEF, 32),
                createNewRow(cid, UNDEF, 102)
        );
    }

    @Test
    public void dropSingleColumnOfMultiColumnGroupIndex() throws StandardException {
        createAndLoadCOI();
        createLeftGroupIndex(C_NAME, "c1_o1_o2", "c.c1", "o.o1", "i.i1");

        runAlter("ALTER TABLE o DROP COLUMN o1");

        AkibanInformationSchema ais = ddl().getAIS(session());
        Index index = ais.getGroup(C_NAME).getIndex("c1_o1_o2");
        assertNotNull("Index still exists", index);
        assertEquals("Index column count", 2, index.getKeyColumns().size());

        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        IndexRowType indexRowType = schema.indexRowType(index);

        StoreAdapter adapter = newStoreAdapter(schema);
        QueryContext queryContext = new SimpleQueryContext(adapter);
        QueryBindings queryBindings = queryContext.createBindings();
        compareRows(
                new Row[] {
                        testRow(indexRowType, "a", 110, 1, 10, 100),
                        testRow(indexRowType, "a", 111, 1, 10, 101),
                        testRow(indexRowType, "a", 122, 1, 11, 111),
                },
                API.cursor(
                        API.indexScan_Default(indexRowType, false, IndexKeyRange.unbounded(indexRowType)),
                        queryContext, queryBindings
                )
        );
    }

    @Test
    public void dropGroupingForeignKeyTableInGroupIndex() throws StandardException {
        createAndLoadCOI();
        createLeftGroupIndex(new TableName(SCHEMA, "c"), "c1_o1_i1", "c.c1", "o.o1", "i.i1");

        runAlter(ChangeLevel.GROUP, "ALTER TABLE o DROP GROUPING FOREIGN KEY");

        AkibanInformationSchema ais = ddl().getAIS(session());
        Index index = ais.getGroup(C_NAME).getIndex("c1_o1_i1");
        assertNull("Index should not exist on c group", index);
        index = ais.getGroup(O_NAME).getIndex("c1_o1_i1");
        assertNull("Index should not exist on o group", index);
    }

    @Test
    public void changeDataTypeSingleColumnSingleTableGroup() throws StandardException {
        createAndLoadSingleTableGroup();
        runAlter("ALTER TABLE c ALTER COLUMN c1 SET DATA TYPE int");
        expectFullRows(
                cid,
                createNewRow(cid, 1, 10),
                createNewRow(cid, 2, 20),
                createNewRow(cid, 3, 30)
        );
    }

    @Test
    public void changeDataTypeSingleColumnInIndex() throws StandardException {
        createAndLoadSingleTableGroup();
        createIndex(SCHEMA, "c", "c1", "c1");
        runAlter("ALTER TABLE c ALTER COLUMN c1 SET DATA TYPE int");
        expectFullRows(
                cid,
                createNewRow(cid, 1, 10),
                createNewRow(cid, 2, 20),
                createNewRow(cid, 3, 30)
        );
        expectRows(
                scanAllIndexRequest(getTable(SCHEMA, "c").getIndex("c1")),
                createNewRow(cid, UNDEF, 10),
                createNewRow(cid, UNDEF, 20),
                createNewRow(cid, UNDEF, 30)
        );
    }

    @Test
    public void addDropAndAlterColumnSingleTableGroup() throws StandardException {
        cid = createTable(SCHEMA, "c", "c1 int not null primary key, c2 char(5), c3 int, c4 char(1)");
        writeRows(
                createNewRow(cid, 1, "one", 10, "A"),
                createNewRow(cid, 2, "two", 20, "B"),
                createNewRow(cid, 3, "three", 30, "C"),
                createNewRow(cid, 10, "ten", 100, "D")
        );

        // Our parser doesn't (yet) support multi-action alters, manually build parameters
        // ALTER TABLE c ADD COLUMN c5 INT, DROP COLUMN c2, ALTER COLUMN c3 SET DATA TYPE char(3)
        TestAISBuilder builder = new TestAISBuilder(typesRegistry());
        builder.table(SCHEMA, "c");
        builder.column(SCHEMA, "c", "c1", 0, "MCOMPAT", "int", false);
        builder.column(SCHEMA, "c", "c3", 1, "MCOMPAT", "char", 3L, null, true);
        builder.column(SCHEMA, "c", "c4", 2, "MCOMPAT", "char", 1L, null, true);
        builder.column(SCHEMA, "c", "c5", 3, "MCOMPAT", "int", true);
        builder.pk(SCHEMA, "c");
        builder.indexColumn(SCHEMA, "c", Index.PRIMARY, "c1", 0, true, null);
        builder.basicSchemaIsComplete();
        builder.createGroup("c", SCHEMA);
        builder.addTableToGroup(C_NAME, SCHEMA, "c");
        builder.groupingIsComplete();
        Table newTable = builder.akibanInformationSchema().getTable(SCHEMA, "c");

        List<TableChange> changes = new ArrayList<>();
        changes.add(TableChange.createAdd("c5"));
        changes.add(TableChange.createDrop("c2"));
        changes.add(TableChange.createModify("c3", "c3"));

        ddl().alterTable(session(), new TableName(SCHEMA, "c"), newTable, changes, NO_CHANGES, queryContext());
        updateAISGeneration();

        expectFullRows(
                cid,
                createNewRow(cid, 1, "10", "A", null),
                createNewRow(cid, 2, "20", "B", null),
                createNewRow(cid, 3, "30", "C", null),
                createNewRow(cid, 10, "100", "D", null)
        );
    }

    @Test
    public void addUniqueKeyExistingColumn() throws StandardException {
        createAndLoadSingleTableGroup();
        runAlter(ChangeLevel.INDEX, "ALTER TABLE c ADD UNIQUE(c1)");
        expectIndexes(cid, "PRIMARY", "c1");
        expectRows(
                scanAllIndexRequest(getTable(SCHEMA, "c").getIndex("c1")),
                createNewRow(cid, UNDEF, "10"),
                createNewRow(cid, UNDEF, "20"),
                createNewRow(cid, UNDEF, "30")
        );
    }

    @Test
    public void alterColumnNotNullToNull() throws StandardException {
        cid = createTable(SCHEMA, "c", "id int not null primary key, c1 char(5) not null");
        writeRows(
                createNewRow(cid, 1, "10"),
                createNewRow(cid, 2, "20"),
                createNewRow(cid, 3, "30")
        );
        runAlter(ChangeLevel.METADATA, "ALTER TABLE c ALTER COLUMN c1 NULL");
        // Just check metadata
        // Insert needs more plumbing (e.g. Insert_Default), checked in test-alter-nullability.yaml
        Table table = getTable(SCHEMA, "c");
        assertEquals("c1 nullable", true, table.getColumn("c1").getNullable());
    }

    @Test
    public void alterColumnNullToNotNull() throws StandardException {
        cid = createTable(SCHEMA, "c", "id int not null primary key, c1 char(5) null");
        writeRows(
                createNewRow(cid, 1, "10"),
                createNewRow(cid, 2, "20"),
                createNewRow(cid, 3, "30")
        );
        runAlter(ChangeLevel.METADATA_CONSTRAINT, "ALTER TABLE c ALTER COLUMN c1 NOT NULL");
        // Just check metadata
        // Insert needs more plumbing (e.g. Insert_Default), checked in test-alter-nullability.yaml
        Table table = getTable(SCHEMA, "c");
        assertEquals("c1 nullable", false, table.getColumn("c1").getNullable());
    }

    @Test
    public void dropUniqueAddIndexSameName() {
        cid = createTable(SCHEMA, "c", "id int not null primary key, c1 char(1), c2 char(1), constraint foo unique(c1)");
        writeRows(
                createNewRow(cid, 1, "A", "3"),
                createNewRow(cid, 2, "B", "2"),
                createNewRow(cid, 3, "C", "1")
        );

        AkibanInformationSchema ais = aisCloner().clone(ddl().getAIS(session()));
        Table table = ais.getTable(SCHEMA, "c");
        table.removeIndexes(Collections.singleton(table.getIndex("foo")));
        AISBuilder builder = new AISBuilder(ais);
        builder.index(SCHEMA, "c", "foo");
        builder.indexColumn(SCHEMA, "c", "foo", "c2", 0, true, null);

        List<TableChange> changes = new ArrayList<>();
        changes.add(TableChange.createDrop("foo"));
        changes.add(TableChange.createAdd("foo"));

        ddl().alterTable(session(), new TableName(SCHEMA, "c"), table, NO_CHANGES, changes, queryContext());
        updateAISGeneration();

        expectIndexes(cid, "foo", "PRIMARY");
        expectRows(
                scanAllIndexRequest(getTable(SCHEMA, "c").getIndex("foo")),
                createNewRow(cid, UNDEF, UNDEF, "1"),
                createNewRow(cid, UNDEF, UNDEF, "2"),
                createNewRow(cid, UNDEF, UNDEF, "3")
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

        AkibanInformationSchema ais = aisCloner().clone(ddl().getAIS(session()));
        Table table = ais.getTable(SCHEMA, "c");
        table.removeIndexes(Collections.singleton(table.getIndex("foo")));
        AISBuilder builder = new AISBuilder(ais);
        builder.index(SCHEMA, "c", "foo");
        builder.indexColumn(SCHEMA, "c", "foo", "c2", 0, true, null);
        builder.indexColumn(SCHEMA, "c", "foo", "c1", 1, true, null);

        List<TableChange> changes = new ArrayList<>();
        changes.add(TableChange.createModify("foo", "foo"));

        ddl().alterTable(session(), new TableName(SCHEMA, "c"), table, NO_CHANGES, changes, queryContext());
        updateAISGeneration();

        expectIndexes(cid, "foo", "PRIMARY");
        expectRows(
                scanAllIndexRequest(getTable(SCHEMA, "c").getIndex("foo")),
                createNewRow(cid, UNDEF, "C", "1"),
                createNewRow(cid, UNDEF, "B", "2"),
                createNewRow(cid, UNDEF, "A", "3")
        );
    }

    @Test
     public void addColumnDropColumnAddIndexOldNewMiddleOfGroup() throws StandardException {
        createAndLoadCOI();

        // ALTER TABLE o DROP COLUMN o1, ADD COLUMN o1 INT, ADD INDEX x(o1), ADD INDEX y(cid)
        AkibanInformationSchema ais = aisCloner().clone(ddl().getAIS(session()));
        Table table = ais.getTable(SCHEMA, "o");
        table.dropColumn("o1");
        TestAISBuilder builder = new TestAISBuilder(ais, typesRegistry());
        builder.column(SCHEMA, "o", "o1", 2, "MCOMPAT", "int", true);
        builder.index(SCHEMA, "o", "x");
        builder.indexColumn(SCHEMA, "o", "x", "o1", 0, true, null);
        builder.index(SCHEMA, "o", "y");
        builder.indexColumn(SCHEMA, "o", "y", "cid", 0, true, null);

        List<TableChange> columnChanges = new ArrayList<>();
        columnChanges.add(TableChange.createDrop("o1"));
        columnChanges.add(TableChange.createAdd("o1"));
        List<TableChange> indexChanges = new ArrayList<>();
        indexChanges.add(TableChange.createAdd("x"));
        indexChanges.add(TableChange.createAdd("y"));

        ddl().alterTable(session(), new TableName(SCHEMA, "o"), table, columnChanges, indexChanges, queryContext());
        updateAISGeneration();

        expectFullRows(
                oid,
                createNewRow(oid, 10, 1, null),
                createNewRow(oid, 11, 1, null),
                createNewRow(oid, 30, 3, null)
        );

        expectIndexes(oid, "PRIMARY", "x", "y");

        expectRows(
                scanAllIndexRequest(getTable(SCHEMA, "o").getIndex("x")),
                createNewRow(oid, UNDEF, UNDEF, null),
                createNewRow(oid, UNDEF, UNDEF, null),
                createNewRow(oid, UNDEF, UNDEF, null)
        );

        expectRows(
                scanAllIndexRequest(getTable(SCHEMA, "o").getIndex("y")),
                createNewRow(oid, UNDEF, 1, UNDEF),
                createNewRow(oid, UNDEF, 1, UNDEF),
                createNewRow(oid, UNDEF, 3, UNDEF)
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
        RowType cType = schema.tableRowType(getTable(SCHEMA, "c"));
        RowType oType = schema.tableRowType(getTable(SCHEMA, "o"));
        RowType iType = schema.tableRowType(getTable(SCHEMA, "i"));

        StoreAdapter adapter = newStoreAdapter(schema);
        compareRows(
                new Row[] {
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
                adapter.newGroupCursor(cType.table().getGroup())
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
        RowType cType = schema.tableRowType(getTable(SCHEMA, "c"));
        RowType oType = schema.tableRowType(getTable(SCHEMA, "o"));

        StoreAdapter adapter = newStoreAdapter(schema);
        compareRows(
                new Row[] {
                        testRow(cType, 1L, "asdf"),
                        testRow(cType, 5, "qwer"),
                        testRow(cType, 10, "zxcv")
                },
                adapter.newGroupCursor(cType.table().getGroup())
        );
        compareRows(
                new Row[] {
                        testRow(oType, 10, 1, "a"),
                        testRow(oType, 11, 1, "b"),
                        testRow(oType, 60, 6, "c"),
                },
                adapter.newGroupCursor(oType.table().getGroup())
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
        RowType cType = schema.tableRowType(getTable(SCHEMA, "c"));
        RowType oType = schema.tableRowType(getTable(SCHEMA, "o"));
        RowType iType = schema.tableRowType(getTable(SCHEMA, "i"));

        StoreAdapter adapter = newStoreAdapter(schema);
        compareRows(
                new Row[] {
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
                adapter.newGroupCursor(cType.table().getGroup())
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
        RowType cType = schema.tableRowType(getTable(SCHEMA, "c"));
        RowType oType = schema.tableRowType(getTable(SCHEMA, "o"));
        RowType iType = schema.tableRowType(getTable(SCHEMA, "i"));

        StoreAdapter adapter = newStoreAdapter(schema);
        compareRows(
                new Row[] {
                        testRow(cType, 1, "asdf"),
                        testRow(cType, 5, "qwer"),
                        testRow(cType, 10, "zxcv")
                },
                adapter.newGroupCursor(cType.table().getGroup())
        );

        compareRows(
                new Row[] {
                        testRow(oType, 10, 1, "a"),
                            testRow(iType, 100, 10, "d"),
                            testRow(iType, 101, 10, "e"),
                            testRow(iType, 102, 10, "f"),
                        testRow(oType, 11, 1, "b"),
                        // none
                            testRow(iType, 200, 20, "d"),
                        testRow(oType, 60, 6, "c"),
                },
                adapter.newGroupCursor(oType.table().getGroup())
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
        Table table = getTable(C_NAME);
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

        TestAISBuilder builder = new TestAISBuilder(typesRegistry());
        builder.table(SCHEMA, C_TABLE);
        builder.column(SCHEMA, C_TABLE, "c2", 0, "MCOMPAT", "int", true);
        builder.column(SCHEMA, C_TABLE, "c1", 1, "MCOMPAT", "int", false);
        builder.pk(SCHEMA, C_TABLE);
        builder.indexColumn(SCHEMA, C_TABLE, Index.PRIMARY, "c1", 0, true, null);
        builder.index(SCHEMA, C_TABLE, "c2");
        builder.indexColumn(SCHEMA, C_TABLE, "c2", "c2", 0, true, null);
        builder.basicSchemaIsComplete();
        builder.createGroup(C_TABLE, SCHEMA);
        builder.addTableToGroup(C_NAME, SCHEMA, C_TABLE);
        builder.groupingIsComplete();

        runAlter(ChangeLevel.TABLE,
                 C_NAME, builder.akibanInformationSchema().getTable(C_NAME),
                 Arrays.asList(TableChange.createModify("c1", "c1"), TableChange.createModify("c2", "c2")),
                 NO_CHANGES);

        expectFullRows(
                cid,
                createNewRow(cid, 10, 1),
                createNewRow(cid, 20, 2),
                createNewRow(cid, 30, 3)
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
                createNewRow(cid, 1, "AZ"),
                createNewRow(cid, 2, "NY"),
                createNewRow(cid, 3, "MA"),
                createNewRow(cid, 4, "WA")
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
        Sequence seq = getTable(id).getColumn("id").getIdentityGenerator();
        assertNotNull("id column has sequence", seq);
        writeRows(
                createNewRow(id, 1, "1"),
                createNewRow(id, 2, "2"),
                createNewRow(id, 3, "3")
        );
        runAlter(ChangeLevel.GROUP, "ALTER TABLE c ALTER COLUMN id SET DATA TYPE varchar(10)");
        assertNull("sequence was dropped", ddl().getAIS(session()).getSequence(seq.getSequenceName()));
        assertNull("id column has no sequence", getTable(id).getColumn("id").getIdentityGenerator());
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
        Sequence seq = getTable(id).getColumn("id").getIdentityGenerator();
        assertNotNull("id column has sequence", seq);
        writeRows(
                createNewRow(id, 1, "1"),
                createNewRow(id, 2, "2"),
                createNewRow(id, 3, "3")
        );
        runAlter(ChangeLevel.GROUP, "ALTER TABLE c DROP COLUMN id");
        assertNull("sequence was dropped", ddl().getAIS(session()).getSequence(seq.getSequenceName()));
        assertNull("id column does not exist", getTable(id).getColumn("id"));
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
        createLeftGroupIndex(C_NAME, "c1_01", "c.c1", "o.o1");
        runAlter(ChangeLevel.TABLE, "ALTER TABLE o DROP COLUMN o1");
        assertEquals("Remaining group indexes", "[]", ddl().getAIS(session()).getGroup(C_NAME).getIndexes().toString());
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
        TableName groupName = getTable(schema2, "c").getGroup().getName();
        createLeftGroupIndex(groupName, "c1_01", "c.c1", "o.o1");

        runAlter(ChangeLevel.TABLE, "ALTER TABLE test2.o ALTER COLUMN o1 SET DATA TYPE bigint");

        Index gi = getTable(schema2, "c").getGroup().getIndex("c1_01");
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
        TableName groupName = getTable(SCHEMA, table).getGroup().getName();
        createLeftGroupIndex(groupName, giName, "c.c1", "o.o1", "i.i1");

        alterRunnable.run();

        Index gi = getTable(SCHEMA, table).getGroup().getIndex(giName);
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

    @Test
    public void alterColumnDefaultIdentity() {
        final int id = createTable(SCHEMA, C_TABLE,
                                   "id INT NOT NULL PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY");
        Column column = getTable(id).getColumn("id");
        assertEquals("identity is default", true, column.getDefaultIdentity());
        Sequence seq = column.getIdentityGenerator();
        assertNotNull("id column has sequence", seq);

        runAlter(ChangeLevel.METADATA, "ALTER TABLE c ALTER COLUMN id DROP DEFAULT");
        assertNull("Old seq was dropped", ais().getSequence(seq.getSequenceName()));

        runAlter(ChangeLevel.METADATA, "ALTER TABLE c ALTER COLUMN id SET GENERATED ALWAYS AS IDENTITY");
        Column newColumn = getTable(id).getColumn("id");
        assertEquals("altered is always", false, newColumn.getDefaultIdentity());
        seq = newColumn.getIdentityGenerator();
        assertEquals("Sequence name suffix",
                     true,
                     seq.getSequenceName().getTableName().endsWith("_seq"));
    }

    @Test
    public void alterNoPKAddGroupingFK() {
        int cid = createTable(SCHEMA, C_TABLE, "id INT NOT NULL PRIMARY KEY, a CHAR(5)");
        int oid = createTable(SCHEMA, O_TABLE, "b CHAR(5), cid INT");

        writeRows(createNewRow(cid, 1L, "a"),
                  createNewRow(cid, 2L, "b"),
                  createNewRow(cid, 3L, "c"),
                  createNewRow(cid, 4L, "d"));

        writeRows(createNewRow(oid, "aa", 1L),
                  createNewRow(oid, "bb", 2L),
                  createNewRow(oid, "cc", 3L));

        runAlter(ChangeLevel.GROUP, "ALTER TABLE o ADD GROUPING FOREIGN KEY(cid) REFERENCES c(id)");

        // Check for a hidden PK generator in a bad state (e.g. reproducing old values)
        writeRows(createNewRow(oid, "dd", 4L));

        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        RowType cType = schema.tableRowType(getTable(SCHEMA, C_TABLE));
        RowType oType = schema.tableRowType(getTable(SCHEMA, O_TABLE));
        StoreAdapter adapter = newStoreAdapter(schema);
        compareRows(
                new Row[] {
                        testRow(cType, 1L, "a"),
                            testRow(oType, "aa", 1L, 1L),
                        testRow(cType, 2L, "b"),
                            testRow(oType, "bb", 2L, 2L),
                        testRow(cType, 3L, "c"),
                            testRow(oType, "cc", 3L, 3L),
                        testRow(cType, 4L, "d"),
                            testRow(oType, "dd", 4L, 4L),
                },
                adapter.newGroupCursor(cType.table().getGroup())
        );
    }

    @Test
    public void renameColumnWithNoPK() {
        int cid = createTable(SCHEMA, C_TABLE, "n char(1)");

        writeRows(createNewRow(cid, "a"),
                  createNewRow(cid, "b"),
                  createNewRow(cid, "c"),
                  createNewRow(cid, "d"));

        runAlter(ChangeLevel.METADATA, "ALTER TABLE c RENAME COLUMN \"n\" TO \"n2\"");

        // Check for a hidden PK generator in a bad state (e.g. reproducing old values)
        writeRows(createNewRow(cid, "e"));

        expectFullRows(
                cid,
                createNewRow(cid, "a"),
                createNewRow(cid, "b"),
                createNewRow(cid, "c"),
                createNewRow(cid, "d"),
                // inserted after alter
                createNewRow(cid, "e")
        );
    }

    @Test
    public void addColumnToPKLessTable() {
        int cid = createTable(SCHEMA, C_TABLE, "s char(1)");

        writeRows(createNewRow(cid, "a"),
                createNewRow(cid, "b"),
                createNewRow(cid, "c"),
                createNewRow(cid, "d"));

        runAlter(ChangeLevel.TABLE, "ALTER TABLE c ADD COLUMN n INT DEFAULT 0");

        // the -1L is filler for the hidden key
        writeRows(createNewRow(cid, "e", 3, -1L));

        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        TableRowType cType = schema.tableRowType(getTable(SCHEMA, C_TABLE));
        StoreAdapter adapter = newStoreAdapter(schema);
        long pk = 1L;
        compareRows(
                new Row[]{
                        testRow(cType, "a", 0, pk++),
                        testRow(cType, "b", 0, pk++),
                        testRow(cType, "c", 0, pk++),
                        testRow(cType, "d", 0, pk++),
                        testRow(cType, "e", 3, pk++),
                },
                adapter.newGroupCursor(cType.table().getGroup())
        );
    }

    @Test
    public void dropPKColumn() {
        int cid = createTable(SCHEMA, C_TABLE, "s char(1), n int not null primary key");

        writeRows(createNewRow(cid, "a", 1),
                createNewRow(cid, "b", 2),
                createNewRow(cid, "c", 3),
                createNewRow(cid, "d", 4));

        runAlter(ChangeLevel.GROUP, "ALTER TABLE c DROP COLUMN n");

        // the -1L is filler for the hidden key
        writeRows(createNewRow(cid, "e", -1L));

        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        TableRowType cType = schema.tableRowType(getTable(SCHEMA, C_TABLE));
        StoreAdapter adapter = newStoreAdapter(schema);
        long pk = 1L;
        compareRows(
                new Row[]{
                        testRow(cType, "a", pk++),
                        testRow(cType, "b", pk++),
                        testRow(cType, "c", pk++),
                        testRow(cType, "d", pk++),
                        testRow(cType, "e", pk++),
                },
                adapter.newGroupCursor(cType.table().getGroup())
        );
    }

    @Test
    public void addPKColumnToPKLessTable() {
        int cid = createTable(SCHEMA, C_TABLE, "s char(1)");

        writeRows(createNewRow(cid, "a"),
                createNewRow(cid, "b"),
                createNewRow(cid, "c"),
                createNewRow(cid, "d"));

        runAlter(ChangeLevel.GROUP, "ALTER TABLE c ADD COLUMN n SERIAL PRIMARY KEY");

        // writerows doesn't run default handling behavior
        writeRows(createNewRow(cid, "e", 5));

        expectFullRows(
                cid,
                createNewRow(cid, "a", 1),
                createNewRow(cid, "b", 2),
                createNewRow(cid, "c", 3),
                createNewRow(cid, "d", 4),
                // inserted after alter
                createNewRow(cid, "e", 5)
        );
    }

    @Test
    public void addPKToPKLessTable() {
        int cid = createTable(SCHEMA, C_TABLE, "n char(1) NOT NULL");

        writeRows(createNewRow(cid, "a"),
                createNewRow(cid, "b"),
                createNewRow(cid, "c"),
                createNewRow(cid, "d"));

        runAlter(ChangeLevel.GROUP, "ALTER TABLE c ADD PRIMARY KEY(n)");

        // Check for a hidden PK generator in a bad state (e.g. reproducing old values)
        writeRows(createNewRow(cid, "e"));

        expectFullRows(
                cid,
                createNewRow(cid, "a"),
                createNewRow(cid, "b"),
                createNewRow(cid, "c"),
                createNewRow(cid, "d"),
                // inserted after alter
                createNewRow(cid, "e")
        );
    }

    @Test
    public void alterPKlessTableWithIndex() {
        // This changes an index, and does a TABLE change, but not a GROUP change
        int cid = createTable(SCHEMA, C_TABLE, "a char(1) NOT NULL, b char(1) NOT NULL");
        createIndex(SCHEMA, C_TABLE, "a_index", "a");

        writeRows(createNewRow(cid, "a", "z"),
                createNewRow(cid, "b", "y"),
                createNewRow(cid, "c", "x"),
                createNewRow(cid, "d", "w"));

        runAlter(ChangeLevel.TABLE, "ALTER TABLE c ALTER a SET DATA TYPE varchar(3)");

        // Check for a hidden PK generator in a bad state (e.g. reproducing old values)
        writeRows(createNewRow(cid, "e", "v"));

        expectFullRows(
                cid,
                createNewRow(cid, "a", "z"),
                createNewRow(cid, "b", "y"),
                createNewRow(cid, "c", "x"),
                createNewRow(cid, "d", "w"),
                // inserted after alter
                createNewRow(cid, "e", "v")
        );
    }

    @Test(expected=NoColumnsInTableException.class)
    public void alterDropsAllColumns() {
        createTable(SCHEMA, "t", "c1 int");
        runAlter("ALTER TABLE t DROP COLUMN c1");
    }
}
