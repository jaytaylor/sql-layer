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
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.View;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.error.ForeignKeyPreventsDropTableException;
import com.foundationdb.server.error.ForeignConstraintDDLException;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.ReferencedSQLJJarException;
import com.foundationdb.server.error.ViewReferencesExist;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.sql.aisddl.AISDDLITBase;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public final class DropSchemaIT extends AISDDLITBase {
    private void expectTables(String schemaName, String... tableNames) {
        final AkibanInformationSchema ais = ddl().getAIS(session());
        for (String name : tableNames) {
            final Table table = ais.getTable(schemaName, name);
            assertNotNull(schemaName + " " + name + " doesn't exist", table);
        }
    }

    private void expectNotTables(String schemaName, String... tableNames) {
        final AkibanInformationSchema ais = ddl().getAIS(session());
        for (String name : tableNames) {
            final Table table = ais.getTable(schemaName, name);
            assertNull(schemaName + " " + name + " still exists", table);
        }
    }

    private void expectRoutines(String schemaName, String... routineNames) {
        final AkibanInformationSchema ais = ddl().getAIS(session());
        for (String name : routineNames) {
            final Routine routine = ais.getRoutine(schemaName, name);
            assertNotNull(schemaName + " " + name + " doesn't exist", routine);
        }
    }

    private void expectNotRoutines(String schemaName, String... routineNames) {
        final AkibanInformationSchema ais = ddl().getAIS(session());
        for (String name : routineNames) {
            final Routine routine = ais.getRoutine(schemaName, name);
            assertNull(schemaName + " " + name + " still exists", routine);
        }
    }

    private void expectSqljJars(String schemaName, String... sqljJarNames) {
        final AkibanInformationSchema ais = ddl().getAIS(session());
        for (String name : sqljJarNames) {
            final SQLJJar sqljJar = ais.getSQLJJar(schemaName, name);
            assertNotNull(schemaName + " " + name + " doesn't exist", sqljJar);
        }
    }

    private void expectNotSqljJars(String schemaName, String... sqljJarNames) {
        final AkibanInformationSchema ais = ddl().getAIS(session());
        for (String name : sqljJarNames) {
            final SQLJJar sqljJar = ais.getSQLJJar(schemaName, name);
            assertNull(schemaName + " " + name + " still exists", sqljJar);
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

    public void createJarAndRoutine(String jarSchema, String jarName,
                                    String routineSchema, String routineName) throws MalformedURLException {
        AISBuilder builder = new AISBuilder();
        builder.sqljJar(jarSchema, jarName, new URL("file://ajar.jar"));
        ddl().createSQLJJar(session(), builder.akibanInformationSchema().getSQLJJar(jarSchema, jarName));
        builder.routine(routineSchema, routineName, "java", Routine.CallingConvention.JAVA);
        builder.routineExternalName(routineSchema, routineName, jarSchema, jarName, "className", "method");
        ddl().createRoutine(session(), builder.akibanInformationSchema().getRoutine(routineSchema, routineName), true);
    }

    @After
    public void lookForDanglingStorage() throws Exception {
        super.lookForDanglingStorage();
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
        writeRows(row(tid1, 1L), row(tid1, 2L));
        ddl().dropSchema(session(), "one");
        expectNotTables("one", "t");
        // Check for lingering data
        final int tid2 = createTable("one", "t", "id int not null primary key");
        expectRowCount(tid2, 0);
        expectRows(tid2);
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
        createChildTable("one", "o", "one", "c");
        createChildTable("two", "i", "one", "c");
        ddl().dropSchema(session(), "two");
        expectTables("one", "c", "o");
        expectNotTables("two", "i");
    }

    @Test
    public void crossSchemaGroupValidCheckData() throws InvalidOperationException {
        int cTableId = createTable("one", "c", "id int not null primary key");
        writeRow(cTableId, 1);
        int oTableId = createTable("two", "o",
                "id int not null primary key, oid int, grouping foreign key(oid) references one.c(id)");
        writeRow(oTableId, 100, 10);
        ddl().dropSchema(session(), "two");
        expectTables("one", "c");
        expectNotTables("two", "o");
        cTableId = getTable("one", "c").getTableId();
        expectRows(
                cTableId,
                row(cTableId, 1));
        oTableId = createTable("two", "o",
                "id int not null primary key, oid int, grouping foreign key(oid) references one.c(id)");
        writeRow(oTableId, 102, 10);
        List<Row> newRows = scanAllIndex(getTable("two", "o").getPrimaryKey().getIndex());
        assertEquals(newRows.toString(), newRows.size(), 1);
        assertEquals(102, ValueSources.toObject(newRows.get(0).value(0)));
        expectRows(
                oTableId,
                row(oTableId, 102, 10));
    }

    private int createChildTable(String childSchema, String childName, String parentSchema, String parentName) {
        return createTable(childSchema, childName,
                "id int not null primary key, pid int, grouping foreign key(pid) references " +
                        parentSchema + "." + parentName + "(id)");
    }

    @Test
    public void crossSchemaGroupValid2() throws InvalidOperationException {
        createTable("one", "c", "id int not null primary key");
        createChildTable("two", "o", "one", "c");
        createChildTable("two", "i", "two", "o");
        ddl().dropSchema(session(), "two");
        expectTables("one", "c");
        expectNotTables("two", "o", "i");
    }

    @Test
    public void crossSchemaGroupValid3() throws InvalidOperationException {
        createTable("one", "a", "id int not null primary key");
        createChildTable("one", "b", "one", "a");
        createChildTable("two", "c", "one", "b");
        createChildTable("two", "d", "two", "c");
        createChildTable("two", "e", "two", "d");
        ddl().dropSchema(session(), "two");
        expectTables("one", "a", "b");
        expectNotTables("two", "c", "d", "e");
    }

    @Test
    public void crossSchemaForeignKeyInvalid() throws InvalidOperationException {
        createTable("one", "c", "id int not null primary key");
        createTable("two", "o", "id int not null primary key, cid int, foreign key(cid) references one.c(id)");
        try {
            ddl().dropSchema(session(), "one");
            Assert.fail("ForeignConstraintDDLException expected");
        } catch(ForeignKeyPreventsDropTableException e) {
            // expected
        }
        expectTables("one", "c");
        expectTables("two", "o");
    }

    @Test
    public void crossSchemaForeignKeyValid() throws InvalidOperationException {
        createTable("one", "c", "id int not null primary key");
        createTable("two", "o", "id int not null primary key, cid int, foreign key(cid) references one.c(id)");
        ddl().dropSchema(session(), "two");
        expectTables("one", "c");
        expectNotTables("two", "o");
    }

    @Test
    public void dropSchemaSequence() throws InvalidOperationException {
        createTable("one", "o", "id int not null PRIMARY KEY generated by default as identity (start with 1)");
        createSequence("one", "seq1", "Start with 1 increment by 1 no cycle");
        ddl().dropSchema(session(), "one");
        expectNotSequence("one", "seq1");
        expectNotTables("one", "o");
    }

    @Test
    public void dropViewValidInSchema() throws Exception {
        executeDDL("create table one.t1(id int not null primary key, name varchar(128))");
        executeDDL("create table two.t2(id int not null primary key, name varchar(128))");
        executeDDL("create view one.v1 AS select * from one.t1");
        executeDDL("create view two.v2 AS select * from two.t2");
        ddl().dropSchema(session(), "one");
        expectNotTables("one", "t1");
        expectNotViews("one", "v1");
        expectTables("two", "t2");
        expectViews("two", "v2");
    }

    @Test
    public void dropViewInvalidOutsideSchema() throws Exception {
        executeDDL("create table one.t1(id int not null primary key, name varchar(128))");
        executeDDL("create table two.t2(id int not null primary key, name varchar(128))");
        executeDDL("create view one.crossview AS select t1.id,t1.name,t2.name "
                + "AS name2 FROM one.t1 t1, two.t2 t2 WHERE t1.id = t2.id");
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
        executeDDL("create table test.t1(id int not null primary key, name varchar(128))");
        executeDDL("create view test.v1 AS SELECT * FROM t1");
        executeDDL("create view test.v2 AS SELECT * FROM v1");
        ddl().dropSchema(session(), "test");
        expectNotViews("test", "v1", "v2", "v3");
    }

    @Test
    public void dropRoutine() throws Exception {
        AISBuilder builder = new AISBuilder();
        builder.routine("drop", "f", "javascript", Routine.CallingConvention.SCRIPT_FUNCTION_JSON);
        builder.routineDefinition("drop", "f", "function f() { return 3; }");
        builder.routine("keep", "p", "javascript", Routine.CallingConvention.SCRIPT_FUNCTION_JSON);
        builder.routineDefinition("keep", "p", "function f() { return 8; }");
        ddl().createRoutine(session(), builder.akibanInformationSchema().getRoutine("drop", "f"), true);
        ddl().createRoutine(session(), builder.akibanInformationSchema().getRoutine("keep", "p"), true);
        expectRoutines("drop", "f");
        expectRoutines("keep", "p");
        ddl().dropSchema(session(), "drop");
        expectNotRoutines("drop", "f");
        expectRoutines("keep", "p");
    }

    @Test
    public void dropRoutineInJarOfOtherSchema() throws Exception {
        createJarAndRoutine("keep", "mixedNuts", "drop", "peanuts");

        expectSqljJars("keep", "mixedNuts");
        expectRoutines("drop", "peanuts");

        ddl().dropSchema(session(), "drop");
        expectSqljJars("keep", "mixedNuts");
        expectNotRoutines("drop", "peanuts");
    }

    @Test
    public void dropSqljJar() throws Exception {
        AISBuilder builder = new AISBuilder();
        builder.sqljJar("drop", "grapeJelly", new URL("file://boo.jar"));
        ddl().createSQLJJar(session(), builder.akibanInformationSchema().getSQLJJar("drop", "grapeJelly"));
        builder.sqljJar("keep", "strawberryJam", new URL("file://far.jar"));
        ddl().createSQLJJar(session(), builder.akibanInformationSchema().getSQLJJar("keep", "strawberryJam"));
        expectSqljJars("drop", "grapeJelly");
        expectSqljJars("keep", "strawberryJam");
        ddl().dropSchema(session(), "drop");
        expectNotSqljJars("drop", "grapeJelly");
        expectSqljJars("keep", "strawberryJam");
    }

    @Test
    public void dropSqljJarWithRoutines() throws Exception {
        createJarAndRoutine("drop", "mixedNuts", "drop", "peanuts");
        createJarAndRoutine("keep", "treeNuts", "keep", "cashews");
        expectSqljJars("drop", "mixedNuts");
        expectRoutines("drop", "peanuts");

        ddl().dropSchema(session(), "drop");

        expectNotSqljJars("drop", "mixedNuts");
        expectNotRoutines("drop", "peanut");
        expectSqljJars("keep", "treeNuts");
        expectRoutines("keep", "cashews");
    }

    @Test
    public void dropSqljJarWithRoutinesInOtherSchemas() throws Exception {
        createJarAndRoutine("drop", "mixedNuts", "keep", "cashews");
        try {
            ddl().dropSchema(session(), "drop");
            Assert.fail("Expected exception to be thrown");
        } catch (ReferencedSQLJJarException e) {
            // expected exception
        }
        expectSqljJars("drop", "mixedNuts");
        expectRoutines("keep", "cashews");
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
