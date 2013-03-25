/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server.service.restdml;

import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.ModelBuilderException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.service.servicemanager.GuicedServiceManager;
import com.akiban.server.test.it.ITBase;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ModelBuilderIT extends ITBase {
    private static final String SCHEMA = "test";
    private static final String TABLE = "t";
    private static final TableName TABLE_NAME = new TableName(SCHEMA, TABLE);
    private static final String SIMPLE_JSON_1 = "{ \"name\": \"foo\" }";
    private static final String SIMPLE_JSON_2 = "{ \"year\": \"2013\" }";

    private static final PrintWriter NULL_WRITER = new PrintWriter(new Writer() {
        @Override
        public void write(char[] cbuf, int off, int len) {
        }

        @Override
        public void flush() throws IOException {
        }

        @Override
        public void close() throws IOException {
        }
    });


    private ModelBuilder builder;

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bindAndRequire(RestDMLService.class, RestDMLServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(ModelBuilderIT.class);
    }

    @Before
    public void createBuilder() {
        RestDMLService restDMLService = serviceManager().getServiceByClass(RestDMLService.class);
        builder = new ModelBuilder(
            serviceManager().getSessionService(),
            txnService(),
            dxl(),
            store(),
            treeService(),
            configService(),
            restDMLService
        );
    }


    @Test
    public void create() {
        builder.create(TABLE_NAME);
        assertNotNull("Created table", getUserTable(TABLE_NAME));
    }

    @Test
    public void createAndCheckIsBuilderTable() {
        builder.create(TABLE_NAME);
        assertTrue("Is builder table", builder.isBuilderTable(TABLE_NAME));
    }

    @Test
    public void isNotBuilderTable() {
        createTable(SCHEMA, TABLE, "foo int");
        assertFalse("Is builder table", builder.isBuilderTable(TABLE_NAME));
    }

    @Test
    public void createMulti() {
        builder.create(TABLE_NAME);
        builder.create(TABLE_NAME);
        assertTrue("Is builder table", builder.isBuilderTable(TABLE_NAME));
    }

    @Test
    public void insertCreates() {
        builder.insert(NULL_WRITER, TABLE_NAME, SIMPLE_JSON_1);
        assertNotNull("Table exists after insert", getUserTable(TABLE_NAME));
    }

    @Test
    public void insertAndCheckRow() {
        builder.insert(NULL_WRITER, TABLE_NAME, SIMPLE_JSON_1);
        updateAISGeneration();
        int id = tableId(TABLE_NAME);
        expectFullRows(id, createNewRow(id, 1L, SIMPLE_JSON_1));
    }

    @Test
    public void updateNoMatchCreates() {
        builder.update(NULL_WRITER, TABLE_NAME, "1", SIMPLE_JSON_1);
        assertNotNull("Table exists after insert", getUserTable(TABLE_NAME));
    }

    @Test
    public void insertUpdateCheckRow() {
        builder.insert(NULL_WRITER, TABLE_NAME, SIMPLE_JSON_1);
        builder.update(NULL_WRITER, TABLE_NAME, "1", SIMPLE_JSON_2);
        updateAISGeneration();
        int id = tableId(TABLE_NAME);
        expectFullRows(id, createNewRow(id, 1L, SIMPLE_JSON_2));
    }

    @Test
    public void explodeSingleRow() throws IOException {
        builder.insert(NULL_WRITER, TABLE_NAME, SIMPLE_JSON_1);
        builder.explode(NULL_WRITER, TABLE_NAME);
        updateAISGeneration();
        assertEquals("Tables in schema", "["+TABLE+"]", ais().getSchema(SCHEMA).getUserTables().keySet().toString());
        UserTable table = getUserTable(TABLE_NAME);
        assertEquals("Column count", 2, table.getColumns().size());
        assertEquals("Column 0", "name", table.getColumn(0).getName());
        assertEquals("Column 1", ModelBuilder.ID_COL_NAME, table.getColumn(1).getName());
        expectFullRows(table.getTableId(), createNewRow(table.getTableId(), "foo", 1L));
    }

    @Test
    public void explodeMultiRowDifferentShapes() throws IOException {
        builder.insert(NULL_WRITER, TABLE_NAME, SIMPLE_JSON_1);
        builder.insert(NULL_WRITER, TABLE_NAME, SIMPLE_JSON_2);
        builder.explode(NULL_WRITER, TABLE_NAME);
        updateAISGeneration();
        assertEquals("Tables in schema", "["+TABLE+"]", ais().getSchema(SCHEMA).getUserTables().keySet().toString());
        UserTable table = getUserTable(TABLE_NAME);
        assertEquals("Column count", 2, table.getColumns().size());
        assertEquals("Column 0", "year", table.getColumn(0).getName());
        assertEquals("Column 1", ModelBuilder.ID_COL_NAME, table.getColumn(1).getName());
        expectFullRows(
                table.getTableId(),
                createNewRow(table.getTableId(), null, 1L),
                createNewRow(table.getTableId(), "2013", 2L)
        );
    }

    @Test
    public void implodeEmptyTable() {
        createTable(SCHEMA, TABLE, "foo int, bar int, zap int");
        builder.implode(NULL_WRITER, TABLE_NAME);
        assertEquals("Tables in schema", "[" + TABLE + "]", ais().getSchema(SCHEMA).getUserTables().keySet().toString());
        assertTrue("Is builder table", builder.isBuilderTable(TABLE_NAME));
    }

    @Test
    public void implodeWithRows() {
        int id = createTable(SCHEMA, TABLE, "name varchar(32)");
        writeRow(id, "foo");
        writeRow(id, new Object[]{null});
        writeRow(id, "bar");
        builder.implode(NULL_WRITER, TABLE_NAME);
        updateAISGeneration();
        assertTrue("Is builder table", builder.isBuilderTable(TABLE_NAME));
        id = tableId(TABLE_NAME);
        expectFullRows(
                id,
                createNewRow(id, 1L, "{\"name\":\"foo\"}"),
                createNewRow(id, 2L, "{\"name\":null}"),
                createNewRow(id, 3L, "{\"name\":\"bar\"}")
        );
    }

    @Test(expected=ModelBuilderException.class)
    public void implodeBuilderTable() {
        builder.create(TABLE_NAME);
        builder.implode(NULL_WRITER, TABLE_NAME);
    }

    @Test(expected=NoSuchTableException.class)
    public void implodeNoSuchTable() {
        builder.implode(NULL_WRITER, TABLE_NAME);
    }

    @Test(expected=ModelBuilderException.class)
    public void explodeEmpty() throws IOException {
        builder.create(TABLE_NAME);
        builder.explode(NULL_WRITER, TABLE_NAME);
    }

    @Test(expected=NoSuchTableException.class)
    public void explodeNoSuchTable() throws IOException {
        builder.explode(NULL_WRITER, TABLE_NAME);
    }

    @Test(expected=NoSuchTableException.class)
    public void checkNoSuchTable() {
        builder.isBuilderTable(TABLE_NAME);
    }

    @Test(expected=ModelBuilderException.class)
    public void createConflictsExisting() {
        createTable(SCHEMA, TABLE, "foo int");
        builder.create(TABLE_NAME);
    }
}
