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

package com.foundationdb.server.service.is;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.memoryadapter.MemoryAdapter;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.rowtype.ValuesRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.server.test.it.qp.TestRow;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.texpressions.TPreparedField;
import com.foundationdb.server.types.texpressions.TPreparedLiteral;

import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ServerSchemaTablesServiceIT extends ITBase
{
    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                    .bindAndRequire(ServerSchemaTablesService.class, ServerSchemaTablesServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    @Test
    public void examine() {
        AkibanInformationSchema ais = ais();
        assertEquals ("Table count", 12, ServerSchemaTablesServiceImpl.createTablesToRegister(ddl().getTypesTranslator()).getTables().size());
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.ERROR_CODES));
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.ERROR_CODE_CLASSES));
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.SERVER_INSTANCE_SUMMARY));
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.SERVER_SERVERS));
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.SERVER_SESSIONS));
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.SERVER_PARAMETERS));
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.SERVER_MEMORY_POOLS));
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.SERVER_GARBAGE_COLLECTORS));
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.SERVER_TAPS));
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.SERVER_PREPARED_STATEMENTS));
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.SERVER_CURSORS));
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.SERVER_USERS));
    }
    
    @Test
    public void testErrorCode() {
        final Object[][] expected = { 
        };
        
        checkLimitTable (expected, ServerSchemaTablesServiceImpl.ERROR_CODES, 0);
    }
    
    @Test
    public void testErrorClass() {
        final Object[][] expected = {
        };
        checkLimitTable (expected, ServerSchemaTablesServiceImpl.ERROR_CODE_CLASSES, 0);
    }
    
    @Test
    public void testServerInstanceSummary() {
        final Object[][] expected = {
                {null}
        };

        checkSubsetTable(expected, ServerSchemaTablesServiceImpl.SERVER_INSTANCE_SUMMARY, 0);
    }
    
    @Test
    public void testCursors() {
        final Object[][] expected = {
        };
        checkLimitTable (expected, ServerSchemaTablesServiceImpl.SERVER_CURSORS, 0);
    }

    @Test
    public void testGarbageCollectors() {
        final Object[][] expected = {
        };
        checkLimitTable (expected, ServerSchemaTablesServiceImpl.SERVER_GARBAGE_COLLECTORS, 0);
    }

    @Test
    public void testMemoryPools() {
        final Object[][] expected = {
        };
        checkLimitTable (expected, ServerSchemaTablesServiceImpl.SERVER_MEMORY_POOLS, 0);
    }

    @Test
    public void testServers() {
        final Object[][] expected = {
        };

        checkLimitTable(expected, ServerSchemaTablesServiceImpl.SERVER_SERVERS, 0);
    }

    @Test
    public void testServerSessions() {
        final Object[][] expected = {
        };
        checkLimitTable (expected, ServerSchemaTablesServiceImpl.SERVER_SESSIONS, 0);
    }
    
    @Test
    public void testServerParameters() {
        final Object[][] expected = {
        };
        checkLimitTable (expected, ServerSchemaTablesServiceImpl.SERVER_PARAMETERS, 0);
    }

    
    @Test
    public void testServerUsers() {
        final Object[][] expected = { 
        };
        
        checkTable(expected,ServerSchemaTablesServiceImpl.SERVER_USERS);
    }
    
    @Test
    public void testServerPreparedStatements() {
        final Object[][] expected = { 
        };
        
        checkTable(expected,ServerSchemaTablesServiceImpl.SERVER_PREPARED_STATEMENTS);
    }
    
    @Test
    public void testTaps() {
        final Object[][] expected = {
        };
        checkLimitTable (expected, ServerSchemaTablesServiceImpl.SERVER_TAPS, 0);
    }
    

    private void checkLimitTable (Object[][] expected, TableName tableName, int limit) 
    {
        Table table = ais().getTable(tableName);
        Schema schema = SchemaCache.globalSchema(ais());
        TableRowType rowType = schema.tableRowType(table);
        MemoryAdapter adapter = new MemoryAdapter(session(), configService());

        QueryContext queryContext = new SimpleQueryContext(adapter);
        Row[] rows = objectToRows(expected, rowType);

        Cursor cursor = API.cursor(
                API.limit_Default(API.groupScan_Default(table.getGroup()), limit), 
                queryContext, queryContext.createBindings());
        compareRows(rows, cursor, true);

    }
    
    private void checkSubsetTable(Object[][] expected, TableName tableName, int columnNum) 
    {
        Table table = ais().getTable(tableName);
        Schema schema = SchemaCache.globalSchema(ais());
        TableRowType rowType = schema.tableRowType(table);
        MemoryAdapter adapter = new MemoryAdapter(session(), configService());

        QueryContext queryContext = new SimpleQueryContext(adapter);
        
        List<TPreparedExpression> pExpressions = new ArrayList<>(1);
       
        Value value = new Value(rowType.typeAt(columnNum));
        value.putNull();
        TPreptimeValue ptval = new TPreptimeValue (value.getType(), value);
        pExpressions.add(new TPreparedLiteral(ptval.type(), ptval.value()));
        
        ValuesRowType expectedType = schema.newValuesType(rowType.typeAt(columnNum));
        
        Row[] rows = objectToRows(expected, expectedType);
        
        Cursor cursor = API.cursor(
                API.project_Default(
                        API.groupScan_Default(table.getGroup()),
                        rowType, pExpressions),
                    queryContext, 
                    queryContext.createBindings());
        
        compareRows(rows, cursor, true);
    }

    private void checkTable (Object[][] expected, TableName table) {
        Table serverTable = ais().getTable(table);
        Schema schema = SchemaCache.globalSchema(ais());
        TableRowType rowType = schema.tableRowType(serverTable);
        MemoryAdapter adapter = new MemoryAdapter(session(), configService());

        Row[] rows = objectToRows(expected, rowType);

        
        Group group = serverTable.getGroup();
        RowCursor cursor = adapter.newGroupCursor(group);
        compareRows(rows, cursor, false);
    }

    
    
    private Row[] objectToRows (Object[][] expected, RowType rowType) {
        Row[] rows = new Row[expected.length];
        for (int i = 0; i < expected.length; i++) {
            rows[i] = new TestRow(rowType, expected[i]);
        }
        return rows;
    }
}
