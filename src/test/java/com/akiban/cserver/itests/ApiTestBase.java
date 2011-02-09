/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.cserver.itests;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.*;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.store.PersistitStore;
import com.akiban.cserver.service.memcache.MemcacheService;
import com.akiban.cserver.store.Store;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;

import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.TableStatistics;
import com.akiban.cserver.api.DDLFunctions;
import com.akiban.cserver.api.DDLFunctionsImpl;
import com.akiban.cserver.api.DMLFunctions;
import com.akiban.cserver.api.DMLFunctionsImpl;
import com.akiban.cserver.api.HapiProcessor;
import com.akiban.cserver.api.common.NoSuchTableException;
import com.akiban.cserver.api.dml.scan.CursorId;
import com.akiban.cserver.api.dml.scan.NewRow;
import com.akiban.cserver.api.dml.scan.NiceRow;
import com.akiban.cserver.api.dml.scan.RowOutput;
import com.akiban.cserver.api.dml.scan.ScanAllRequest;
import com.akiban.cserver.api.dml.scan.ScanRequest;
import com.akiban.cserver.service.Service;
import com.akiban.cserver.service.ServiceManager;
import com.akiban.cserver.service.ServiceManagerImpl;
import com.akiban.cserver.service.UnitTestServiceFactory;
import com.akiban.cserver.service.network.NetworkService;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.service.session.SessionImpl;

/**
 * <p>Base class for all API tests. Contains a @SetUp that gives you a fresh DDLFunctions and DMLFunctions, plus
 * various convenience testing methods.</p>
 */
public class ApiTestBase {

    public static class ListRowOutput implements RowOutput {
        private final List<NewRow> rows = new ArrayList<NewRow>();

        @Override
        public void output(NewRow row) {
            rows.add(row);
        }

        public List<NewRow> getRows() {
            return rows;
        }
    }

    private static class TestServiceServiceFactory extends UnitTestServiceFactory {

        private TestServiceServiceFactory() {
            super(false, null);
        }

        @Override
        public Service<NetworkService> networkService() {
            return new Service<NetworkService>() {
                @Override
                public NetworkService cast() {
                    return (NetworkService) Proxy.newProxyInstance(NetworkService.class.getClassLoader(),
                            new Class<?>[]{NetworkService.class},
                            new InvocationHandler() {
                                @Override
                                public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                                    return null;
                                }
                            }
                    );
                }

                @Override
                public Class<NetworkService> castClass() {
                    return NetworkService.class;
                }

                @Override
                public void start() throws Exception {
                }

                @Override
                public void stop() throws Exception {
                }
            };
        }
    }

    private static class TestServiceManager extends ServiceManagerImpl {
        private TestServiceManager() {
            super(new TestServiceServiceFactory());
        }
    }

    protected ApiTestBase()
    {}

    private DMLFunctions dml;
    private DDLFunctions ddl;
    private ServiceManager sm;
    protected Session session;

    @Before
    public final void startTestServices() throws Exception {
        session = new SessionImpl();
        sm = new TestServiceManager( );
        sm.startServices();
        ddl = new DDLFunctionsImpl();
        dml = new DMLFunctionsImpl(ddl);
    }

    @After
    public final void stopTestServices() throws Exception {
        sm.stopServices();
        ddl = null;
        dml = null;
        sm = null;
        session = null;
    }

    protected final HapiProcessor hapi() {
        return sm.getMemcacheService();
    }
    
    protected final DMLFunctions dml() {
        return dml;
    }

    protected final DDLFunctions ddl() {
        return ddl;
    }

    protected final Store store() {
        return sm.getStore();
    }

    protected final PersistitStore persistitStore() {
        return (PersistitStore) sm.getStore();
    }

    protected final MemcacheService memcache() {
        return sm.getMemcacheService();
    }

    protected final RowDefCache rowDefCache() {
        Store store = sm.getStore();
        return store.getRowDefCache();
    }

    protected final int createTable(String schema, String table, String definition) throws InvalidOperationException {
        ddl().createTable(session, schema, String.format("CREATE TABLE %s (%s)", table, definition));
        return ddl().getTableId(session, new TableName(schema, table));
    }

    protected final int createTable(String schema, String table, String... definitions) throws InvalidOperationException {
        assertTrue("must have at least one definition element", definitions.length >= 1);
        StringBuilder unifiedDef = new StringBuilder();
        for (String definition : definitions) {
            unifiedDef.append(definition).append(',');
        }
        unifiedDef.setLength( unifiedDef.length() - 1);
        return createTable(schema, table, unifiedDef.toString());
    }

    /**
     * Expects an exact number of rows. This checks both the countRowExactly and countRowsApproximately
     * methods on DMLFunctions.
     * @param tableId the table to count
     * @param rowsExpected how many rows we expect
     * @throws InvalidOperationException for various reasons :)
     */
    protected final void expectRowCount(int tableId, long rowsExpected) throws InvalidOperationException {
        TableStatistics tableStats = dml().getTableStatistics(session, tableId, true);
        assertEquals("table ID", tableId, tableStats.getRowDefId());
        assertEquals("rows by TableStatistics", rowsExpected, tableStats.getRowCount());
    }

    protected static RuntimeException unexpectedException(Throwable cause) {
        return new RuntimeException("unexpected exception", cause);
    }

    protected final List<NewRow> scanAll(ScanRequest request) throws InvalidOperationException {
        Session session = new SessionImpl();
        ListRowOutput output = new ListRowOutput();
        CursorId cursorId = dml().openCursor(session, request);

        while(dml().scanSome(session, cursorId, output, -1))
        {}
        dml().closeCursor(session, cursorId);

        return output.getRows();
    }

    protected final void writeRows(NewRow... rows) throws InvalidOperationException {
        for (NewRow row : rows) {
            dml().writeRow(session, row);
        }
    }

    protected final void expectRows(ScanRequest request, NewRow... expectedRows) throws InvalidOperationException {
        assertEquals("rows scanned", Arrays.asList(expectedRows), scanAll(request));
    }

    protected final ScanAllRequest scanAllRequest(int tableId) throws NoSuchTableException {
        Table uTable = ddl().getTable(session, tableId);
        Set<Integer> allCols = new HashSet<Integer>();
        for (int i=0, MAX=uTable.getColumns().size(); i < MAX; ++i) {
            allCols.add(i);
        }
        return new ScanAllRequest(tableId, allCols);
    }

    protected final void expectFullRows(int tableId, NewRow... expectedRows) throws InvalidOperationException {
        ScanRequest all = scanAllRequest(tableId);
        expectRows(all, expectedRows);
//        expectRowCount(tableId, expectedRows.length); TODO broken pending fix to bug 703136
    }

    protected final List<NewRow> convertRowDatas(List<RowData> rowDatas) throws NoSuchTableException {
        List<NewRow> ret = new ArrayList<NewRow>(rowDatas.size());
        for(RowData rowData : rowDatas) {
            NewRow newRow = NiceRow.fromRowData(rowData, ddl().getRowDef(rowData.getRowDefId()));
            ret.add(newRow);
        }
        return ret;
    }

    protected static Set<CursorId> cursorSet(CursorId... cursorIds) {
        Set<CursorId> set = new HashSet<CursorId>();
        for (CursorId id : cursorIds) {
            if(!set.add(id)) {
                fail(String.format("while adding %s to %s", id, set));
            }
        }
        return set;
    }

    protected static NewRow createNewRow(int tableId, Object... columns) {
        NewRow row = new NiceRow(tableId);
        for (int i=0; i < columns.length; ++i) {
            row.put(i, columns[i] );
        }
        return row;
    }

    protected final void dropAllTables() throws InvalidOperationException {
        // Can't drop a parent before child. Get all to drop and sort children first (they always have higher id).
        List<Integer> allIds = new ArrayList<Integer>();
        for (Map.Entry<TableName, UserTable> entry : ddl().getAIS(session).getUserTables().entrySet()) {
            if (!"akiba_information_schema".equals(entry.getKey().getSchemaName())) {
                allIds.add(entry.getValue().getTableId());
            }
        }
        Collections.sort(allIds, Collections.reverseOrder());
        for (Integer id : allIds) {
            ddl().dropTable(session, tableName(id));
        }
        Set<TableName> uTables = new HashSet<TableName>(ddl().getAIS(session).getUserTables().keySet());
        for (Iterator<TableName> iter = uTables.iterator(); iter.hasNext();) {
            if ("akiba_information_schema".equals(iter.next().getSchemaName())) {
                iter.remove();
            }
        }
        Assert.assertEquals("user tables", Collections.<TableName>emptySet(), uTables);
    }

    protected static class TestException extends RuntimeException {
        private final InvalidOperationException cause;

        public TestException(String message, InvalidOperationException cause) {
            super(message, cause);
            this.cause = cause;
        }

        public TestException(InvalidOperationException cause) {
            super(cause);
            this.cause = cause;
        }

        @Override
        public InvalidOperationException getCause() {
            assert super.getCause() == cause;
            return cause;
        }
    }

    protected final int tableId(String schema, String table) {
        return tableId(new TableName(schema, table));
    }

    protected final int tableId(TableName tableName) {
        try {
            return ddl().getTableId(session, tableName);
        } catch (NoSuchTableException e) {
            throw new TestException(e);
        }
    }

    protected final TableName tableName(int tableId) {
        try {
            return ddl().getTableName(session, tableId);
        } catch (NoSuchTableException e) {
            throw new TestException(e);
        }
    }

    protected final TableName tableName(String schema, String table) {
        return new TableName(schema, table);
    }

    protected final UserTable getUserTable(int tableId) {
        final Table table;
        try {
            table = ddl().getTable(session, tableId);
        } catch (NoSuchTableException e) {
            throw new TestException(e);
        }
        if (table.isUserTable()) {
            return (UserTable) table;
        }
        throw new RuntimeException("not a user table: " + table);
    }

    protected final Map<TableName,UserTable> getUserTables() {
        return stripAISTables(ddl().getAIS(session).getUserTables());
    }

    protected final Map<TableName,GroupTable> getGroupTables() {
        return stripAISTables(ddl().getAIS(session).getGroupTables());
    }

    private static <T extends Table> Map<TableName,T> stripAISTables(Map<TableName,T> map) {
        final Map<TableName,T> ret = new HashMap<TableName, T>(map);
        for(Iterator<TableName> iter=ret.keySet().iterator(); iter.hasNext(); ) {
            if("akiba_information_schema".equals(iter.next().getSchemaName())) {
                iter.remove();
            }
        }
        return ret;
    }

    protected void expectIndexes(int tableId, String... expectedIndexNames) {
        UserTable table = getUserTable(tableId);
        Set<String> expectedIndexesSet = new TreeSet<String>(Arrays.asList(expectedIndexNames));
        Set<String> actualIndexes = new TreeSet<String>();
        for (Index index : table.getIndexes()) {
            String indexName = index.getIndexName().getName();
            boolean added = actualIndexes.add(indexName);
            assertTrue("duplicate index name: " + indexName, added);
        }
        assertEquals("indexes in " + table.getName(), expectedIndexesSet, actualIndexes);
    }

    protected void expectIndexColumns(int tableId, String indexName, String... expectedColumns) {
        UserTable table = getUserTable(tableId);
        List<String> expectedColumnsList = Arrays.asList(expectedColumns);
        Index index = table.getIndex(indexName);
        assertNotNull(indexName + " was null", index);
        List<String> actualColumns = new ArrayList<String>();
        for (IndexColumn indexColumn : index.getColumns()) {
            actualColumns.add(indexColumn.getColumn().getName());
        }
        assertEquals(indexName + " columns", actualColumns, expectedColumnsList);
    }
}
