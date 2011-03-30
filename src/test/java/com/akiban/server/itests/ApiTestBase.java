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

package com.akiban.server.itests;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.*;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;

import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.server.RowData;
import com.akiban.server.RowDefCache;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.RowDataOutput;
import com.akiban.server.service.config.Property;
import com.akiban.server.service.memcache.HapiProcessorFactory;
import com.akiban.server.store.PersistitStore;
import com.akiban.server.service.memcache.MemcacheService;
import com.akiban.server.store.Store;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;

import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.TableStatistics;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DDLFunctionsImpl;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.DMLFunctionsImpl;
import com.akiban.server.api.HapiProcessor;
import com.akiban.server.api.common.NoSuchTableException;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.api.dml.scan.RowOutput;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.api.dml.scan.ScanRequest;
import com.akiban.server.service.Service;
import com.akiban.server.service.ServiceManager;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.service.UnitTestServiceFactory;
import com.akiban.server.service.network.NetworkService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;

/**
 * <p>Base class for all API tests. Contains a @SetUp that gives you a fresh DDLFunctions and DMLFunctions, plus
 * various convenience testing methods.</p>
 */
public class ApiTestBase {

    public static class ListRowOutput implements RowOutput {
        private final List<NewRow> rows = new ArrayList<NewRow>();
        private final List<NewRow> rowsUnmodifiable = Collections.unmodifiableList(rows);

        @Override
        public void output(NewRow row) {
            rows.add(row);
        }

        public List<NewRow> getRows() {
            return rowsUnmodifiable;
        }

        public void clear() {
            rows.clear();
        }
    }

    public final static ColumnSelector ALL_COLUMNS = new ColumnSelector() {
        @Override
        public boolean includesColumn(int columnPosition) {
            return true;
        }
    };

    private static class TestServiceServiceFactory extends UnitTestServiceFactory {

        private TestServiceServiceFactory(Collection<Property> startupConfigProperties) {
            super(false, startupConfigProperties);
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
        private TestServiceManager(Collection<Property> startupConfigProperties) {
            super(new TestServiceServiceFactory(startupConfigProperties));
        }
    }

    protected ApiTestBase()
    {
        final String name = this.getClass().getSimpleName();
        assertTrue("Please name integration tests FooIT or FooMT instead of FooTest or somethign else",
                name.endsWith("IT") || name.endsWith("MT")
        );
    }

    private DMLFunctions dml;
    private DDLFunctions ddl;
    private ServiceManager sm;
    private Session session;

    @Before
    public final void startTestServices() throws Exception {
        session = new SessionImpl();
        sm = new TestServiceManager(startupConfigProperties());
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

    protected final HapiProcessor hapi(HapiProcessorFactory whichHapi) {
        memcache().setHapiProcessor(whichHapi);
        return hapi();
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

    protected final Session session() {
        return session;
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

    protected Collection<Property> startupConfigProperties() {
        return null;
    }

    protected final int createTable(String schema, String table, String definition) throws InvalidOperationException {
        ddl().createTable(session(), schema, String.format("CREATE TABLE %s (%s)", table, definition));
        return ddl().getTableId(session(), new TableName(schema, table));
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
        TableStatistics tableStats = dml().getTableStatistics(session(), tableId, true);
        assertEquals("table ID", tableId, tableStats.getRowDefId());
        assertEquals("rows by TableStatistics", rowsExpected, tableStats.getRowCount());
    }

    protected static RuntimeException unexpectedException(Throwable cause) {
        return new RuntimeException("unexpected exception", cause);
    }

    protected final List<RowData> scanFull(ScanRequest request) {
        try {
            return RowDataOutput.scanFull(session(), dml(), request);
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }
    }

    protected final List<NewRow> scanAll(ScanRequest request) throws InvalidOperationException {
        Session session = new SessionImpl();
        ListRowOutput output = new ListRowOutput();
        CursorId cursorId = dml().openCursor(session, request);

        while(dml().scanSome(session, cursorId, output))
        {}
        dml().closeCursor(session, cursorId);

        return output.getRows();
    }

    protected final void writeRows(NewRow... rows) throws InvalidOperationException {
        for (NewRow row : rows) {
            dml().writeRow(session(), row);
        }
    }

    protected final void expectRows(ScanRequest request, NewRow... expectedRows) throws InvalidOperationException {
        assertEquals("rows scanned", Arrays.asList(expectedRows), scanAll(request));
    }

    protected final ScanAllRequest scanAllRequest(int tableId) throws NoSuchTableException {
        Table uTable = ddl().getTable(session(), tableId);
        Set<Integer> allCols = new HashSet<Integer>();
        for (int i=0, MAX=uTable.getColumns().size(); i < MAX; ++i) {
            allCols.add(i);
        }
        return new ScanAllRequest(tableId, allCols);
    }

    protected static <T> Set<T> set(T... items) {
        return new HashSet<T>(Arrays.asList(items));
    }

    protected static <T> T get(NewRow row, int field, Class<T> castAs) {
        Object obj = row.get(field);
        return castAs.cast(obj);
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

    public static NewRow createNewRow(int tableId, Object... columns) {
        NewRow row = new NiceRow(tableId);
        for (int i=0; i < columns.length; ++i) {
            row.put(i, columns[i] );
        }
        return row;
    }

    protected final void dropAllTables() throws InvalidOperationException {
        // Can't drop a parent before child. Get all to drop and sort children first (they always have higher id).
        List<Integer> allIds = new ArrayList<Integer>();
        for (Map.Entry<TableName, UserTable> entry : ddl().getAIS(session()).getUserTables().entrySet()) {
            if (!"akiban_information_schema".equals(entry.getKey().getSchemaName())) {
                allIds.add(entry.getValue().getTableId());
            }
        }
        Collections.sort(allIds, Collections.reverseOrder());
        for (Integer id : allIds) {
            ddl().dropTable(session(), tableName(id));
        }
        Set<TableName> uTables = new HashSet<TableName>(ddl().getAIS(session()).getUserTables().keySet());
        for (Iterator<TableName> iter = uTables.iterator(); iter.hasNext();) {
            if ("akiban_information_schema".equals(iter.next().getSchemaName())) {
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
            return ddl().getTableId(session(), tableName);
        } catch (NoSuchTableException e) {
            throw new TestException(e);
        }
    }

    protected final TableName tableName(int tableId) {
        try {
            return ddl().getTableName(session(), tableId);
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
            table = ddl().getTable(session(), tableId);
        } catch (NoSuchTableException e) {
            throw new TestException(e);
        }
        if (table.isUserTable()) {
            return (UserTable) table;
        }
        throw new RuntimeException("not a user table: " + table);
    }

    protected final Map<TableName,UserTable> getUserTables() {
        return stripAISTables(ddl().getAIS(session()).getUserTables());
    }

    protected final Map<TableName,GroupTable> getGroupTables() {
        return stripAISTables(ddl().getAIS(session()).getGroupTables());
    }

    private static <T extends Table> Map<TableName,T> stripAISTables(Map<TableName,T> map) {
        final Map<TableName,T> ret = new HashMap<TableName, T>(map);
        for(Iterator<TableName> iter=ret.keySet().iterator(); iter.hasNext(); ) {
            if("akiban_information_schema".equals(iter.next().getSchemaName())) {
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
