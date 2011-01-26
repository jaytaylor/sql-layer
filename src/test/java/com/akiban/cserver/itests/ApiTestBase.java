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
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.store.Store;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.cserver.CServerTestCase;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.TableStatistics;
import com.akiban.cserver.api.DDLFunctions;
import com.akiban.cserver.api.DDLFunctionsImpl;
import com.akiban.cserver.api.DMLFunctions;
import com.akiban.cserver.api.DMLFunctionsImpl;
import com.akiban.cserver.api.HapiProcessor;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.NoSuchTableException;
import com.akiban.cserver.api.common.TableId;
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
import com.akiban.cserver.service.logging.LoggingServiceImpl;
import com.akiban.cserver.service.network.NetworkService;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.service.session.SessionImpl;

/**
 * <p>Base class for all API tests. Contains a @SetUp that gives you a fresh DDLFunctions and DMLFunctions, plus
 * various convenience testing methods.</p>
 */
public class ApiTestBase extends CServerTestCase {

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

    @Before
    public final void startTestServices() throws Exception {
        sm = new TestServiceManager( );
        sm.startServices();
        dml = new DMLFunctionsImpl(new LoggingServiceImpl());
        ddl = new DDLFunctionsImpl();
    }

    @After
    public final void stopTestServices() throws Exception {
        sm.stopServices();
        ddl = null;
        dml = null;
        sm = null;
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

    protected final RowDefCache rowDefCache() {
        Store store = sm.getStore();
        return store.getRowDefCache();
    }

    protected final TableId createTable(String schema, String table, String definition) throws InvalidOperationException {
        ddl().createTable(session, schema, String.format("CREATE TABLE %s (%s)", table, definition));
        return getTableId(schema, table);
    }

    protected final TableId createTable(String schema, String table, String... definitions) throws InvalidOperationException {
        assertTrue("must have at least one definition element", definitions.length >= 1);
        StringBuilder unifiedDef = new StringBuilder();
        for (String definition : definitions) {
            unifiedDef.append(definition).append(',');
        }
        unifiedDef.setLength( unifiedDef.length() - 1);
        return createTable(schema, table, unifiedDef.toString());
    }

    /**
     * Creates a new NewRow by copying the old one. This creates a new TableId for the new row using the incoming
     * row's table name, which means that any ID resolution that's done on one NewRow won't affect the other. The
     * incoming row must be resolvable to TableName without a resolver.
     * @param row the row to copy
     * @return a new NewRow, with a new TableId
     */
    protected final NewRow copyRow(NewRow row) {
        final TableName tableName;
        try {
            tableName = ddl().resolveTableId(row.getTableId()).getTableName(null);
        } catch (NoSuchTableException e) {
            throw new TestException(e);
        }

        TableId tableId = TableId.of(tableName.getSchemaName(), tableName.getTableName());
        NewRow ret = new NiceRow(tableId);
        for (Map.Entry<ColumnId,Object> field : row.getFields().entrySet()) {
            ret.put(field.getKey(), field.getValue());
        }
        return ret;
    }

    /**
     * Expects an exact number of rows. This checks both the countRowExactly and countRowsApproximately
     * methods on DMLFunctions.
     * @param tableId the table to count
     * @param rowsExpected how many rows we expect
     * @throws InvalidOperationException for various reasons :)
     */
    protected final void expectRowCount(TableId tableId, long rowsExpected) throws InvalidOperationException {
        TableStatistics tableStats = dml().getTableStatistics(session, tableId, true);
        assertEquals("table ID", tableId.getTableId(null), tableStats.getRowDefId());
        assertEquals("rows by TableStatistics", rowsExpected, tableStats.getRowCount());
    }

    protected final TableId getTableId(String schema, String table) {
        AkibaInformationSchema ais = ddl().getAIS(session);
        assertNotNull("ais was null", ais);
        UserTable uTable = ais.getUserTable(schema, table);
        assertNotNull("utable was null", uTable);

        TableId byName = TableId.of(schema, table);
        TableId byId = TableId.of(uTable.getTableId());
        try {
            assertEquals("table ID name", TableName.create(schema, table), ddl().getTableName(byId) );
            assertEquals("table ID int", byId.getTableId(null), ddl.resolveTableId(byName).getTableId(null));
            assertEquals("table IDs", byId, byName);
            assertEquals("table IDs hash", byId.hashCode(), byName.hashCode());
        } catch (NoSuchTableException e) {
            throw new RuntimeException(e);
        }
        return byId;
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

    protected final void expectFullRows(TableId tableId, NewRow... expectedRows) throws InvalidOperationException {
        Table uTable = ddl().getAIS(session).getTable( ddl().getTableName(tableId) );
        Set<ColumnId> allCols = new HashSet<ColumnId>();
        for (int i=0, MAX=uTable.getColumns().size(); i < MAX; ++i) {
            allCols.add( ColumnId.of(i) );
        }
        ScanRequest all = new ScanAllRequest(tableId, allCols);
        expectRows(all, expectedRows);
//        expectRowCount(tableId, expectedRows.length); TODO broken pending fix to bug 703136
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

    protected static NewRow createNewRow(TableId tableId, Object... columns) {
        NewRow row = new NiceRow(tableId);
        for (int i=0; i < columns.length; ++i) {
            row.put(ColumnId.of(i), columns[i] );
        }
        return row;
    }

    protected final void dropAllTables() throws InvalidOperationException {
        for (TableName tableName : ddl().getAIS(session).getUserTables().keySet()) {
            if (!"akiba_information_schema".equals(tableName.getSchemaName())) {
                ddl().dropTable(session, TableId.of(tableName.getSchemaName(), tableName.getTableName()));
            }
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

    protected final UserTable getUserTable(TableId tableId) {
        try {
            TableName tableName = ddl().getTableName(tableId);
            return ddl().getAIS(session).getUserTable(tableName);
        } catch (NoSuchTableException e) {
            throw new TestException(e);
        }
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

    protected void expectIndexes(TableId tableId, String... expectedIndexNames) {
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

    protected void expectIndexColumns(TableId tableId, String indexName, String... expectedColumns) {
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
