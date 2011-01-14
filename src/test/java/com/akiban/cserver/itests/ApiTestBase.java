package com.akiban.cserver.itests;

import static org.junit.Assert.*;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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

    static class ListRowOutput implements RowOutput {
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

    protected final DMLFunctions dml() {
        return dml;
    }

    protected final DDLFunctions ddl() {
        return ddl;
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
        expectRowCount(tableId, expectedRows.length);
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

        @Override
        public InvalidOperationException getCause() {
            assert super.getCause() == cause;
            return cause;
        }
    }
}
