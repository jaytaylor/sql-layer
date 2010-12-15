package com.akiban.test.cserverapi;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.TableStatistics;
import com.akiban.cserver.api.*;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.NoSuchTableException;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.scan.*;
import com.akiban.cserver.service.Service;
import com.akiban.cserver.service.ServiceManager;
import com.akiban.cserver.service.ServiceManagerImpl;
import com.akiban.cserver.service.logging.LoggingServiceImpl;
import com.akiban.cserver.service.network.NetworkService;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.service.session.SessionImpl;
import com.akiban.cserver.service.session.UnitTestServiceManagerFactory;
import org.junit.After;
import org.junit.Before;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;

import static junit.framework.Assert.*;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * <p>Base class for all API tests. Contains a @SetUp that gives you a fresh DDLFunctions and DMLFunctions, plus
 * various convenience testing methods.</p>
 */
public class ApiTestBase {

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

    private static class TestServiceManagerFactory extends UnitTestServiceManagerFactory {

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
            super(new TestServiceManagerFactory());
        }
    }

    protected ApiTestBase()
    {}

    private DMLFunctions dml;
    private DDLFunctions ddl;
    private ServiceManager sm;

    @Before
    public void setUp() throws Exception {
        sm = new TestServiceManager( );
        sm.startServices();
        dml = new DMLFunctionsImpl(sm.getStore(), new LoggingServiceImpl());
        ddl = new DDLFunctionsImpl(sm.getStore());
    }

    @After
    public void tearDown() throws Exception {
        sm.stopServices();
    }

    protected final DMLFunctions dml() {
        return dml;
    }

    protected final DDLFunctions ddl() {
        return ddl;
    }

    protected final TableId createTable(String schema, String table, String definition) throws InvalidOperationException {
        ddl().createTable(schema, String.format("CREATE TABLE %s (%s)", table, definition));
        return getTableId(schema, table);
    }

    /**
     * Expects an exact number of rows. This checks both the countRowExactly and countRowsApproximately
     * methods on DMLFunctions.
     * @param tableId the table to count
     * @param rowsExpected how many rows we expect
     * @throws InvalidOperationException for various reasons :)
     */
    protected void expectRowCount(TableId tableId, long rowsExpected) throws InvalidOperationException {
        TableStatistics tableStats = dml().getTableStatistics(tableId, true);
        assertEquals("table ID", tableId.getTableId(null), tableStats.getRowDefId());
        assertEquals("rows by TableStatistics", rowsExpected, tableStats.getRowCount());
    }

    protected final TableId getTableId(String schema, String table) {
        AkibaInformationSchema ais = ddl().getAIS();
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

    protected List<NewRow> scanAll(ScanRequest request) throws InvalidOperationException {
        Session session = new SessionImpl();
        ListRowOutput output = new ListRowOutput();
        CursorId cursorId = dml().openCursor(request, session);

        while(dml().scanSome(cursorId, session, output, -1))
        {}
        dml().closeCursor(cursorId, session);

        return output.getRows();
    }

    protected void writeRows(NewRow... rows) throws InvalidOperationException {
        for (NewRow row : rows) {
            dml().writeRow(row);
        }
    }

    protected void expectRows(ScanRequest request, NewRow... expectedRows) throws InvalidOperationException {
        assertEquals("rows scanned", Arrays.asList(expectedRows), scanAll(request));
    }

    protected void expectFullRows(TableId tableId, NewRow... expectedRows) throws InvalidOperationException {
        Table uTable = ddl().getAIS().getTable( ddl().getTableName(tableId) );
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
}
