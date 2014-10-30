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

package com.foundationdb.server.test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;

import com.foundationdb.qp.expression.ExpressionRow;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.UpdateFunction;
import com.foundationdb.qp.row.BindableRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.NoSuchIndexException;
import com.foundationdb.server.store.FDBHolder;
import com.foundationdb.server.store.FDBStore;
import com.foundationdb.ais.AISCloner;
import com.foundationdb.ais.model.*;
import com.foundationdb.ais.model.Index.JoinType;
import com.foundationdb.ais.util.TableChangeValidator;
import com.foundationdb.server.test.it.qp.TestRow;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.texpressions.TPreparedLiteral;
import com.foundationdb.server.types.value.ValueRecord;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.sql.LayerInfoInterface;
import com.foundationdb.server.AkServerUtil;
import com.foundationdb.server.api.dml.scan.ScanFlag;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.rowdata.SchemaFactory;
import com.foundationdb.server.service.ServiceManagerImpl;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.config.TestConfigService;
import com.foundationdb.server.service.dxl.DXLService;
import com.foundationdb.server.service.dxl.DXLTestHookRegistry;
import com.foundationdb.server.service.dxl.DXLTestHooks;
import com.foundationdb.server.service.routines.RoutineLoader;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.service.TCastResolver;
import com.foundationdb.server.types.service.TypesRegistry;
import com.foundationdb.sql.RegexFilenameFilter;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.aisddl.AlterTableDDL;
import com.foundationdb.sql.optimizer.rule.PlanGenerator;
import com.foundationdb.sql.parser.AlterTableNode;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.util.AssertUtils;
import com.foundationdb.util.Exceptions;
import com.foundationdb.util.Strings;
import com.foundationdb.util.tap.TapReport;
import com.foundationdb.util.Undef;

import com.geophile.z.Space;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;

import com.foundationdb.server.store.Store;
import com.foundationdb.util.ListUtils;

import com.foundationdb.server.TableStatistics;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.api.DMLFunctions;
import com.foundationdb.server.api.dml.scan.CursorId;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.api.dml.scan.RowOutput;
import com.foundationdb.server.api.dml.scan.ScanAllRequest;
import com.foundationdb.server.api.dml.scan.ScanRequest;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.session.Session;
import org.junit.Rule;
import org.junit.rules.MethodRule;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

/**
 * <p>Base class for all API tests. Contains a @SetUp that gives you a fresh DDLFunctions and DMLFunctions, plus
 * various convenience testing methods.</p>
 */
public class ApiTestBase {
    private static final int MIN_FREE_SPACE = 256 * 1024 * 1024;
    private static final String TAPS = System.getProperty("it.taps");
    protected final static Object UNDEF = Undef.only();

    private static final Comparator<? super TapReport> TAP_REPORT_COMPARATOR = new Comparator<TapReport>() {
        @Override
        public int compare(TapReport o1, TapReport o2) {
            return o1.getName().compareTo(o2.getName());
        }
    };

    public static interface TestRowOutput extends RowOutput {
        public void clear();
    }

    public static class ListRowOutput implements TestRowOutput {
        private final List<NewRow> rows = new ArrayList<>();
        private final List<NewRow> rowsUnmodifiable = Collections.unmodifiableList(rows);
        private int mark = 0;

        @Override
        public void output(NewRow row) {
            rows.add(row);
        }
        
        public List<NewRow> getRows() {
            return rowsUnmodifiable;
        }

        @Override
        public void clear() {
            rows.clear();
        }

        @Override
        public void mark() {
            mark = rows.size();
        }

        @Override
        public void rewind() {
            ListUtils.truncate(rows, mark);
        }
    }

    private static class RetryRule implements MethodRule {
        private static int MAX_TRIES = 5;
        private static int totalRetries = 0;

        @Override
        public Statement apply(final Statement base, FrameworkMethod method, Object target) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    int tryCount = 1;
                    try {
                        base.evaluate();
                    } catch(Throwable t) {
                        if(++tryCount > MAX_TRIES || !Exceptions.isRollbackException(t)) {
                            throw t;
                        }
                        ++totalRetries;
                        needServicesRestart = true;
                    }
                }
            };
        }
    }


    protected ApiTestBase(String suffix)
    {
    }

    private static ServiceManager sm;
    private Session sharedSession;
    private int aisGeneration;
    private static Map<String,String> lastStartupConfigProperties = null;
    private static boolean needServicesRestart = false;
    protected static Set<Callable<Void>> beforeStopServices = new HashSet<>();

    @Rule
    public final TestName testName = new TestName();

    @Rule
    public final RetryRule retryRule = new RetryRule();


    protected String testName() {
        return testName.getMethodName();
    }

    private void clearFDBData() throws Exception {
        final FDBHolder holder = sm.getServiceByClass(FDBHolder.class);
        holder.getRootDirectory().remove(holder.getDatabase()).get();
    }

    @Before
    public final void startTestServices() throws Throwable {
        System.setProperty("fdbsql.home", System.getProperty("user.home"));
        try {
            Map<String, String> startupConfigProperties = startupConfigProperties();
            Map<String,String> propertiesForEquality = propertiesForEquality(startupConfigProperties);
            if (needServicesRestart ||
                lastStartupConfigProperties == null ||
                !lastStartupConfigProperties.equals(propertiesForEquality))
            {
                // we need a shutdown if we needed a restart, or if the lastStartupConfigProperties are not null,
                // which (because of the condition on the "if" above) implies the last startup config properties
                // are different from this one's
                boolean needShutdown = needServicesRestart || lastStartupConfigProperties != null;
                if (needShutdown) {
                    needServicesRestart = false; // clear the flag if it was set
                    stopTestServices();
                }
                int attempt = 1;
                while (!AkServerUtil.cleanUpDirectory(TestConfigService.dataDirectory())) {
                    assertTrue("Too many directory failures", (attempt++ < 10));
                    TestConfigService.newDataDirectory();
                }
                assertNull("lastStartupConfigProperties should be null", lastStartupConfigProperties);
                sm = createServiceManager(startupConfigProperties);

                // Clear out FDB's data, too.
                // TODO: Better way to shove this behind an interface?
                if(sm.serviceIsBoundTo(Store.class, FDBStore.class)) {
                    clearFDBData();
                }

                sm.startServices();
                ServiceManagerImpl.setServiceManager(sm);
                if (TAPS != null) {
                    sm.getStatisticsService().reset(TAPS);
                    sm.getStatisticsService().setEnabled(TAPS, true);
                }
                lastStartupConfigProperties = propertiesForEquality;
            }
            sharedSession = sm.getSessionService().createSession();
        } catch (Exception e) {
            handleStartupFailure(e);
        }
    }

    @Rule
    public TestRule exceptionCatchingRule = new TestWatcher() {
        @Override
        public void failed(Throwable e, Description description)  {
            needServicesRestart = true;
        }
    };

    /**
     * Handle a failure during services startup. The default implementation is to just throw the exception, and
     * most tests should <em>not</em> override this. It's designed solely as a testing hook for FailureOnStartupIT.
     * @param e the startup exception
     * @throws Exception the startup exception
     */
    void handleStartupFailure(Exception e) throws Exception {
        if(sm != null && sm.getState() != ServiceManager.State.IDLE) {
            sm.stopServices();
        }
        throw e;
    }

    protected ServiceManager createServiceManager(Map<String, String> startupConfigProperties) {
        TestConfigService.setOverrides(startupConfigProperties);
        return new GuicedServiceManager(serviceBindingsProvider());
    }

    /** Specify special service bindings.
     * If you override this, you need to override {@link #startupConfigProperties} 
     * to return something different so that the special services aren't shared 
     * with other tests.
     */
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return GuicedServiceManager.testUrls();
    }

    @After
    public final void tearDownAllTables() throws Exception {
        if (lastStartupConfigProperties == null)
            return; // services never started up
        String openCursorsMessage = null;
        if (sm.serviceIsStarted(DXLService.class)) {
            dropAllTables();
            DXLTestHooks dxlTestHooks = DXLTestHookRegistry.get();
            // Check for any residual open cursors
            if (dxlTestHooks.openCursorsExist()) {
                openCursorsMessage = "open cursors remaining:" + dxlTestHooks.describeOpenCursors();
            }
        }
        if (TAPS != null) {
            TapReport[] reports = sm.getStatisticsService().getReport(TAPS);
            Arrays.sort(reports, TAP_REPORT_COMPARATOR);
            for (TapReport report : reports) {
                long totalNanos = report.getCumulativeTime();
                double totalSecs = ((double) totalNanos) / 1000000000.0d;
                double secsPer = totalSecs / report.getOutCount();
                System.err.printf("%s:\t in=%d out=%d time=%.2f sec (%d nanos, %.5f sec per out)%n",
                        report.getName(),
                        report.getInCount(),
                        report.getOutCount(),
                        totalSecs,
                        totalNanos,
                        secsPer
                );
            }
        }
        sharedSession.close();

        if (openCursorsMessage != null) {
            fail(openCursorsMessage);
        }
        
        needServicesRestart |= runningOutOfSpace();
    }
    
    private static boolean runningOutOfSpace() {
        return TestConfigService.dataDirectory().getFreeSpace() < MIN_FREE_SPACE;
    }

    private static void beforeStopServices() throws Exception {
        for (Callable<Void> callable : beforeStopServices) {
            callable.call();
        }
    }

    public final void stopTestServices() throws Exception {
        beforeStopServices();
        ServiceManagerImpl.setServiceManager(null);
        lastStartupConfigProperties = null;
        sm.stopServices();
    }

    public final void restartTestServices(Map<String, String> properties) throws Exception {
        ServiceManagerImpl.setServiceManager(null);
        sm = createServiceManager( properties );
        sm.startServices();
        sharedSession = sm.getSessionService().createSession();
        lastStartupConfigProperties = propertiesForEquality(properties);
        ddl(); // loads up the schema manager et al
        ServiceManagerImpl.setServiceManager(sm);
    }

    public final Session createNewSession()
    {
        return sm.getSessionService().createSession();
    }

    protected Map<String, String> defaultPropertiesToPreserveOnRestart() {
        return Collections.singletonMap(
                TestConfigService.DATA_PATH_KEY,
                TestConfigService.dataDirectory().getAbsolutePath());
    }

    protected boolean defaultDoCleanOnUnload() {
        return true;
    }

    public final void safeRestartTestServices() throws Exception {
        safeRestartTestServices(defaultPropertiesToPreserveOnRestart());
    }

    public void safeRestartTestServices(Map<String, String> propertiesToPreserve) throws Exception {
        final boolean original = TestConfigService.getDoCleanOnUnload();
        try {
            TestConfigService.setDoCleanOnUnload(defaultDoCleanOnUnload());
            stopTestServices();
        } finally {
            TestConfigService.setDoCleanOnUnload(original);
        }
        restartTestServices(propertiesToPreserve);
    }
    
    protected final DMLFunctions dml() {
        return dxl().dmlFunctions();
    }

    protected final DDLFunctions ddl() {
        return dxl().ddlFunctions();
    }

    protected final Store store() {
        return sm.getStore();
    }

    protected final LayerInfoInterface layerInfo() {
        return sm.getLayerInfo();
    }

    protected String akibanFK(String childCol, String parentTable, String parentCol) {
        return String.format("GROUPING FOREIGN KEY (%s) REFERENCES \"%s\" (%s)",
                             childCol, parentTable, parentCol
        );
    }

    protected final Session session() {
        return sharedSession;
    }

    protected final StoreAdapter newStoreAdapter(Schema schema) {
        return newStoreAdapter(session(), schema);
    }

    protected final StoreAdapter newStoreAdapter(Session session, Schema schema) {
        return store().createAdapter(session, schema);
    }

    protected final QueryContext queryContext(StoreAdapter adapter) {
        return new SimpleQueryContext(adapter) {
                @Override
                public ServiceManager getServiceManager() {
                    return sm;
                }
            };
    }

    protected final AkibanInformationSchema ais() {
        return ais(session());
    }

    protected final AkibanInformationSchema ais(Session session) {
        return ddl().getAIS(session);
    }

    protected final AISCloner aisCloner() {
        return ddl().getAISCloner();
    }

    protected final ServiceManager serviceManager() {
        return sm;
    }

    protected final TypesRegistryService typesRegistryService() {
        return sm.getServiceByClass(TypesRegistryService.class);
    }

    protected final TypesRegistry typesRegistry() {
        return typesRegistryService().getTypesRegistry();
    }

    protected final TCastResolver castResolver() {
        return typesRegistryService().getCastsResolver();
    }

    protected final ConfigurationService configService() {
        return sm.getConfigurationService();
    }

    protected final DXLService dxl() {
        return sm.getDXL();
    }

    protected final TransactionService txnService() {
        return sm.getServiceByClass(TransactionService.class);
    }

    protected final RoutineLoader routineLoader() {
        return sm.getServiceByClass(RoutineLoader.class);
    }

    protected SecurityService securityService() {
        return sm.getServiceByClass(SecurityService.class);
    }

    protected final int aisGeneration() {
        return aisGeneration;
    }

    protected final void updateAISGeneration() {
        aisGeneration = ddl().getGenerationAsInt(session());
    }

    protected Map<String, String> startupConfigProperties() {
        return Collections.emptyMap();
    }

    // Property.equals() does not include the value.
    protected Map<String,String> propertiesForEquality(Map<String, String> properties) {
        Map<String,String> result = new HashMap<>(properties.size());
        for (Map.Entry<String, String> p : properties.entrySet()) {
            result.put(p.getKey(), p.getValue());
        }
        return result;
    }

    /**
     * A simple unique (per class) property that can be returned for tests
     * overriding the {@link #  } and/or
     * {@link #serviceBindingsProvider()} methods.
     */
    protected Map<String, String> uniqueStartupConfigProperties(Class clazz) {
        return Collections.singletonMap(
                "test.services",
                clazz.getName()
        );
    }

    protected void createFromDDL(String schema, String ddl) {
        SchemaFactory schemaFactory = new SchemaFactory(schema);
        schemaFactory.ddl(ddl(), session(), ddl);
        updateAISGeneration();
    }

    protected static final class SimpleColumn {
        public final String columnName;
        public final String typeName;
        public final Long param1;
        public final Long param2;

        public SimpleColumn(String columnName, String typeName) {
            this(columnName, typeName, null, null);
        }

        public SimpleColumn(String columnName, String typeName, Long param1, Long param2) {
            this.columnName = columnName;
            this.typeName = typeName;
            this.param1 = param1;
            this.param2 = param2;
        }

        public TInstance getType(TypesRegistry typesRegistry, boolean nullable) {
            int idx = typeName.indexOf("_ ");
            if (idx < 0)
                throw new IllegalArgumentException("type name must be fully qualified: <bundle>_ <name>: " + typeName);
            return typesRegistry.getType(typeName.substring(0, idx),
                                         typeName.substring(idx+2),
                                         param1, param2, nullable,
                                         null, null, columnName);
        }
    }

    protected void runAlter(String schema, QueryContext queryContext, String sql) {
        SQLParser parser = new SQLParser();
        StatementNode node;
        try {
            node = parser.parseStatement(sql);
        } catch(StandardException e) {
            throw new RuntimeException(e);
        }
        org.junit.Assert.assertTrue("is alter node", node instanceof AlterTableNode);
        AlterTableDDL.alterTable(ddl(), dml(), session(), schema, (AlterTableNode) node, queryContext);
        updateAISGeneration();
    }

    protected final int createTableFromTypes(String schema, String table, boolean firstIsPk, boolean createIndexes,
                                             SimpleColumn... columns) {
        TypesRegistry typesRegistry = typesRegistry();
        AISBuilder builder = new AISBuilder();
        builder.table(schema, table);

        int colPos = 0;
        SimpleColumn pk = firstIsPk ? columns[0] : new SimpleColumn("id", "MCOMPAT_ int");
        builder.column(schema, table, pk.columnName, colPos++, pk.getType(typesRegistry, false), false, null, null);
        builder.pk(schema, table);
        builder.indexColumn(schema, table, Index.PRIMARY, pk.columnName, 0, true, null);

        for(int i = firstIsPk ? 1 : 0; i < columns.length; ++i) {
            SimpleColumn sc = columns[i];
            String name = sc.columnName == null ? "c" + (colPos + 1) : sc.columnName;
            builder.column(schema, table, name, colPos++, sc.getType(typesRegistry, true), false, null, null);

            if(createIndexes) {
                builder.index(schema, table, name);
                builder.indexColumn(schema, table, name, name, 0, true, null);
            }
        }

        Table tempTable = builder.akibanInformationSchema().getTable(schema, table);
        ddl().createTable(session(), tempTable);
        updateAISGeneration();
        return tableId(schema, table);
    }
    
    protected final int createTableFromTypes(String schema, String table, boolean firstIsPk, boolean createIndexes,
                                             String... typeNames) {
        SimpleColumn simpleColumns[] = new SimpleColumn[typeNames.length];
        for(int i = 0; i < typeNames.length; ++i) {
            simpleColumns[i] = new SimpleColumn(null, typeNames[i]);
        }
        return createTableFromTypes(schema, table, firstIsPk, createIndexes, simpleColumns);
    }

    protected final int createTable(String schema, String table, String definition) throws InvalidOperationException {
        String ddl = String.format("CREATE TABLE \"%s\" (%s)", table, definition);
        createFromDDL(schema, ddl);
        return ddl().getTableId(session(), new TableName(schema, table));
    }

    protected final int createTable(String schema, String table, String... definitions) throws InvalidOperationException {
        assertTrue("must have at least one definition element", definitions.length >= 1);
        StringBuilder unifiedDef = new StringBuilder();
        for (String definition : definitions) {
            unifiedDef.append(definition).append(',');
        }
        unifiedDef.setLength(unifiedDef.length() - 1);
        return createTable(schema, table, unifiedDef.toString());
    }
    
    protected final void createSequence (String schema, String name, String definition) {
        String ddl = String.format("CREATE SEQUENCE %s %s", name, definition);
        createFromDDL(schema, ddl);
    }

    protected final void createView(String schema, String name, String definition) {
        String ddl = String.format("CREATE VIEW %s AS %s", name, definition);
        createFromDDL(schema, ddl);
    }

    protected final int createTable(TableName tableName, String... definitions) throws InvalidOperationException {
        return createTable(tableName.getSchemaName(), tableName.getTableName(), definitions);
    }

    private void createUniqueIndexInternal(String schema,
                                           String table,
                                           String indexName,
                                           String... indexCols) {
        createIndexInternal(schema, table, indexName, true, indexCols);
    }

    private void createIndexInternal(String schema,
                                     String table,
                                     String indexName,
                                     String... indexCols) {
        createIndexInternal(schema, table, indexName, false, indexCols);
    }

    private void createIndexInternal(String schema,
                                     String table,
                                     String indexName,
                                     boolean unique,
                                     String... indexCols) {
        String ddl = String.format("CREATE %s INDEX \"%s\" ON \"%s\".\"%s\"(%s)",
                                   unique ? "UNIQUE" : "",
                                   indexName,
                                   schema,
                                   table,
                                   Strings.join(Arrays.asList(indexCols), ","));
        createFromDDL(schema, ddl);
    }

    protected final TableIndex createIndex(String schema, String table, String indexName, String... indexCols) {
        createIndexInternal(schema, table, indexName, indexCols);
        return ddl().getTable(session(), new TableName(schema, table)).getIndex(indexName);
    }

    protected final TableIndex createUniqueIndex(String schema, String table, String indexName, String... indexCols) {
        createUniqueIndexInternal(schema, table, indexName, indexCols);
        return ddl().getTable(session(), new TableName(schema, table)).getIndex(indexName);
    }

    protected final TableIndex createSpatialTableIndex(String schema, String table, String indexName,
                                                       int firstSpatialArgument, int spatialColumns,
                                                       String... indexCols) {
        StringBuilder cols = new StringBuilder();
        for (int i = 0; i < indexCols.length; i++) {
            if (i > 0) cols.append(",");
            if (i == firstSpatialArgument) cols.append("Z_ORDER_LAT_LON(");
            cols.append(indexCols[i]);
            if (i == firstSpatialArgument + spatialColumns - 1) cols.append(")");
        }
        createIndexInternal(schema, table, indexName, cols.toString());
        return ddl().getTable(session(), new TableName(schema, table)).getIndex(indexName);
    }

    protected final GroupIndex createGroupIndex(TableName groupName,
                                                String indexName,
                                                Index.JoinType joinType,
                                                String... columnNames) {
        return createGroupIndexInternal(groupName, indexName, null, null, columnNames, joinType);
    }

    protected final GroupIndex createLeftGroupIndex(TableName groupName,
                                                    String indexName,
                                                    String... columnNames) {
        return createGroupIndexInternal(groupName, indexName, null, null, columnNames, JoinType.LEFT);
    }

    protected final GroupIndex createRightGroupIndex(TableName groupName,
                                                     String indexName,
                                                     String... columnNames) {
        return createGroupIndexInternal(groupName, indexName, null, null, columnNames, JoinType.RIGHT);
    }

    protected final GroupIndex createSpatialGroupIndex(TableName groupName,
                                                       String indexName,
                                                       int firstSpatialArg,
                                                       int numDimensions,
                                                       Index.JoinType joinType,
                                                       String... columnNames) {
        return createGroupIndexInternal(groupName, indexName, firstSpatialArg, numDimensions, columnNames, joinType);
    }

    /** Internal helper: {@code firstSpatialArg} and {@code numDimensions} may be {@code null} if not spatial. */
    private GroupIndex createGroupIndexInternal(TableName groupName,
                                                String indexName,
                                                Integer firstSpatialArg,
                                                Integer numDimensions,
                                                String[] columnNames,
                                                Index.JoinType joinType) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE INDEX \"");
        ddl.append(indexName);
        ddl.append("\" ON \"");
        ddl.append(groupName.getSchemaName());
        ddl.append("\".\"");
        ddl.append(groupName.getTableName());
        ddl.append("\"(");
        int firstSpatialIndex = (firstSpatialArg != null) ? firstSpatialArg : -1;
        int lastSpatialIndex = (firstSpatialArg != null) ? firstSpatialIndex + numDimensions - 1: -1;
        for(int i = 0; i < columnNames.length; ++i) {
            if(i != 0) {
                ddl.append(",");
            }
            if(i == firstSpatialIndex) {
                ddl.append("Z_ORDER_LAT_LON(");
            }
            ddl.append(columnNames[i]);
            if(i == lastSpatialIndex) {
                ddl.append(')');
            }
        }
        ddl.append(") USING ");
        ddl.append(joinType.name());
        ddl.append(" JOIN");
        createFromDDL(groupName.getSchemaName(), ddl.toString());
        GroupIndex index = ais().getGroup(groupName).getIndex(indexName);
        assert index != null;
        return index;
    }

    protected final FullTextIndex createFullTextIndex(String schema, String table, String indexName, String... indexCols) {
        createIndexInternal(schema, table, indexName, "FULL_TEXT(" + Strings.join(Arrays.asList(indexCols), ",") + ")");
        return ddl().getTable(session(), new TableName(schema, table)).getFullTextIndex(indexName);
    }

    protected BitSet getAllTableIds() {
        BitSet tableIds = new BitSet();
        for (Table table : ddl().getAIS(session()).getTables().values()) {
            if (table.getTableId() < 1000000000) { // Not IS tables
                tableIds.set(table.getTableId());
            }
        }
        return tableIds;
    }

    protected int createTablesAndIndexesFromDDL(String schema, String ddl) {
        BitSet before = getAllTableIds();

        // Insert DDL into the System.
        createFromDDL(schema, ddl);

        // Return lowest id newly added.
        BitSet after = getAllTableIds();
        after.andNot(before);
        return after.nextSetBit(0);
    }

    protected int loadSchemaFile(String schemaName, File file) throws Exception {
        String sql = Strings.dumpFileToString(file);
        return createTablesAndIndexesFromDDL(schemaName, sql);
    }

    protected void loadDataFile(String schemaName, File file) throws Exception {
        String tableName = file.getName().replace(".dat", "");
        Table table = getTable(schemaName, tableName);
        final List<Row> rows = new ArrayList<>();
        for (String line : Strings.dumpFile(file)) {
            String[] cols = line.split("\t");
            Object[] values = new Object[cols.length];
            for (int i = 0; i < cols.length; i++) {
                Object val;
                if ("NULL".equalsIgnoreCase(cols[i])) {
                    val = null;
                } else if (isBinary(table.getColumn(i))) {
                    val = Strings.fromBase64(cols[i]);
                } else {
                    val = cols[i];
                }
                values[i] = val;
            }
            rows.add(row(table, values));
        }
        txnService().run(session(), new Runnable() {
            @Override
            public void run() {
                writeRows(rows);
            }
        });
    }

    public int loadDatabase(String schemaName, File dir) throws Exception {
        int rootTableID = loadSchemaFile(schemaName, new File(dir, "schema.ddl"));
        File[] files = dir.listFiles(new RegexFilenameFilter(".*\\.dat"));
        Arrays.sort(files);
        for (File data : files) {
            loadDataFile(schemaName, data);
        }
        return rootTableID;
    }

    protected static boolean isBinary(Column column) {
        return (column.getType().typeClass() instanceof com.foundationdb.server.types.common.types.TBinary);
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

    protected List<Row> scanAll(int tableID) {
        Table table = getTable(tableID);
        Operator plan = scanTablePlan(table);
        return runPlan(session(), SchemaCache.globalSchema(table.getAIS()), plan);
    }

    protected List<Row> scanAll(int tableID, int indexID) {
        Table table = getTable(tableID);
        Index index = null;
        for(Index idx : table.getIndexesIncludingInternal()) {
            if(idx.getIndexId() == indexID) {
                index = idx;
                break;
            }
        }
        if(index == null) {
            throw new IllegalArgumentException("no indexid: " + indexID);
        }
        Operator plan = scanIndexPlan(index);
        return runPlan(session(), SchemaCache.globalSchema(table.getAIS()), plan);
    }

    private Operator scanTablePlan(Table table) {
        Schema schema = SchemaCache.globalSchema(table.getAIS());
        return API.filter_Default(API.groupScan_Default(table.getGroup()),
                                  Arrays.asList(schema.tableRowType(table)));
    }

    protected Operator scanIndexPlan(Index index) {
        Schema schema = SchemaCache.globalSchema(index.getAIS());
        return API.indexScan_Default(schema.indexRowType(index));
    }

    protected final List<Row> scanAllIndex(Index index) {
        AkibanInformationSchema ais = ais();
        Schema schema = SchemaCache.globalSchema(ais);
        Operator plan = API.indexScan_Default(schema.indexRowType(index));
        return runPlan(session(), schema, plan);
    }

    protected List<Row> writeRow(int tableID, Object... fields) {
        return writeRow(session(), tableID, fields);
    }

    protected List<Row> writeRow(Session session, int tableID, Object... fields) {
        Row row = row(session, tableID, fields);
        return writeRows(session, row);
    }

    protected List<Row> writeRow(Row row) {
        return writeRow(session(), row);
    }

    protected List<Row> writeRow(Session session, Row row) {
        return writeRows(session, row);
    }

    protected List<Row> writeRows(Row... rows) {
        return writeRows(session(), rows);
    }

    protected List<Row> writeRows(Session session, Row... rows) {
        return writeRows(session, Arrays.asList(rows));
    }

    protected List<Row> writeRows(Collection<Row> rows) {
        return writeRows(session(), rows);
    }

    protected List<Row> writeRows(Session session, Collection<Row> rows) {
        RowType lastRowType = null;
        List<Row> someRows = new ArrayList<>();
        List<Row> outputRows = new ArrayList<>();
        for(Row r : rows) {
            if(lastRowType != r.rowType()) {
                if(!someRows.isEmpty()) {
                    outputRows.addAll(writeRowsInternal(session, lastRowType, someRows));
                }
                someRows.clear();
                lastRowType = r.rowType();
            }
            someRows.add(r);
        }
        if(!someRows.isEmpty()) {
            outputRows.addAll(writeRowsInternal(session, lastRowType, someRows));
        }
        return outputRows;
    }

    private List<Row> writeRowsInternal(Session session, final RowType rowType, final Collection<Row> rows) {
        return runPlan(session, rowType.schema(), new PlanCreator()
        {
            @Override
            public Operator createPlan() {
                // Delicate: Create on demand so bindableRows can evaluate an expression for
                //           hidden sequences. This can go away if ITs get sane txn management.
                return API.insert_Returning(API.valuesScan_Default(bindableRows(rows), rowType));
            }
        });
    }

    protected List<Row> updateRow(final Row oldRow, final Row newRow) {
        return updateRow(session(), oldRow, newRow);
    }

    protected List<Row> updateRow(Session session, final Row oldRow, final Row newRow) {
        if(oldRow.rowType() != newRow.rowType()) {
            throw new IllegalArgumentException("mixed RowTypes");
        }
        Operator plan = API.update_Returning(API.valuesScan_Default(bindableRows(oldRow), oldRow.rowType()),
                                             new UpdateFunction()
                                             {
                                                 @Override
                                                 public Row evaluate(Row original,
                                                                     QueryContext context,
                                                                     QueryBindings bindings) {
                                                     return newRow;
                                                 }

                                                 @Override
                                                 public boolean rowIsSelected(Row row) {
                                                     return row == oldRow;
                                                 }
                                             });
        return runPlan(session, oldRow.rowType().schema(), plan);
    }

    protected List<Row> deleteRow(int tableID, Object... fields) {
        Row row = row(tableID, fields);
        return deleteRow(row);
    }

    protected List<Row> deleteRow(Row row) {
        return deleteRow(row, false);
    }

    protected List<Row> deleteRow(Row row, boolean cascade) {
        Operator plan = API.delete_Returning(API.valuesScan_Default(bindableRows(row),
                                                                    row.rowType()),
                                             cascade);
        return runPlan(session(), row.rowType().schema(), plan);
    }

    protected static List<? extends BindableRow> bindableRows(Row... rows) {
        return bindableRows(Arrays.asList(rows));
    }

    protected static List<? extends BindableRow> bindableRows(Collection<Row> rows) {
        List<BindableRow> output = new ArrayList<>();
        for(Row r : rows) {
            output.add(BindableRow.of(r));
        }
        return output;
    }

    private interface PlanCreator {
        Operator createPlan();
    }

    protected List<Row> runPlan(final Session session, final Schema schema, final Operator plan) {
        return runPlan(session, schema, new PlanCreator()
        {
            public Operator createPlan() {
                return plan;
            }
        });
    }

    protected List<Row> runPlan(final Session session, final Schema schema, final PlanCreator plan) {
        if(txnService().isTransactionActive(session)) {
            return runPlanInternal(session, schema, plan.createPlan());
        } else {
            // TODO: Get all ITs managing their own transactions
            return txnService().run(session, new Callable<List<Row>>()
            {
                @Override
                public List<Row> call() throws Exception {
                    return runPlanInternal(session, schema, plan.createPlan());
                }
            });
        }
    }

    protected List<Row> runPlanInternal(Session session, Schema schema, Operator plan) {
        StoreAdapter adapter = newStoreAdapter(session, schema);
        QueryContext context = queryContext(adapter);
        Cursor cursor = API.cursor(plan, context, context.createBindings());
        cursor.openTopLevel();
        try {
            List<Row> output = new ArrayList<>();
            Row row;
            while((row = cursor.next()) != null) {
                output.add(row);
            }
            return output;
        } finally {
            cursor.closeTopLevel();
        }
    }

    protected final int indexId(String schema, String table, String index) {
        AkibanInformationSchema ais = ddl().getAIS(session());
        Table aisTable = ais.getTable(schema, table);
        Index aisIndex = aisTable.getIndex(index);
        if (aisIndex == null) {
            throw new RuntimeException("no such index: " + index);
        }
        return aisIndex.getIndexId();
    }

    protected final CursorId openFullScan(String schema, String table, String index) throws InvalidOperationException {
        AkibanInformationSchema ais = ddl().getAIS(session());
        Table aisTable = ais.getTable(schema, table);
        Index aisIndex = aisTable.getIndex(index);
        if (aisIndex == null) {
            throw new RuntimeException("no such index: " + index);
        }
        return openFullScan(aisTable.getTableId(), aisIndex.getIndexId());
    }

    protected final CursorId openFullScan(int tableId, int indexId) throws InvalidOperationException {
        Table table = ddl().getTable(session(), tableId);
        Set<Integer> allCols = new HashSet<>();
        for (int i=0, MAX=table.getColumns().size(); i < MAX; ++i) {
            allCols.add(i);
        }
        ScanRequest request = new ScanAllRequest(tableId, allCols, indexId,
                EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END)
        );
        return dml().openCursor(session(), aisGeneration(), request);
    }

    protected static Set<Integer> set(Integer... items) {
        return new HashSet<>(Arrays.asList(items));
    }

    @SafeVarargs
    protected static <T> T[] array(Class<T> ofClass, T... items) {
        if (ofClass == null) {
            throw new IllegalArgumentException(
                    "T[] of null class; you probably meant the array(Object...) overload "
                            +"with a null for the first element. Use array(Object.class, null, ...) instead"
            );
        }
        for(T t : items) {
            if(t != null && !ofClass.isInstance(t)) {
                throw new IllegalArgumentException("Mismatched class: " + t.getClass());
            }
        }
        return items;
    }

    protected static Object[] array(Object... items) {
        return array(Object.class, items);
    }

    protected static <T> T get(NewRow row, int field, Class<T> castAs) {
        Object obj = row.get(field);
        return castAs.cast(obj);
    }

    public static Object getObject(ValueSource value) {
        if (value.isNull())
            return null;
        if (value.hasCacheValue())
            return value.getObject();
        switch (ValueSources.underlyingType(value)) {
        case BOOL:
            return value.getBoolean();
        case INT_8:
            return value.getInt8();
        case INT_16:
            return value.getInt16();
        case UINT_16:
            return value.getUInt16();
        case INT_32:
            return value.getInt32();
        case INT_64:
            return value.getInt64();
        case FLOAT:
            return value.getFloat();
        case DOUBLE:
            return value.getDouble();
        case BYTES:
            return value.getBytes();
        case STRING:
            return value.getString();
        default:
            throw new AssertionError(value);
        }
    }

    public static boolean isNull(ValueRecord row, int pos) {
        return row.value(pos).isNull();
    }

    public static Long getLong(ValueRecord row, int field) {
        final Long result;
        ValueSource value = row.value(field);
        if (value.isNull()) {
            result = null;
        }
        else {
            switch (ValueSources.underlyingType(value)) {
            case INT_8:
                result = (long) value.getInt8();
                break;
            case INT_16:
                result = (long) value.getInt16();
                break;
            case UINT_16:
                result = (long) value.getUInt16();
                break;
            case INT_32:
                result = (long) value.getInt32();
                break;
            case INT_64:
                result = value.getInt64();
                break;
            default:
                throw new AssertionError(value);
            }
        }
        return result;
    }

    protected void expectRows(int tableID, Row... expectedRows) {
        expectRows(tableID, Arrays.asList(expectedRows));
    }

    protected void expectRows(int tableID, Collection<Row> expectedRows) {
        expectRows(tableID, false, expectedRows);
    }

    protected void expectRowsSkipInternal(int tableID, Row... expectedRows) {
        expectRows(tableID, true, Arrays.asList(expectedRows));
    }

    protected void expectRows(int tableID, boolean skipInternal, Collection<Row> expectedRows) {
        Operator plan = scanTablePlan(getTable(tableID));
        expectRows(plan, skipInternal, expectedRows);
        expectRowCount(tableID, expectedRows.size());
    }

    protected void expectRows(Index index, Row... expectedRows) {
        expectRows(index, Arrays.asList(expectedRows));
    }

    protected void expectRows(Index index, Collection<Row> expectedRows) {
        expectRows(scanIndexPlan(index), false, expectedRows);
    }

    protected void expectRowsSkipInternal(Index index, Row... expectedRows) {
        expectRows(scanIndexPlan(index), true, Arrays.asList(expectedRows));
    }

    protected void expectRows(Operator plan, boolean skipInternal, Collection<Row> expectedRows) {
        Schema schema = SchemaCache.globalSchema(ais());
        expectRows(schema, plan, skipInternal, expectedRows);
    }

    protected void expectRows(Schema schema, Operator plan, boolean skipInternal, Collection<Row> expectedRows) {
        List<Row> actual = runPlan(session(), schema, plan);
        compareRows(expectedRows, actual, skipInternal);
    }

    protected static Set<CursorId> cursorSet(CursorId... cursorIds) {
        Set<CursorId> set = new HashSet<>();
        for (CursorId id : cursorIds) {
            if(!set.add(id)) {
                fail(String.format("while adding %s to %s", id, set));
            }
        }
        return set;
    }

    public Row row(RowDef rowDef, Object... fields) {
        return row(rowDef.table(), fields);
    }

    public Row row(Table table, Object... fields) {
        return row(table.getTableId(), fields);
    }

    public Row row(int tableID, Object... fields) {
        return row(session(), tableID, fields);
    }

    public Row row(Session session, int tableID, Object... fields) {
        AkibanInformationSchema ais = ais(session);
        Schema schema = SchemaCache.globalSchema(ais);
        RowType rowType = schema.tableRowType(tableID);
        return row(rowType, fields);
    }

    public Row row(Index index, Object... fields) {
        AkibanInformationSchema ais = ais(session());
        Schema schema = SchemaCache.globalSchema(ais);
        RowType rowType = schema.indexRowType(index);
        return row(rowType, fields);
    }

    public Row row(RowType rowType, Object... fields) {
        for(Object o : fields) {
            if(o == UNDEF) {
                throw new UnsupportedOperationException("UNDEF");
            }
        }

        if(fields.length < rowType.nFields()) {
            QueryContext context = new SimpleQueryContext(newStoreAdapter(rowType.schema()));
            List<TPreparedExpression> expressions = new ArrayList<>();
            for(int i = 0; i < fields.length; ++i) {
                TInstance type = rowType.typeAt(i);
                TPreptimeValue val = ValueSources.fromObject(fields[i], type);
                expressions.add(new TPreparedLiteral(type, val.value()));
            }
            for(int i = fields.length; i < rowType.nFields(); ++i) {
                Column col = getColumn(rowType, i);
                if(col == null) {
                    throw new IllegalArgumentException("Column " + i + "not specified and no default: " + rowType);
                }
                expressions.add(PlanGenerator.generateDefaultExpression(col, null, typesRegistryService(), ddl().getTypesTranslator(), context));
            }
            return new ExpressionRow(rowType, context, context.createBindings(), expressions);
        } else {
            return new TestRow(rowType, fields);
        }
    }

    protected final void dropAllTables() throws InvalidOperationException {
        dropAllTables(session());
    }

    protected final void dropAllTables(Session session) throws InvalidOperationException {
        ddl().dropNonSystemSchemas(session);

        // Now sanity check
        Set<TableName> uTables = new HashSet<>(ddl().getAIS(session).getTables().keySet());
        for (Iterator<TableName> iter = uTables.iterator(); iter.hasNext();) {
            String schemaName = iter.next().getSchemaName();
            if (TableName.INFORMATION_SCHEMA.equals(schemaName) ||
                TableName.SECURITY_SCHEMA.equals(schemaName)) {
                iter.remove();
            }
        }
        Assert.assertEquals("user table count", Collections.<TableName>emptySet(), uTables);

        Set<TableName> views = new HashSet<>(ddl().getAIS(session).getViews().keySet());
        Assert.assertEquals("user table count", Collections.<TableName>emptySet(), views);
    }

    protected static <T> void assertEqualLists(String message, List<? extends T> expected, List<? extends T> actual) {
        AssertUtils.assertCollectionEquals(message, expected, actual);
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

    protected final Table getTable(String schema, String name) {
        return getTable(tableName(schema, name));
    }

    protected final Table getTable(TableName name) {
        return ddl().getTable(session(), name);
    }

    protected final RowDef getRowDef(int rowDefId) {
        return getTable(rowDefId).rowDef();
    }

    protected final RowDef getRowDef(String schema, String table) {
        return getTable(schema, table).rowDef();
    }

    protected final RowDef getRowDef(TableName tableName) {
        return getTable(tableName).rowDef();
    }

    protected final Table getTable(int tableId) {
        final Table table;
        try {
            table = ddl().getTable(session(), tableId);
        } catch (NoSuchTableException e) {
            throw new TestException(e);
        }
        return table;
    }

    protected final Map<TableName,Table> getTables() {
        return stripAISTables(ddl().getAIS(session()).getTables());
    }

    private static <T extends Table> Map<TableName,T> stripAISTables(Map<TableName,T> map) {
        final Map<TableName,T> ret = new HashMap<>(map);
        for(Iterator<TableName> iter=ret.keySet().iterator(); iter.hasNext(); ) {
            if(TableName.INFORMATION_SCHEMA.equals(iter.next().getSchemaName())) {
                iter.remove();
            }
        }
        return ret;
    }

    protected void expectIndexes(int tableId, String... expectedIndexNames) {
        Table table = getTable(tableId);
        Set<String> expectedIndexesSet = new TreeSet<>(Arrays.asList(expectedIndexNames));
        Set<String> actualIndexes = new TreeSet<>();
        for (Index index : table.getIndexes()) {
            String indexName = index.getIndexName().getName();
            boolean added = actualIndexes.add(indexName);
            assertTrue("duplicate index name: " + indexName, added);
        }
        assertEquals("indexes in " + table.getName(), expectedIndexesSet, actualIndexes);
    }

    protected <T> T transactionally(Callable<T> callable) throws Exception {
        txnService().beginTransaction(session());
        try {
            T value = callable.call();
            txnService().commitTransaction(session());
            return value;
        }
        finally {
            txnService().rollbackTransactionIfOpen(session());
        }
    }

    protected <T> T transactionallyUnchecked(Callable<T> callable) {
        try {
            return transactionally(callable);
        } catch (Exception e) {
            throw Exceptions.throwAlways(e);
        }
    }

    protected void transactionallyUnchecked(final Runnable runnable) {
        transactionallyUnchecked(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                runnable.run();
                return null;
            }
        });
    }

    protected boolean pipelineMap() {
        return false;
    }

    protected DDLFunctions ddlForAlter() {
        return ddl();
    }

    protected void runAlter(TableChangeValidator.ChangeLevel expectedChangeLevel, String defaultSchema, String sql) {
        runAlter(session(), ddlForAlter(), dml(), null, expectedChangeLevel, defaultSchema, sql);
        updateAISGeneration();
    }

    protected static void runAlter(Session session, DDLFunctions ddl, DMLFunctions dml, QueryContext context,
                                   TableChangeValidator.ChangeLevel expectedChangeLevel, String defaultSchema, String sql) {
        SQLParser parser = new SQLParser();
        StatementNode node;
        try {
            node = parser.parseStatement(sql);
        } catch(StandardException e) {
            throw new RuntimeException(e);
        }
        assertTrue("is alter node", node instanceof AlterTableNode);
        TableChangeValidator.ChangeLevel level = AlterTableDDL.alterTable(ddl, dml, session, defaultSchema, (AlterTableNode) node, context);
        assertEquals("ChangeLevel", expectedChangeLevel, level);
    }

    protected static void compareRows(Collection<? extends Row> expected, Collection<? extends Row> actual, boolean skipInternalColumns) {
        Iterator<? extends Row> eIt = expected.iterator();
        Iterator<? extends Row> aIt = actual.iterator();
        int i = 0;
        while(eIt.hasNext() && aIt.hasNext()) {
            compareTwoRows(eIt.next(), aIt.next(), i++, skipInternalColumns);
        }
        assertEquals("row count", expected.size(), actual.size());
    }

    protected static void compareTwoRows(Row expected, Row actual, int rowNumber) {
        compareTwoRows(expected, actual, rowNumber, false);
    }


    private static void compareTwoRows(Row expected, Row actual, int rowNumber, boolean skipInternalColumns) {
        if(!equal(expected, actual,skipInternalColumns)) {
            assertEquals("row " + rowNumber, String.valueOf(expected), String.valueOf(actual));
        }
        if(expected instanceof TestRow) {
            TestRow expectedTestRow = (TestRow) expected;
            if (expectedTestRow.persistityString() != null) {
                Object hKey = (actual != null) ? actual.hKey() : null;
                String actualHKeyString = String.valueOf(hKey);
                assertEquals(rowNumber + ": hkey", expectedTestRow.persistityString(), actualHKeyString);
            }
        }
    }

    private static boolean equal(Row expected, Row actual, boolean skipInternalColumns)
    {
        int nFields;
        if(skipInternalColumns){
            boolean equal = getTotalNonInternalColumns(expected) == getTotalNonInternalColumns(actual);
            if (!equal)
                return false;
            nFields = getTotalNonInternalColumns(actual);
        }//Used to ignore added pk column when create table as select is used
        else {
            boolean equal = expected.rowType().nFields() == actual.rowType().nFields();
            if (!equal)
                return false;
            nFields = actual.rowType().nFields();
        }
        Space space = space(expected.rowType());
        if (space != null) {
            nFields = nFields - space.dimensions() + 1;
        }
        for (int actualPosition = 0, expectedPosition = 0;
             actualPosition < nFields && expectedPosition < nFields;
             actualPosition++, expectedPosition++) {
            if(skipInternalColumns) {
                while (isInternalColumn(actual, actualPosition)) {
                    if(++actualPosition == nFields)
                        return true;
                }
                while (isInternalColumn(expected, actualPosition)) {
                    if(++expectedPosition == nFields)
                        return true;
                }
            }
            ValueSource expectedField = expected.value(expectedPosition);
            ValueSource actualField = actual.value(actualPosition);
            TInstance expectedType = expected.rowType().typeAt(expectedPosition);
            TInstance actualType = actual.rowType().typeAt(actualPosition);
            assertEquals("expected type", expectedType.typeClass(), actualType.typeClass());
            int c = TClass.compare(expectedType, expectedField, actualType, actualField);
            if (c != 0)
                return false;
        }
        return true;
    }

    private static int getTotalNonInternalColumns(Row row){
        int count = 0;
        for(int i = 0; i < row.rowType().nFields();i++){
            if(!isInternalColumn(row, i)) {
                count++;
            }
        }
        return count;
    }

    private static Column getColumn(RowType rowType, int position) {
        Column col = null;
        if(rowType.hasTable()){
            col = rowType.table().getColumnsIncludingInternal().get(position);
        } else if(rowType instanceof IndexRowType) {
            col = ((IndexRowType)rowType).index().getAllColumns().get(position).getColumn();
        }
        return col;
    }

    private static boolean isInternalColumn(Row row, int position) {
        return isInternalColumn(row.rowType(), position);
    }

    private static boolean isInternalColumn(RowType rowType, int position) {
        Column col = getColumn(rowType, position);
        return (col != null) && col.isInternalColumn();
    }

    private static Space space(RowType rowType)
    {
        Space space = null;
        if (rowType instanceof IndexRowType) {
            Index index = ((IndexRowType)rowType).index();
            if (index.isSpatial()) {
                space = index.space();
            }
        }
        return space;
    }
}
