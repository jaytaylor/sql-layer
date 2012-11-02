/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.test.mt.mtatomics;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.ais.util.TableChange;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.api.dml.scan.ScanFlag;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.OldAISException;
import com.akiban.server.error.TableChangedByDDLException;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.service.dxl.DXLReadWriteLockHook;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.test.mt.mtutil.TimePoints;
import com.akiban.server.test.mt.mtutil.TimePointsComparison;
import com.akiban.server.test.mt.mtutil.TimedCallable;
import com.akiban.server.test.mt.mtutil.TimedExceptionCatcher;
import com.akiban.server.test.mt.mtutil.Timing;
import com.akiban.server.test.mt.mtutil.TimedResult;
import com.akiban.server.service.dxl.ConcurrencyAtomicsDXLService;
import com.akiban.server.service.session.Session;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static com.akiban.server.test.mt.mtatomics.DelayableIUDCallable.IUDType;

public final class ConcurrentDDLAtomicsMT extends ConcurrentAtomicsBase {
    /** Used by {@link #largeEnoughTable(long)} to save length computation between tests */
    private static long lastLargeEnoughMS = 0;
    private static int lastLargeEnoughCount = 0;

    private boolean isDDLLockOn() {
        return DXLReadWriteLockHook.only().isDDLLockEnabled();
    }

    @Test
    public void dropTableWhileScanningPK() throws Exception {
        final int tableId = tableWithTwoRows();
        dropTableWhileScanning(
                tableId,
                "PRIMARY",
                createNewRow(tableId, 1L, "the snowman"),
                createNewRow(tableId, 2L, "mr melty")
        );
    }

    @Test
    public void dropTableWhileScanningOnIndex() throws Exception {
        final int tableId = tableWithTwoRows();
        dropTableWhileScanning(
                tableId,
                "name",
                createNewRow(tableId, 2L, "mr melty"),
                createNewRow(tableId, 1L, "the snowman")
        );
    }

    private void dropTableWhileScanning(int tableId, String indexName, NewRow... expectedScanRows) throws Exception {
        final int SCAN_WAIT = 5000;

        int indexId = ddl().getUserTable(session(), TABLE_NAME).getIndex(indexName).getIndexId();

        TimedCallable<List<NewRow>> scanCallable
                = new DelayScanCallableBuilder(aisGeneration(), tableId, indexId)
                .topOfLoopDelayer(0, SCAN_WAIT, "SCAN: PAUSE").get();

        TimedCallable<Void> dropIndexCallable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(TimePoints timePoints, Session session) throws Exception {
                TableName table = TABLE_NAME;
                Timing.sleep(2000);
                timePoints.mark("TABLE: DROP>");
                ddl().dropTable(session, table);
                timePoints.mark("TABLE: <DROP");
                return null;
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<TimedResult<List<NewRow>>> scanFuture = executor.submit(scanCallable);
        Future<TimedResult<Void>> dropIndexFuture = executor.submit(dropIndexCallable);

        TimedResult<List<NewRow>> scanResult = scanFuture.get();
        TimedResult<Void> dropIndexResult = dropIndexFuture.get();

        final String[] expected;
        if(isDDLLockOn()) {
            // Pause is deep within DMLFunctions, DDL waits for global lock release
            expected = new String[] {
                    "SCAN: START",
                    "(SCAN: PAUSE)>",
                    "TABLE: DROP>",
                    "<(SCAN: PAUSE)",
                    "SCAN: FINISH",
                    "TABLE: <DROP"
            };
        } else {
            // Scan takes no table locks, DDL can proceed as-is
            expected = new String[] {
                    "SCAN: START",
                    "(SCAN: PAUSE)>",
                    "TABLE: DROP>",
                    "TABLE: <DROP",
                    "<(SCAN: PAUSE)",
                    "SCAN: FINISH"
            };
        }

        new TimePointsComparison(scanResult, dropIndexResult).verify(expected);
        List<NewRow> rowsScanned = scanResult.getItem();
        assertEquals("rows scanned size", expectedScanRows.length, rowsScanned.size());
        assertEquals("rows", Arrays.asList(expectedScanRows), rowsScanned);
    }

    @Test
    public void rowConvertedAfterTableDrop() throws Exception {
        final String index = "PRIMARY";
        final int tableId = tableWithTwoRows();
        final int indexId = ddl().getUserTable(session(), TABLE_NAME).getIndex(index).getIndexId();

        DelayScanCallableBuilder callableBuilder = new DelayScanCallableBuilder(aisGeneration(), tableId, indexId)
                .markFinish(false)
                .beforeConversionDelayer(new DelayerFactory() {
                    @Override
                    public Delayer delayer(TimePoints timePoints) {
                        return new Delayer(timePoints, 0, 5000)
                                .markBefore(1, "SCAN: PAUSE")
                                .markAfter(1, "SCAN: CONVERTED");
                    }
                });
        TimedCallable<List<NewRow>> scanCallable = callableBuilder.get();

        TimedCallable<Void> dropIndexCallable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(TimePoints timePoints, Session session) throws Exception {
                TableName table = TABLE_NAME;
                Timing.sleep(2000);
                timePoints.mark("TABLE: DROP>");
                ddl().dropTable(session, table);
                timePoints.mark("TABLE: <DROP");
                return null;
            }
        };

        // Has to happen before the table is dropped!
        List<NewRow> rowsExpected = Arrays.asList(
                createNewRow(tableId, 1L, "the snowman"),
                createNewRow(tableId, 2L, "mr melty")
        );

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<TimedResult<List<NewRow>>> scanFuture = executor.submit(scanCallable);
        Future<TimedResult<Void>> dropIndexFuture = executor.submit(dropIndexCallable);

        TimedResult<List<NewRow>> scanResult = scanFuture.get();
        TimedResult<Void> dropIndexResult = dropIndexFuture.get();

        final String[] expected;
        if(isDDLLockOn()) {
            // Pause is deep within DMLFunctions, DDL waits for global lock release
            expected = new String[] {
                    "SCAN: START",
                    "SCAN: PAUSE",
                    "TABLE: DROP>",
                    "SCAN: CONVERTED",
                    "TABLE: <DROP"
            };
        } else {
            // Scan takes no table locks, DDL can proceed as-is
            expected = new String[] {
                    "SCAN: START",
                    "SCAN: PAUSE",
                    "TABLE: DROP>",
                    "TABLE: <DROP",
                    "SCAN: CONVERTED"
            };
        }

        new TimePointsComparison(scanResult, dropIndexResult).verify(expected);
        List<NewRow> rowsScanned = scanResult.getItem();
        assertEquals("rows scanned (in order)", rowsExpected, rowsScanned);
    }

    @Test
    public void scanPKWhileDropping() throws Exception {
        scanWhileDropping("PRIMARY");
    }

    @Test
    public void scanIndexWhileDropping() throws Exception {
        scanWhileDropping("name");
    }

    @Test
    public void dropShiftsIndexIdWhileScanning() throws Exception {
        final int tableId = createTable(SCHEMA, TABLE, "id int not null primary key", "name varchar(32)", "age varchar(2)",
                                        "UNIQUE(name)", "UNIQUE(age)");
        writeRows(
                createNewRow(tableId, 2, "alpha", 3),
                createNewRow(tableId, 1, "bravo", 2),
                createNewRow(tableId, 3, "charlie", 1)
                // the above are listed in order of index #1 (the name index)
                // after that index is dropped, index #1 is age, and that will come in this order:
                // (3, charlie 1)
                // (1, bravo, 2)
                // (2, alpha, 3)
                // We'll get to the 2nd index (bravo) when we drop the index, and we want to make sure we don't
                // continue scanning with alpha (which would thus badly order name)
        );
        final TableName tableName = TABLE_NAME;
        Index nameIndex = ddl().getUserTable(session(), tableName).getIndex("name");
        Index ageIndex = ddl().getUserTable(session(), tableName).getIndex("age");
        final int nameIndexId = nameIndex.getIndexId();
        assertTrue("age index's ID relative to name's", ageIndex.getIndexId() != nameIndexId);

        TimedCallable<List<NewRow>> scanCallable
                = new DelayScanCallableBuilder(aisGeneration(), tableId, nameIndexId)
                .topOfLoopDelayer(2, 5000, "SCAN: PAUSE").get();

        TimedCallable<Void> dropIndexCallable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(TimePoints timePoints, Session session) throws Exception {
                Timing.sleep(2500);
                timePoints.mark("INDEX: DROP>");
                ddl().dropTableIndexes(session, tableName, Collections.singleton("name"));
                timePoints.mark("INDEX: <DROP");
                return null;
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<TimedResult<List<NewRow>>> scanFuture = executor.submit(scanCallable);
        Future<TimedResult<Void>> dropIndexFuture = executor.submit(dropIndexCallable);

        TimedResult<List<NewRow>> scanResult = scanFuture.get();
        TimedResult<Void> dropIndexResult = dropIndexFuture.get();

        final String[] expected;
        if(isDDLLockOn()) {
            // Pause is deep within DMLFunctions, DDL waits for global lock release
            expected = new String[] {
                    "SCAN: START",
                    "(SCAN: PAUSE)>",
                    "INDEX: DROP>",
                    "<(SCAN: PAUSE)",
                    "SCAN: FINISH",
                    "INDEX: <DROP"
            };
        } else {
            // Scan takes no table locks, DDL can proceed as-is
            expected = new String[] {
                    "SCAN: START",
                    "(SCAN: PAUSE)>",
                    "INDEX: DROP>",
                    "INDEX: <DROP",
                    "<(SCAN: PAUSE)",
                    "SCAN: FINISH"
            };
        }

        new TimePointsComparison(scanResult, dropIndexResult).verify(expected);
        newRowsOrdered(scanResult.getItem(), 1);
    }

    /**
     * Smoke test of concurrent DDL. One thread will drop a table; the other one will try to create
     * another table in a different schema while that drop is still going on.
     */
    @Test
    public void createTableWhileDroppingAnother() throws Exception {
        largeEnoughTable(5000);
        final String uniqueTableName = TABLE + "thesnowman";

        NewAISBuilder builder = AISBBasedBuilder.create();
        builder.userTable(SCHEMA2, uniqueTableName).colLong("id", false).pk("id");
        final UserTable tableToCreate = builder.ais().getUserTable(SCHEMA2, uniqueTableName);

        TimedCallable<Void> dropTable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(TimePoints timePoints, Session session) throws Exception {
                timePoints.mark("DROP>");
                ddl().dropTable(session, TABLE_NAME);
                timePoints.mark("<DROP");
                return null;
            }
        };

        TimedCallable<Void> createTable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(TimePoints timePoints, Session session) throws Exception {
                Timing.sleep(2000);
                timePoints.mark("CREATE>");
                try {
                    ddl().createTable(session, tableToCreate);
                    timePoints.mark("<CREATE");
                } catch (IllegalStateException e) {
                    timePoints.mark("CREATE: IllegalStateException");
                }
                return null;
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<TimedResult<Void>> dropFuture = executor.submit(dropTable);
        Future<TimedResult<Void>> createFuture = executor.submit(createTable);

        TimedResult<Void> dropResult = dropFuture.get();
        TimedResult<Void> createResult = createFuture.get();

        final String[] expected;
        if(isDDLLockOn()) {
            // DDL lock asserts that is only one DDL at a time
            expected = new String[] {
                    "DROP>",
                    "CREATE>",
                    "CREATE: IllegalStateException",
                    "<DROP"
            };
        } else {
            // Concurrent DDL (in different schema) is allowed
            expected = new String[] {
                    "DROP>",
                    "CREATE>",
                    "<CREATE",
                    "<DROP"
            };
        }

        new TimePointsComparison(dropResult, createResult).verify(expected);

        Set<TableName> userTableNames = new HashSet<TableName>();
        for (UserTable userTable : ddl().getAIS(session()).getUserTables().values()) {
            if (!TableName.INFORMATION_SCHEMA.equals(userTable.getName().getSchemaName())) {
                userTableNames.add(userTable.getName());
            }
        }
        assertEquals(
                "user tables at end",
                Collections.singleton(new TableName(SCHEMA, TABLE+"parent")),
                userTableNames
        );
    }

    @Test
    public void dropIndexWhileScanning() throws Exception {
        final int tableId = tableWithTwoRows();
        final int SCAN_WAIT = 5000;

        int indexId = ddl().getUserTable(session(), TABLE_NAME).getIndex("name").getIndexId();

        TimedCallable<List<NewRow>> scanCallable
                = new DelayScanCallableBuilder(aisGeneration(), tableId, indexId)
                .topOfLoopDelayer(0, SCAN_WAIT, "SCAN: PAUSE").get();

        TimedCallable<Void> dropIndexCallable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(TimePoints timePoints, Session session) throws Exception {
                TableName table = TABLE_NAME;
                Timing.sleep(2000);
                timePoints.mark("INDEX: DROP>");
                ddl().dropTableIndexes(ServiceManagerImpl.newSession(), table, Collections.singleton("name"));
                timePoints.mark("INDEX: <DROP");
                return null;
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<TimedResult<List<NewRow>>> scanFuture = executor.submit(scanCallable);
        Future<TimedResult<Void>> dropIndexFuture = executor.submit(dropIndexCallable);

        TimedResult<List<NewRow>> scanResult = scanFuture.get();
        TimedResult<Void> dropIndexResult = dropIndexFuture.get();

        final String[] expected;
        if(isDDLLockOn()) {
            // Pause is deep within DMLFunctions, DDL waits for global lock release
            expected = new String[] {
                    "SCAN: START",
                    "(SCAN: PAUSE)>",
                    "INDEX: DROP>",
                    "<(SCAN: PAUSE)",
                    "SCAN: FINISH",
                    "INDEX: <DROP"
            };
        } else {
            // Scan takes no table locks, DDL can proceed as-is
            expected = new String[] {
                    "SCAN: START",
                    "(SCAN: PAUSE)>",
                    "INDEX: DROP>",
                    "INDEX: <DROP",
                    "<(SCAN: PAUSE)",
                    "SCAN: FINISH"
            };
        }

        new TimePointsComparison(scanResult, dropIndexResult).verify(expected);
        List<NewRow> rowsScanned = scanResult.getItem();
        List<NewRow> rowsExpected = Arrays.asList(
                createNewRow(tableId, 2L, "mr melty"),
                createNewRow(tableId, 1L, "the snowman")
        );
        assertEquals("rows scanned (in order)", rowsExpected, rowsScanned);
    }

    @Test
    public void scanWhileDroppingIndex() throws Throwable {
        final long SCAN_PAUSE_LENGTH = 2500;
        final long DROP_START_LENGTH = 1000;
        final long DROP_PAUSE_LENGTH = 2500;


        final int NUMBER_OF_ROWS = 100;
        final int initialTableId = createTable(SCHEMA, TABLE, "id int not null primary key", "age int");
        createIndex(SCHEMA, TABLE, "age", "age");
        final TableName tableName = TABLE_NAME;
        for(int i=0; i < NUMBER_OF_ROWS; ++i) {
            writeRows(createNewRow(initialTableId, i, i + 1));
        }

        final Index index = ddl().getUserTable(session(), tableName).getIndex("age");
        final Collection<String> indexNameCollection = Collections.singleton(index.getIndexName().getName());
        final int tableId = ddl().getTableId(session(), tableName);

        TimedCallable<Throwable> dropIndexCallable = new TimedExceptionCatcher() {
            @Override
            protected void doOrThrow(final TimePoints timePoints, Session session) throws Exception {
                Timing.sleep(DROP_START_LENGTH);
                timePoints.mark("DROP: PREPARING");
                ConcurrencyAtomicsDXLService.hookNextDropIndex(session,
                                                               new Runnable() {
                                                                   @Override
                                                                   public void run() {
                                                                       timePoints.mark("INDEX: DROP>");
                                                                       Timing.sleep(DROP_PAUSE_LENGTH);
                                                                   }
                                                               },
                                                               new Runnable() {
                                                                   @Override
                                                                   public void run() {
                                                                       timePoints.mark("INDEX: <DROP");
                                                                       Timing.sleep(DROP_PAUSE_LENGTH);
                                                                   }
                                                               }
                );

                ddl().dropTableIndexes(session, tableName, indexNameCollection);
                assertFalse("drop hook not removed!", ConcurrencyAtomicsDXLService.isDropIndexHookInstalled(session));
            }
        };
        final int localAISGeneration = aisGeneration();
        TimedCallable<Throwable> scanCallable = new TimedExceptionCatcher() {
            @Override
            protected void doOrThrow(TimePoints timePoints, Session session) throws Exception {
                timePoints.mark("SCAN: PREPARING");

                ScanAllRequest request = new ScanAllRequest(
                        tableId,
                        new HashSet<Integer>(Arrays.asList(0, 1)),
                        index.getIndexId(),
                        EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END),
                        ScanLimit.NONE
                );

                timePoints.mark("(SCAN: PAUSE)>");
                Timing.sleep(SCAN_PAUSE_LENGTH);
                timePoints.mark("<(SCAN: PAUSE)");
                try {
                    CursorId cursorId = dml().openCursor(session, localAISGeneration, request);
                    timePoints.mark("SCAN: cursorID opened");
                    dml().closeCursor(session, cursorId);
                } catch (OldAISException e) {
                    timePoints.mark("SCAN: OldAISException");
                }
            }

            @Override
            protected void handleCaught(TimePoints timePoints, Session session, Throwable t) {
                timePoints.mark("SCAN: Unexpected exception " + t.getClass().getSimpleName());
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<TimedResult<Throwable>> scanFuture = executor.submit(scanCallable);
        Future<TimedResult<Throwable>> dropIndexFuture = executor.submit(dropIndexCallable);

        TimedResult<Throwable> scanResult = scanFuture.get();
        TimedResult<Throwable> dropIndexResult = dropIndexFuture.get();

        final String[] expected;
        if(isDDLLockOn()) {
            // OldAIS is directly related to global lock (rather, transactional AIS) but is a good proxy
            expected = new String[] {
                    "SCAN: PREPARING",
                    "(SCAN: PAUSE)>",
                    "DROP: PREPARING",
                    "INDEX: DROP>",
                    "<(SCAN: PAUSE)",
                    "INDEX: <DROP",
                    "SCAN: OldAISException"
            };
        } else {
            // Pause for index is longer than scan and scan maintains a consistent view
            expected = new String[] {
                    "SCAN: PREPARING",
                    "(SCAN: PAUSE)>",
                    "DROP: PREPARING",
                    "INDEX: DROP>",
                    "<(SCAN: PAUSE)",
                    "SCAN: cursorID opened",
                    "INDEX: <DROP",
            };
        }

        new TimePointsComparison(scanResult, dropIndexResult).verify(expected);
        TimedExceptionCatcher.throwIfThrown(scanResult);
        TimedExceptionCatcher.throwIfThrown(dropIndexResult);
    }


    //
    // Tests below assume DML and DDL can be concurrent (and exit quietly if not possible)
    //

    //
    // Section 1: Test all types of DDL while a transaction doing a scan is open
    //
    @Test
    public void beginWaitDropScanGroup() throws Exception {
        beginWaitDDLThenScan(DDLOp.DROP_TABLE, null);
    }

    @Test
    public void beginWaitDropScanPK() throws Exception {
        beginWaitDDLThenScan(DDLOp.DROP_TABLE, Index.PRIMARY_KEY_CONSTRAINT);
    }

    @Test
    public void beginWaitDropScanIndex() throws Exception {
        beginWaitDDLThenScan(DDLOp.DROP_TABLE, "name");
    }

    @Test
    public void beginWaitCreateTableIndexScanGrop() throws Exception {
        beginWaitDDLThenScan(DDLOp.CREATE_TABLE_INDEX, null);
    }

    @Test
    public void beginWaitCreateTableIndexScanPK() throws Exception {
        beginWaitDDLThenScan(DDLOp.CREATE_TABLE_INDEX, Index.PRIMARY_KEY_CONSTRAINT);
    }

    @Test
    public void beginWaitCreateTableIndexScanOtherIndex() throws Exception {
        beginWaitDDLThenScan(DDLOp.CREATE_TABLE_INDEX, "name");
    }

    @Test
    public void beginWaitDropTableIndexScanGroup() throws Exception {
        beginWaitDDLThenScan(DDLOp.DROP_TABLE_INDEX, null);
    }

    @Test
    public void beginWaitDropTableIndexScanPK() throws Exception {
        beginWaitDDLThenScan(DDLOp.DROP_TABLE_INDEX, Index.PRIMARY_KEY_CONSTRAINT);
    }

    @Test
    public void beginWaitDropTableIndexScanSameIndex() throws Exception {
        beginWaitDDLThenScan(DDLOp.DROP_TABLE_INDEX, "name");
    }

    @Test
    public void beginWaitDropGroupScanGroup() throws Exception {
        beginWaitDDLThenScan(DDLOp.DROP_GROUP, null);
    }

    @Test
    public void beginWaitDropGroupScanPK() throws Exception {
        beginWaitDDLThenScan(DDLOp.DROP_GROUP, Index.PRIMARY_KEY_CONSTRAINT);
    }

    @Test
    public void beginWaitDropGroupScanIndex() throws Exception {
        beginWaitDDLThenScan(DDLOp.DROP_GROUP, "name");
    }

    @Test
    public void beginWaitDropSchemaScanGroup() throws Exception {
        beginWaitDDLThenScan(DDLOp.DROP_SCHEMA, null);
    }

    @Test
    public void beginWaitDropSchemaScanPK() throws Exception {
        beginWaitDDLThenScan(DDLOp.DROP_SCHEMA, Index.PRIMARY_KEY_CONSTRAINT);
    }

    @Test
    public void beginWaitDropSchemaScanIndex() throws Exception {
        beginWaitDDLThenScan(DDLOp.DROP_SCHEMA, "name");
    }

    @Test
    public void beginWaitAlterTableScanGroup() throws Exception {
        beginWaitDDLThenScan(DDLOp.ALTER_TABLE, null);
    }

    @Test
    public void beginWaitAlterTableScanPK() throws Exception {
        beginWaitDDLThenScan(DDLOp.ALTER_TABLE, Index.PRIMARY_KEY_CONSTRAINT);
    }

    @Test
    public void beginWaitAlterTableScanIndex() throws Exception {
        beginWaitDDLThenScan(DDLOp.ALTER_TABLE, "name");
    }

    @Test
    public void beginWaitCreateGroupIndexScanGroup() throws Exception {
        beginWaitDDLThenScan(DDLOp.CREATE_GROUP_INDEX, null);
    }

    @Test
    public void beginWaitCreateGroupIndexScanPK() throws Exception {
        beginWaitDDLThenScan(DDLOp.CREATE_GROUP_INDEX, Index.PRIMARY_KEY_CONSTRAINT);
    }

    @Test
    public void beginWaitCreateGroupIndexScanIndex() throws Exception {
        beginWaitDDLThenScan(DDLOp.CREATE_GROUP_INDEX, "name");
    }

    @Test
    public void beginWaitDropGroupIndexScanGroup() throws Exception {
        beginWaitDDLThenScan(DDLOp.DROP_GROUP_INDEX, null);
    }

    @Test
    public void beginWaitDropGroupIndexScanPK() throws Exception {
        beginWaitDDLThenScan(DDLOp.DROP_GROUP_INDEX, Index.PRIMARY_KEY_CONSTRAINT);
    }

    @Test
    public void beginWaitDropGroupIndexScanIndex() throws Exception {
        beginWaitDDLThenScan(DDLOp.DROP_GROUP_INDEX, "name");
    }

    @Test
    public void beginWaitRenameTableScanGroup() throws Exception {
        beginWaitDDLThenScan(DDLOp.RENAME_TABLE, null);
    }

    @Test
    public void beginWaitRenameTableScanPK() throws Exception {
        beginWaitDDLThenScan(DDLOp.RENAME_TABLE, Index.PRIMARY_KEY_CONSTRAINT);
    }

    @Test
    public void beginWaitRenameTableScanIndex() throws Exception {
        beginWaitDDLThenScan(DDLOp.RENAME_TABLE, "name");
    }

    //
    // Section 2: Test IUD against a concurrent DROP, then I against all other DDLs
    //

    @Test
    public void insertWithConcurrentDropTable() throws Exception {
        dmlWaitAndDDL(IUDType.INSERT, null, newChildCols(), DDLOp.DROP_TABLE);
    }

    @Test
    public void updateWithConcurrentDropTable() throws Exception {
        dmlWaitAndDDL(IUDType.UPDATE, oldChildCols(), newChildCols(), DDLOp.DROP_TABLE);
    }

    @Test
    public void deleteWithConcurrentDropTable() throws Exception {
        dmlWaitAndDDL(IUDType.DELETE, oldChildCols(), null, DDLOp.DROP_TABLE);
    }

    @Test
    public void insertWithConcurrentCreateTableIndex() throws Exception {
        dmlWaitAndDDL(IUDType.INSERT, null, newChildCols(), DDLOp.CREATE_TABLE_INDEX);
    }

    @Test
    public void insertChildWithConcurrentCreateGroupIndex() throws Exception {
        dmlWaitAndDDL(IUDType.INSERT, null, newChildCols(), DDLOp.CREATE_GROUP_INDEX);
    }

    @Test
    public void insertParentWithConcurrentCreateGroupIndex() throws Exception {
        dmlWaitAndDDL(IUDType.INSERT, null, newParentCols(), DDLOp.CREATE_GROUP_INDEX);
    }

    @Test
    public void insertWithConcurrentDropTableIndex() throws Exception {
        dmlWaitAndDDL(IUDType.INSERT, null, newChildCols(), DDLOp.DROP_TABLE_INDEX);
    }

    @Test
    public void insertChildWithConcurrentDropGroupIndex() throws Exception {
        dmlWaitAndDDL(IUDType.INSERT, null, newChildCols(), DDLOp.DROP_GROUP_INDEX);
    }

    @Test
    public void insertParentWithConcurrentDropGroupIndex() throws Exception {
        dmlWaitAndDDL(IUDType.INSERT, null, newParentCols(), DDLOp.DROP_GROUP_INDEX);
    }

    @Test
    public void insertWithConcurrentDropGroup() throws Exception {
        dmlWaitAndDDL(IUDType.INSERT, null, newChildCols(), DDLOp.DROP_GROUP);
    }

    @Test
    public void insertWithConcurrentDropSchema() throws Exception {
        dmlWaitAndDDL(IUDType.INSERT, null, newChildCols(), DDLOp.DROP_SCHEMA);
    }

    @Test
    public void insertWithConcurrentRenameTable() throws Exception {
        dmlWaitAndDDL(IUDType.INSERT, null, newChildCols(), DDLOp.RENAME_TABLE);
    }

    //
    // Section 3: Attempt to do IUD while DDL is in progress (DML waits and then gets exception)
    //

    @Test
    public void dropTableWithConcurrentInsert() throws Exception {
        ddlWaitAndDML(IUDType.INSERT, null, newChildCols(), DDLOp.DROP_TABLE);
    }


    //
    // Test helper methods
    //

    private static enum DDLOp {
        //
        // DDL that require special handling (lock and then transaction)
        //
        ALTER_TABLE(
                new Runner() {
                    @Override
                    public void run(Session session, DDLFunctions ddl) {
                        UserTable altered = joinedTableTemplates(true, false, false).get(1);
                        ddl.alterTable(
                                session, TABLE_NAME, altered,
                                Arrays.asList(TableChange.createModify("extra", "extra")),
                                Collections.<TableChange>emptyList(),
                                null
                        );
                    }
                }
        ),
        CREATE_TABLE_INDEX(
                new Runner() {
                    @Override
                    public void run(Session session, DDLFunctions ddl) {
                        UserTable table = joinedTableTemplates(false, true, false).get(1);
                        Index index = table.getIndex("extra");
                        ddl.createIndexes(session, Collections.singleton(index));
                    }
                }
        ),
        CREATE_GROUP_INDEX(
                new Runner() {
                    @Override
                    public void run(Session session, DDLFunctions ddl) {
                        List<UserTable> tables = joinedTableTemplates(false, false, true);
                        Index index = tables.get(0).getGroup().getIndex("g_i");
                        ddl.createIndexes(session, Collections.singleton(index));
                    }
                }
        ),
        DROP_TABLE_INDEX(
                new Runner() {
                    @Override
                    public void run(Session session, DDLFunctions ddl) {
                        ddl.dropTableIndexes(session, TABLE_NAME, Collections.singleton("name"));
                    }
                }
        ),
        DROP_GROUP_INDEX(
                new Runner() {
                    @Override
                    public void run(Session session, DDLFunctions ddl) {
                        ddl.dropGroupIndexes(session, TABLE_PARENT, Collections.singleton("g_i"));
                    }
                }
        ),
        DROP_TABLE(
                new Runner() {
                    @Override
                    public void run(Session session, DDLFunctions ddl) {
                        ddl.dropTable(session, TABLE_NAME);
                    }
                }
        ),
        DROP_GROUP(
                new Runner() {
                    @Override
                    public void run(Session session, DDLFunctions ddl) {
                        ddl.dropGroup(session, TABLE_PARENT);
                    }
                }
        ),
        DROP_SCHEMA(
                new Runner() {
                    @Override
                    public void run(Session session, DDLFunctions ddl) {
                        ddl.dropSchema(session, SCHEMA);
                    }
                }
        ),

        //
        // Not a special DDL, only a sanity check
        //
        RENAME_TABLE(new Runner() {
            @Override
            public void run(Session session, DDLFunctions ddl) {
                ddl.renameTable(session, TABLE_NAME, TABLE2_NAME);
            }
        })
        ;


        public interface Runner { void run(Session session, DDLFunctions ddl); }
        private final Runner runner;

        private DDLOp(Runner runner) {
            this.runner = runner;
        }

        public boolean waitsOnIUD() {
            return this != RENAME_TABLE;
        }

        public String inMark() {
            return name() + ">";
        }

        public String outMark() {
            return name() + "<";
        }

        public void run(Session session, DDLFunctions ddl) {
            runner.run(session, ddl);
        }
    }

    private void newRowsOrdered(List<NewRow> rows, final int fieldIndex) {
        assertTrue("not enough rows: " + rows, rows.size() > 1);
        List<NewRow> ordered = new ArrayList<NewRow>(rows);
        Collections.sort(ordered, new Comparator<NewRow>() {
            @Override @SuppressWarnings("unchecked")
            public int compare(NewRow o1, NewRow o2) {
                Object o1Field = o1.getFields().get(fieldIndex);
                Object o2Field = o2.getFields().get(fieldIndex);
                if (o1Field == null) {
                    return o2Field == null ? 0 : -1;
                }
                if (o2Field == null) {
                    return 1;
                }
                Comparable o1Comp = (Comparable)o1Field;
                Comparable o2Comp = (Comparable)o2Field;
                return o1Comp.compareTo(o2Comp);
            }
        });
    }

    private void scanWhileDropping(String indexName) throws Exception {
        final int tableId = largeEnoughTable(5000);
        final int indexId = ddl().getUserTable(session(), TABLE_NAME).getIndex(indexName).getIndexId();

        DelayScanCallableBuilder callableBuilder = new DelayScanCallableBuilder(aisGeneration(), tableId, indexId)
                .topOfLoopDelayer(1, 100, "SCAN: FIRST")
                .initialDelay(2500)
                .markFinish(false)
                .markOpenCursor(true)
                .withFullRowOutput(false);
        DelayableScanCallable scanCallable = callableBuilder.get();
        TimedCallable<Void> dropCallable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(final TimePoints timePoints, Session session) throws Exception {
                ConcurrencyAtomicsDXLService.hookNextDropTable(
                        session,
                        new Runnable() {
                            @Override
                            public void run() {
                                timePoints.mark("DROP: IN");
                            }
                        },
                        new Runnable() {
                            @Override
                            public void run() {
                                timePoints.mark("DROP: OUT");
                                Timing.sleep(50);
                            }
                        }
                );
                ddl().dropTable(session, TABLE_NAME); // will take ~5 seconds
                assertFalse(
                       "drop table hook still installed",
                       ConcurrencyAtomicsDXLService.isDropTableHookInstalled(session)
               );
                return null;
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<TimedResult<List<NewRow>>> scanFuture = executor.submit(scanCallable);
        Future<TimedResult<Void>> updateFuture = executor.submit(dropCallable);

        // No OldAIS for read only DML when AIS is transactional (DDL lock not strictly related, but good proxy)
        final boolean expectedRows = !isDDLLockOn();
        final String[] expectedTimePoints;
        if(isDDLLockOn()) {
            expectedTimePoints = new String[] {
                    "DROP: IN",
                    "(SCAN: OPEN CURSOR)>",
                    "DROP: OUT",
                    "SCAN: exception OldAISException"
            };
        } else {
            expectedTimePoints = new String[] {
                    "DROP: IN",
                    "(SCAN: OPEN CURSOR)>",
                    "<(SCAN: OPEN CURSOR)",
                    "SCAN: START",
                    "(SCAN: FIRST)>",
                    "<(SCAN: FIRST)",
                    "DROP: OUT"
            };
        }

        try {
            scanFuture.get();
            // If exception is not thrown, TimePoint comparison will catch it
        } catch (ExecutionException e) {
            if (!OldAISException.class.equals(e.getCause().getClass())) {
                throw new RuntimeException("Expected a OldAISException!", e.getCause());
            }
        }

        // If exception is expected then scanFuture.get() would throw, so use ofNull
        TimedResult<Void> scanResult = TimedResult.ofNull(scanCallable.getTimePoints());
        TimedResult<Void> updateResult = updateFuture.get();

        new TimePointsComparison(scanResult, updateResult).verify(expectedTimePoints);

        if(expectedRows) {
            assertTrue("rows were expected!", scanCallable.getRowCount() > 0);
        } else {
            assertTrue("rows weren't empty!", scanCallable.getRowCount() == 0);
        }
    }

    private void beginWaitDDLThenScan(final DDLOp op, String indexName) throws Exception {
        if(isDDLLockOn()) {
            return;
        }

        final int tableId = createJoinedTablesWithTwoRowsEach().get(1);
        final int indexId = (indexName == null) ? 0 : indexId(SCHEMA, TABLE, indexName);
        if(op == DDLOp.DROP_GROUP_INDEX) {
            DDLOp.CREATE_GROUP_INDEX.run(session(), ddl());
            updateAISGeneration();
        }

        DelayableScanCallable scanCallable = new DelayScanCallableBuilder(aisGeneration(), tableId, indexId)
                .topOfLoopDelayer(1, 100, "SCAN: FIRST")
                .initialDelay(1500)
                .markFinish(true)
                .markOpenCursor(false)
                .withFullRowOutput(false)
                .withExplicitTxn(true)
                .get();

        TimedCallable<Void> ddlCallable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(final TimePoints timePoints, Session session) throws Exception {
                Timing.sleep(500);
                timePoints.mark(op.inMark());
                op.run(session, ddl());
                timePoints.mark(op.outMark());
                return null;
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<TimedResult<List<NewRow>>> scanFuture = executor.submit(scanCallable);
        Future<TimedResult<Void>> ddlFuture = executor.submit(ddlCallable);

        TimedResult scanResult = scanFuture.get();
        TimedResult updateResult = ddlFuture.get();
        new TimePointsComparison(scanResult, updateResult).verify(
                "TXN: BEGAN",
                op.inMark(),
                op.outMark(),
                "SCAN: START",
                "(SCAN: FIRST)>",
                "<(SCAN: FIRST)",
                "SCAN: FINISH",
                "TXN: COMMITTED"
        );

        assertEquals("Scanned rows count", 2, scanCallable.getRowCount());
    }

    private void dmlWaitAndDDL(IUDType iudType, Object[] oldCols, Object[] newCols, final DDLOp ddlOp) throws Exception {
        if(isDDLLockOn()) {
            return;
        }

        final int colCount;
        if(oldCols != null && newCols != null) {
            colCount = oldCols.length;
            assertEquals("Old and new col count", oldCols.length, newCols.length);
        } else if(oldCols != null) {
            colCount = oldCols.length;
        } else {
            colCount = newCols.length;
        }

        final int POST_DML_WAIT = 1500;
        final List<Integer> tIds = createJoinedTablesWithTwoRowsEach();
        final int tableId = (colCount == 2) ? tIds.get(0) : tIds.get(1);

        if(ddlOp == DDLOp.DROP_GROUP_INDEX) {
            DDLOp.CREATE_GROUP_INDEX.run(session(), ddl());
            updateAISGeneration();
        }

        final NewRow oldRow = (oldCols != null) ? createNewRow(tableId, oldCols) : null;
        final NewRow newRow = (newCols != null) ? createNewRow(tableId, newCols) : null;

        TimedCallable<Void> dmlCallable = new DelayableIUDCallable(iudType, oldRow, newRow, 0, 0, POST_DML_WAIT);
        TimedCallable<Void> ddlCallable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(TimePoints timePoints, Session session) throws Exception {
                Timing.sleep(500);
                timePoints.mark(ddlOp.inMark());
                ddlOp.run(session, ddl());
                timePoints.mark(ddlOp.outMark());
                return null;
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<TimedResult<Void>> dmlFuture = executor.submit(dmlCallable);
        Future<TimedResult<Void>> ddlFuture = executor.submit(ddlCallable);

        TimedResult<Void> dmlResult = dmlFuture.get();
        TimedResult<Void> ddlResult = ddlFuture.get();

        final String[] expected = new String[] {
                iudType.startMark(),
                iudType.inMark(),
                ddlOp.inMark(),
                iudType.outMark(),
                ddlOp.outMark()
        };

        if(!ddlOp.waitsOnIUD()) {
            String tmp = expected[3];
            expected[3] = expected[4];
            expected[4] = tmp;
        }

        new TimePointsComparison(dmlResult, ddlResult).verify(expected);
    }

    private void ddlWaitAndDML(IUDType iudType, Object[] oldCols, Object[] newCols, final DDLOp ddlOp) throws Exception {
        if(isDDLLockOn()) {
            return;
        }

        final int colCount;
        if(oldCols != null && newCols != null) {
            colCount = oldCols.length;
            assertEquals("Old and new col count", oldCols.length, newCols.length);
        } else if(oldCols != null) {
            colCount = oldCols.length;
        } else {
            colCount = newCols.length;
        }

        final int DDL_PRE_COMMIT_WAIT = 1500;
        final List<Integer> tIds = createJoinedTablesWithTwoRowsEach();
        final int tableId = (colCount == 2) ? tIds.get(0) : tIds.get(1);

        if(ddlOp == DDLOp.DROP_GROUP_INDEX) {
            DDLOp.CREATE_GROUP_INDEX.run(session(), ddl());
            updateAISGeneration();
        }

        final NewRow oldRow = (oldCols != null) ? createNewRow(tableId, oldCols) : null;
        final NewRow newRow = (newCols != null) ? createNewRow(tableId, newCols) : null;

        TimedCallable<Void> dmlCallable = new DelayableIUDCallable(iudType, oldRow, newRow, 500, 0, 0);
        TimedCallable<Void> ddlCallable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(TimePoints timePoints, Session session) throws Exception {
                AtomicInteger fireCount = new AtomicInteger(0);
                delayInterestingDDLCommit(session, fireCount, DDL_PRE_COMMIT_WAIT);
                timePoints.mark(ddlOp.inMark());
                ddlOp.run(session, ddl());
                timePoints.mark(ddlOp.outMark());
                assertEquals("DDL hook fire count", 2, fireCount.get());
                return null;
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<TimedResult<Void>> dmlFuture = executor.submit(dmlCallable);
        Future<TimedResult<Void>> ddlFuture = executor.submit(ddlCallable);

        TimedResult<Void> ddlResult = ddlFuture.get(); // DDL first to catch hook errors

        try {
            dmlFuture.get();
            // If exception doesn't throw time points check will catch it
        } catch(ExecutionException e) {
            // expected
        }

        final String[] expected = new String[] {
                ddlOp.inMark(),
                iudType.startMark(),
                iudType.inMark(),
                ddlOp.outMark(),
                iudType.exceptionMark(TableChangedByDDLException.class)
        };

        new TimePointsComparison(TimedResult.ofNull(dmlCallable.getTimePoints()), ddlResult).verify(expected);
    }

    private void delayInterestingDDLCommit(Session session, final AtomicInteger fires, final long delay) {
        final TransactionService.Callback preCommitCB = new TransactionService.Callback() {
            @Override
            public void run(Session session, long timestamp) {
                fires.incrementAndGet();
                Timing.sleep(delay);
            }
        };
        TransactionService.Callback endCB = new TransactionService.Callback() {
            @Override
            public void run(Session session, long timestamp) {
                fires.incrementAndGet();
                txnService().addCallback(session, TransactionService.CallbackType.PRE_COMMIT, preCommitCB);
            }
        };
        txnService().addCallback(session, TransactionService.CallbackType.END, endCB);
    }

    private Object[] largeEnoughNewRow(int id, int pid, String nameFormat) {
        return new Object[]{ id, pid, String.format(nameFormat, id) };
    }

    private void largeEnoughWriteRows(int tableId, int count, String nameFormat) {
        for(int i = 1; i <= count; ++i) {
            writeRow(tableId, largeEnoughNewRow(i, i, nameFormat));
        }
    }

    /**
     * Creates a table with enough rows that it takes a while to drop it
     * @param msForDropping how long (at least) it should take to drop this table
     * @return the table's id
     * @throws InvalidOperationException if ever encountered
     */
    private int largeEnoughTable(long msForDropping) throws InvalidOperationException {
        final String NAME_FORMAT_INITIAL = "King Frosty %d";
        final String NAME_FORMAT_EXPANDING = "King Melty %d";
        final String NAME_FORMAT_FINAL = "King Snowy %d";

        if(lastLargeEnoughMS != msForDropping) {
            lastLargeEnoughCount = 0;
        }

        int parentId = createTable(SCHEMA, TABLE+"parent", "id int not null primary key");
        writeRow(parentId, 1);
        final String[] childTableDDL = {"id int not null primary key", "pid int", "name varchar(32)", "UNIQUE(name)",
                "GROUPING FOREIGN KEY (pid) REFERENCES " +TABLE+"parent(id)"};

        // Use previously computed row count if available
        int tableId = createTable(SCHEMA, TABLE, childTableDDL);
        if(lastLargeEnoughCount != 0) {
            largeEnoughWriteRows(tableId, lastLargeEnoughCount, NAME_FORMAT_FINAL);
            return tableId;
        }

        // Start estimate by how long it takes to insert for desired time
        int rowCount = 1;
        final long writeStart = System.currentTimeMillis();
        while (System.currentTimeMillis() - writeStart < msForDropping) {
            writeRow(tableId, largeEnoughNewRow(rowCount, rowCount, NAME_FORMAT_INITIAL));
            ++rowCount;
        }

        for(;;) {
            final long dropStart = System.currentTimeMillis();
            ddl().dropTable(session(), TABLE_NAME);
            final long dropTime = System.currentTimeMillis() - dropStart;
            if(dropTime > msForDropping) {
                lastLargeEnoughMS = msForDropping;
                lastLargeEnoughCount = rowCount;
                break;
            }

            // Compute how fast we dropped, estimate how many more we need (plus some slop)
            float rowsPerMS = rowCount / (float)dropTime;
            float neededMS = (msForDropping - dropTime) * 1.25f;
            rowCount += (int)(rowsPerMS * neededMS);
            tableId = createTable(SCHEMA, TABLE, childTableDDL);
            largeEnoughWriteRows(tableId, rowCount, NAME_FORMAT_EXPANDING);
        }

        tableId = createTable(SCHEMA, TABLE, childTableDDL);
        largeEnoughWriteRows(tableId, rowCount, NAME_FORMAT_FINAL);
        return tableId;
    }
}
