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
import com.akiban.ais.model.aisb2.NewUserTableBuilder;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.service.dxl.DXLReadWriteLockHook;
import com.akiban.server.service.servicemanager.GuicedServiceManager;
import com.akiban.server.test.mt.MTBase;
import com.akiban.server.test.mt.mtutil.TimePointsComparison;
import com.akiban.server.test.mt.mtutil.TimedCallable;
import com.akiban.server.test.mt.mtutil.TimedResult;
import com.akiban.server.service.config.Property;
import com.akiban.server.service.dxl.ConcurrencyAtomicsDXLService;
import com.akiban.server.service.dxl.DXLService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

class ConcurrentAtomicsBase extends MTBase {
    protected static final String SCHEMA = "cold";
    protected static final String SCHEMA2 = "brisk";
    protected static final String PARENT = "icy";
    protected static final String TABLE = "frosty";
    protected static final String TABLE2 = "frosty2";
    protected static final TableName TABLE_PARENT = new TableName(SCHEMA, PARENT);
    protected static final TableName TABLE_NAME = new TableName(SCHEMA, TABLE);
    protected static final TableName TABLE2_NAME = new TableName(SCHEMA, TABLE2);

    // ApiTestBase interface

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider().bind(DXLService.class, ConcurrencyAtomicsDXLService.class);
    }

    @Override
    protected Collection<Property> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    // ConcurrentAtomicsBase interface

    protected void scanUpdateConfirm(int tableId,
                                     TimedCallable<List<NewRow>> scanCallable,
                                     TimedCallable<Void> updateCallable,
                                     List<NewRow> scanCallableExpected,
                                     List<NewRow> endStateExpected)
            throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<TimedResult<List<NewRow>>> scanFuture = executor.submit(scanCallable);
        Future<TimedResult<Void>> updateFuture = executor.submit(updateCallable);

        TimedResult<List<NewRow>> scanResult = scanFuture.get();
        TimedResult<Void> updateResult = updateFuture.get();

        List<String> timePoints = new ArrayList<String>();
        timePoints.addAll(Arrays.asList("SCAN: START",
                                        "(SCAN: PAUSE)>",
                                        "UPDATE: IN",
                                        "UPDATE: OUT",
                                        "<(SCAN: PAUSE)",
                                        "SCAN: FINISH"));

        // 'update: out' will get blocked until scan is done if top level r/w lock is on
        if(DXLReadWriteLockHook.only().isDMLLockEnabled()) {
            timePoints.add(timePoints.remove(3));
        }

        new TimePointsComparison(scanResult, updateResult).verify(timePoints.toArray(new String[timePoints.size()]));

        assertEquals("rows scanned (in order)", scanCallableExpected, scanResult.getItem());
        expectFullRows(tableId, endStateExpected.toArray(new NewRow[endStateExpected.size()]));
    }

    protected static List<UserTable> joinedTableTemplates(String parentSchema, String childSchema,
                                                          boolean extraParentKey, boolean alteredChild,
                                                          boolean extraChildKey, boolean groupIndex) {
        NewAISBuilder builder = AISBBasedBuilder.create();
        NewUserTableBuilder parentBuilder = builder.userTable(parentSchema, PARENT);
        parentBuilder.colLong("id", false).colLong("value", true).pk("id");
        if(extraParentKey) {
            parentBuilder.key("value", "value");
        }
        NewUserTableBuilder childBuilder = builder.userTable(childSchema, TABLE);
        childBuilder.colLong("id", false).colString("name", 32, true).
                pk("id").key("name", "name").
                joinTo(parentSchema, PARENT, "fk_0").on("id", "id");
        if(alteredChild) {
            childBuilder.colString("extra", 32, true);
        } else {
            childBuilder.colLong("extra", true);
        }
        if(extraChildKey) {
            childBuilder.key("extra", "extra");
        }
        if(groupIndex) {
            builder.groupIndex("g_i", Index.JoinType.LEFT).on(childSchema, TABLE, "extra").and(parentSchema, PARENT, "value");
        }
        return Arrays.asList(
                builder.ais().getUserTable(parentSchema, PARENT),
                builder.ais().getUserTable(childSchema, TABLE)
        );
    }

    protected List<Integer> createJoinedTables(String parentSchema, String childSchema) {
        return createJoinedTables(parentSchema, childSchema, false, false, false, false);
    }
    protected List<Integer> createJoinedTables(String parentSchema, String childSchema,
                                               boolean extraParentKey, boolean alteredChild,
                                               boolean extraChildKey, boolean groupIndex) {
        List<UserTable> tables = joinedTableTemplates(parentSchema, childSchema, extraParentKey, alteredChild, extraChildKey, groupIndex);
        ddl().createTable(session(), tables.get(0));
        ddl().createTable(session(), tables.get(1));
        updateAISGeneration();
        return Arrays.asList(
                tableId(tables.get(0).getName()),
                tableId(tables.get(1).getName())
        );
    }

    protected static Object[] newParentCols() {
        return new Object[] { 100L, 10000L };
    }

    protected static Object[] newChildCols() {
        return new Object[] { 100L, "BOBSLED", 1000L };
    }

    protected static Object[] oldChildCols() {
        return new Object[] { 1L, "the snowman", 10L };
    }

    protected List<Integer> createJoinedTablesWithTwoRowsEach() {
        List<Integer> ids = createJoinedTables(SCHEMA, SCHEMA);
        writeRows(
                createNewRow(ids.get(0), 1L, 100L),
                createNewRow(ids.get(1), oldChildCols()),
                createNewRow(ids.get(0), 2L, 200L),
                createNewRow(ids.get(1), 2L, "mr melty", 20L)
        );
        return ids;
    }

    protected int tableWithTwoRows() throws InvalidOperationException {
        int id = createTable(SCHEMA, TABLE, "id int not null primary key", "name varchar(32)");
        createIndex(SCHEMA, TABLE, "name", "name");
        writeRows(
            createNewRow(id, 1L, "the snowman"),
            createNewRow(id, 2L, "mr melty")
        );
        return id;
    }
}
