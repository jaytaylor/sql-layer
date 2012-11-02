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

import com.akiban.ais.model.TableName;
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
    protected static final String TABLE = "frosty";
    protected static final TableName TABLE_NAME = new TableName(SCHEMA, TABLE);

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
