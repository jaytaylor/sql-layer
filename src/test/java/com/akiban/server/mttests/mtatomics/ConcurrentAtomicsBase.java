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

package com.akiban.server.mttests.mtatomics;

import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.itests.ApiTestBase;
import com.akiban.server.mttests.mtutil.TimePointsComparison;
import com.akiban.server.mttests.mtutil.TimedCallable;
import com.akiban.server.mttests.mtutil.TimedResult;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.Property;
import com.akiban.server.service.d_l.DXLService;
import com.akiban.server.service.d_l.ScanhooksDXLService;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

class ConcurrentAtomicsBase extends ApiTestBase {
    protected static final String SCHEMA = "cold";
    protected static final String TABLE = "frosty";

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

        new TimePointsComparison(scanResult, updateResult).verify(
                "SCAN: START",
                "(SCAN: PAUSE)>",
                "UPDATE: IN",
                "UPDATE: OUT",
                "<(SCAN: PAUSE)",
                "SCAN: RETRY",
                "SCAN: FINISH"
        );

        assertEquals("rows scanned (in order)", scanCallableExpected, scanResult.getItem());
        expectFullRows(tableId, endStateExpected.toArray(new NewRow[endStateExpected.size()]));
    }

    protected int tableWithTwoRows() throws InvalidOperationException {
        int id = createTable(SCHEMA, TABLE, "id int key", "name varchar(32)", "key(name)");
        writeRows(
            createNewRow(id, 1L, "the snowman"),
            createNewRow(id, 2L, "mr melty")
        );
        return id;
    }

    @Override
    protected TestServiceServiceFactory createServiceFactory(Collection<Property> startupConfigProperties) {
        return new ScanhooksServiceFactory(startupConfigProperties);
    }

    private static class ScanhooksServiceFactory extends TestServiceServiceFactory {
        private ScanhooksServiceFactory(Collection<Property> startupConfigProperties) {
            super(startupConfigProperties);
        }

        @Override
        public Service<DXLService> dxlService() {
            return new ScanhooksDXLService();
        }
    }
}
