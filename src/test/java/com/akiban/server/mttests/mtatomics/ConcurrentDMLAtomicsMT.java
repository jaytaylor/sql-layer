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

import com.akiban.ais.model.TableName;
import com.akiban.server.api.dml.EasyUseColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.mttests.mtutil.TimePoints;
import com.akiban.server.mttests.mtutil.TimedCallable;
import com.akiban.server.mttests.mtutil.Timing;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public final class ConcurrentDMLAtomicsMT extends ConcurrentAtomicsBase {

    @Test
    public void updateUnIndexedColumnWhileScanning() throws Exception {
        final int tableId = tableWithTwoRows();
        final int SCAN_WAIT = 5000;

        int indexId = ddl().getUserTable(session(), new TableName(SCHEMA, TABLE)).getIndex("PRIMARY").getIndexId();
        TimedCallable<List<NewRow>> scanCallable
                = new DelayScanCallableBuilder(tableId, indexId).topOfLoopDelayer(1, SCAN_WAIT, "SCAN: PAUSE").get(ddl());
        TimedCallable<Void> updateCallable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(TimePoints timePoints, Session session) throws Exception {
                NewRow old = new NiceRow(tableId);
                old.put(0, 2L);
                NewRow updated = new NiceRow(tableId);
                updated.put(0, 2L);
                updated.put(1, "icebox");
                Timing.sleep(2000);
                timePoints.mark("UPDATE: IN");
                dml().updateRow(new SessionImpl(), old, updated, new EasyUseColumnSelector(1));
                timePoints.mark("UPDATE: OUT");
                return null;
            }
        };

        scanUpdateConfirm(
                tableId,
                scanCallable,
                updateCallable,
                Arrays.asList(
                        createNewRow(tableId, 1L, "the snowman"),
                        createNewRow(tableId, 2L, "icebox")
                ),
                Arrays.asList(
                        createNewRow(tableId, 1L, "the snowman"),
                        createNewRow(tableId, 2L, "icebox")
                )
        );
    }

    @Test
    public void updatePKColumnWhileScanning() throws Exception {
        final int tableId = tableWithTwoRows();
        final int SCAN_WAIT = 5000;

        int indexId = ddl().getUserTable(session(), new TableName(SCHEMA, TABLE)).getIndex("PRIMARY").getIndexId();
        TimedCallable<List<NewRow>> scanCallable
                = new DelayScanCallableBuilder(tableId, indexId).topOfLoopDelayer(1, SCAN_WAIT, "SCAN: PAUSE").get(ddl());
        TimedCallable<Void> updateCallable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(TimePoints timePoints, Session session) throws Exception {
                NewRow old = new NiceRow(tableId);
                old.put(0, 1L);
                NewRow updated = new NiceRow(tableId);
                updated.put(0, 5L);
                Timing.sleep(2000);
                timePoints.mark("UPDATE: IN");
                dml().updateRow(new SessionImpl(), old, updated, new EasyUseColumnSelector(0));
                timePoints.mark("UPDATE: OUT");
                return null;
            }
        };

        scanUpdateConfirm(
                tableId,
                scanCallable,
                updateCallable,
                Arrays.asList(
                        createNewRow(tableId, 1L, "the snowman"),
                        createNewRow(tableId, 2L, "mr melty"),
                        createNewRow(tableId, 5L, "the snowman")
                ),
                Arrays.asList(
                        createNewRow(tableId, 2L, "mr melty"),
                        createNewRow(tableId, 5L, "the snowman")
                )
        );
    }

    @Test
    public void updateIndexedColumnWhileScanning() throws Exception {
        final int tableId = tableWithTwoRows();
        final int SCAN_WAIT = 5000;

        int indexId = ddl().getUserTable(session(), new TableName(SCHEMA, TABLE)).getIndex("name").getIndexId();
        TimedCallable<List<NewRow>> scanCallable
                = new DelayScanCallableBuilder(tableId, indexId).topOfLoopDelayer(1, SCAN_WAIT, "SCAN: PAUSE").get(ddl());
//        scanCallable = TransactionalTimedCallable.withRunnable( scanCallable, 10, 1000 );
//        scanCallable = TransactionalTimedCallable.withoutRunnable(scanCallable);

        TimedCallable<Void> updateCallable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(TimePoints timePoints, Session session) throws Exception {
                NewRow oldSnowman = new NiceRow(tableId);
                oldSnowman.put(0, 2L);
                NewRow updatedSnowman = new NiceRow(tableId);
                updatedSnowman.put(0, 2L);
                updatedSnowman.put(1, "xtreme weather");

                NewRow oldMr = new NiceRow(tableId);
                oldMr.put(0, 1L);
                NewRow updatedMr = new NiceRow(tableId);
                updatedMr.put(1, "a snowman");

                Timing.sleep(2000);
                timePoints.mark("UPDATE: IN");
                dml().updateRow(new SessionImpl(), oldSnowman, updatedSnowman, new EasyUseColumnSelector(1));
                dml().updateRow(new SessionImpl(), oldMr, updatedMr, new EasyUseColumnSelector(1));
                timePoints.mark("UPDATE: OUT");
                return null;
            }
        };

        scanUpdateConfirm(
                tableId,
                scanCallable,
                updateCallable,
                Arrays.asList(
                        createNewRow(tableId, 2L, "mr melty"),
                        createNewRow(tableId, 2L, "xtreme weather")
                ),
                Arrays.asList(
                        createNewRow(tableId, 1L, "a snowman"),
                        createNewRow(tableId, 2L, "xtreme weather")
                )
        );
    }

    @Test
    public void updateIndexedColumnAndPKWhileScanning() throws Exception {
        final int tableId = tableWithTwoRows();
        final int SCAN_WAIT = 5000;

        int indexId = ddl().getUserTable(session(), new TableName(SCHEMA, TABLE)).getIndex("name").getIndexId();
        TimedCallable<List<NewRow>> scanCallable
                = new DelayScanCallableBuilder(tableId, indexId).topOfLoopDelayer(1, SCAN_WAIT, "SCAN: PAUSE").get(ddl());
//        scanCallable = TransactionalTimedCallable.withRunnable( scanCallable, 10, 1000 );
//        scanCallable = TransactionalTimedCallable.withoutRunnable(scanCallable);

        TimedCallable<Void> updateCallable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(TimePoints timePoints, Session session) throws Exception {
                NewRow oldSnowman = new NiceRow(tableId);
                oldSnowman.put(0, 2L);
                NewRow updatedSnowman = new NiceRow(tableId);
                updatedSnowman.put(0, 2L);
                updatedSnowman.put(1, "xtreme weather");

                NewRow oldMr = new NiceRow(tableId);
                oldMr.put(0, 1L);
                NewRow updatedMr = new NiceRow(tableId);
                updatedMr.put(0, 10L);
                updatedMr.put(1, "a snowman");

                Timing.sleep(2000);
                timePoints.mark("UPDATE: IN");
                dml().updateRow(new SessionImpl(), oldSnowman, updatedSnowman, new EasyUseColumnSelector(1));
                dml().updateRow(new SessionImpl(), oldMr, updatedMr, new EasyUseColumnSelector(0, 1));
                timePoints.mark("UPDATE: OUT");
                return null;
            }
        };

        scanUpdateConfirm(
                tableId,
                scanCallable,
                updateCallable,
                Arrays.asList(
                        createNewRow(tableId, 2L, "mr melty"),
                        createNewRow(tableId, 2L, "xtreme weather")
                ),
                Arrays.asList(
                        createNewRow(tableId, 2L, "xtreme weather"),
                        createNewRow(tableId, 10L, "a snowman")
                )
        );
    }
}
