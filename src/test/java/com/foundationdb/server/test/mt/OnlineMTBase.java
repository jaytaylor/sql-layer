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

package com.foundationdb.server.test.mt;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.service.dxl.OnlineDDLMonitor;
import com.foundationdb.server.test.mt.util.ConcurrentTestBuilder;
import com.foundationdb.server.test.mt.util.ConcurrentTestBuilderImpl;
import com.foundationdb.server.test.mt.util.MonitoredThread;
import com.foundationdb.server.test.mt.util.OperatorCreator;
import com.foundationdb.server.test.mt.util.ThreadHelper;
import com.foundationdb.server.test.mt.util.ThreadHelper.UncaughtHandler;
import com.foundationdb.server.test.mt.util.ThreadMonitor;
import com.foundationdb.server.test.mt.util.TimeMarkerComparison;
import com.foundationdb.sql.server.ServerSession;
import com.foundationdb.sql.types.DataTypeDescriptor;

import java.util.List;

import static org.junit.Assert.assertEquals;

public abstract class OnlineMTBase extends MTBase
{
    //
    // Required from derived
    //

    protected abstract String getDDL();

    protected abstract String getDDLSchema();

    protected abstract List<Row> getGroupExpected();

    protected abstract List<Row> getOtherExpected();
    //to override

    protected abstract OperatorCreator getGroupCreator();

    protected abstract OperatorCreator getOtherCreator();

    protected abstract void postCheckAIS(AkibanInformationSchema ais);

    protected Class<? extends Exception> getFailingExceptionClass() {
        return store().getOnlineDMLFailureException();

    }

    protected String getFailingMarkString() {
        return getFailingExceptionClass().getSimpleName();
    }

    //
    // Used by derived
    //

    /** As {@link #dmlPostMetaToPreFinal(OperatorCreator, List)} defaulting to expected failure. */
    protected void dmlPreToPostMetadata(OperatorCreator dmlCreator) {
        dmlPreToPostMetadata(dmlCreator, getGroupExpected(), true);
    }

    /** DML transaction starting prior to DDL METADATA and committing after DDL METADATA. */
    protected  void dmlPreToPostMetadata(OperatorCreator dmlCreator, List<Row> expectedRows, boolean isDMLFailing) {
        dmlPreToPostMetadata_Check(dmlPreToPostMetadata_Build(dmlCreator, isDMLFailing, null, null, null, null), expectedRows, isDMLFailing);
    }

    protected  void dmlPreToPostMetadata(OperatorCreator dmlCreator, List<Row> expectedRows, boolean isDMLFailing,
                                         List<DataTypeDescriptor> descriptors, List<String> columnNames, OnlineCreateTableAsMT.TestSession  server, String sqlQuery) {
        dmlPreToPostMetadata_Check(dmlPreToPostMetadata_Build(dmlCreator, isDMLFailing, descriptors, columnNames, server, sqlQuery), expectedRows, isDMLFailing);
    }


        /**This creates a ConcurrentTestBuidlerIMpl that each of these calls adds or modifies then the final call builds it into
         * a monitor list
         */
    protected List<MonitoredThread> dmlPreToPostMetadata_Build(OperatorCreator dmlCreator, boolean isDMLFailing,
                                                               List<DataTypeDescriptor> descriptors, List<String> columnNames,
                                                               OnlineCreateTableAsMT.TestSession  server, String sqlQuery) {
        return ConcurrentTestBuilderImpl
            .create()
            .add("DDL", getDDLSchema(), getDDL())
            .sync("a", OnlineDDLMonitor.Stage.PRE_METADATA)
            .sync("b", OnlineDDLMonitor.Stage.PRE_TRANSFORM)
            .mark(OnlineDDLMonitor.Stage.PRE_METADATA, OnlineDDLMonitor.Stage.POST_METADATA)
            .add("DML", dmlCreator)
            .sync("a", ThreadMonitor.Stage.POST_BEGIN)
            .sync("b", ThreadMonitor.Stage.PRE_SCAN)
            .mark(ThreadMonitor.Stage.PRE_BEGIN, ThreadMonitor.Stage.PRE_COMMIT)
            .rollbackRetry(!isDMLFailing)
            .build(this, descriptors, columnNames, server, sqlQuery);
    }

    protected  void dmlPreToPostMetadata_Check(List<MonitoredThread> threads, List<Row> expectedRows, boolean isDMLFailing) {
        if(isDMLFailing) {
            UncaughtHandler handler = ThreadHelper.startAndJoin(threads);
            assertEquals("ddl failure", null, handler.thrown.get(threads.get(0)));
        } else {
            ThreadHelper.runAndCheck(threads);
        }
        new TimeMarkerComparison(threads).verify("DML:PRE_BEGIN",
                                                 "DDL:PRE_METADATA",
                                                 "DDL:POST_METADATA",
                                                 "DML:PRE_COMMIT",
                                                 isDMLFailing ? "DML:"+getFailingMarkString() : null);
        assertEquals("DML row count", 1, threads.get(1).getScannedRows().size());
        checkExpectedRows(expectedRows);
    }

    /** As {@link #dmlPreToPostFinal(OperatorCreator, List, boolean)} with default expected pass. */
    protected void dmlPostMetaToPreFinal(OperatorCreator dmlCreator, List<Row> finalGroupRows) {
        dmlPostMetaToPreFinal(dmlCreator, finalGroupRows, true);
    }

    /** DML transaction starting after DDL METADATA and committing prior DDL FINAL. */
    protected void dmlPostMetaToPreFinal(OperatorCreator dmlCreator, List<Row> finalGroupRows, boolean isDMLPassing) {
        dmlPostMetaToPreFinal(dmlCreator, finalGroupRows, isDMLPassing, null, null, null, null);
    }

        /** DML transaction starting after DDL METADATA and committing prior DDL FINAL. */
        protected void dmlPostMetaToPreFinal(OperatorCreator dmlCreator, List<Row> finalGroupRows, boolean isDMLPassing,
                                             List<DataTypeDescriptor> descriptors, List<String> columnNames,
                                             OnlineCreateTableAsMT.TestSession  server, String sqlQuery){
        ConcurrentTestBuilder builder = ConcurrentTestBuilderImpl
            .create()
            .add("DDL", getDDLSchema(), getDDL())
            .sync("a", OnlineDDLMonitor.Stage.PRE_TRANSFORM)
            .sync("b", OnlineDDLMonitor.Stage.PRE_FINAL)
            .mark(OnlineDDLMonitor.Stage.POST_METADATA, OnlineDDLMonitor.Stage.POST_FINAL)
            .add("DML", dmlCreator)
            .sync("a", ThreadMonitor.Stage.START)
            .sync("b", ThreadMonitor.Stage.FINISH)
            .mark(ThreadMonitor.Stage.PRE_BEGIN, ThreadMonitor.Stage.POST_COMMIT)
            .rollbackRetry(isDMLPassing);
        final List<MonitoredThread> threads;
        if(isDMLPassing) {
            threads = builder.build(this, descriptors, columnNames, server, sqlQuery);
            ThreadHelper.runAndCheck(threads);
        } else {
            threads = builder.build(this);
            UncaughtHandler handler = ThreadHelper.startAndJoin(threads);
            assertEquals("ddl failure", null, handler.thrown.get(threads.get(0)));
        }
        new TimeMarkerComparison(threads).verify("DDL:POST_METADATA",
                                                 "DML:PRE_BEGIN",
                                                 isDMLPassing ? "DML:POST_COMMIT" : "DML:"+getFailingMarkString(),
                                                 "DDL:POST_FINAL");
        if(isDMLPassing) {
            assertEquals("DML row count", 1, threads.get(1).getScannedRows().size());
        }
        checkExpectedRows(finalGroupRows);
    }

    /** As {@link #dmlPreToPostFinal(OperatorCreator, List, boolean)} with default expected failure. */
    protected void dmlPreToPostFinal(OperatorCreator dmlCreator) {
        dmlPreToPostMetadata(dmlCreator, getGroupExpected(), true);
    }
    protected void dmlPreToPostFinal(OperatorCreator dmlCreator, List<Row> expectedRows, boolean isDMLFailing){
        dmlPreToPostFinal(dmlCreator, expectedRows, isDMLFailing, null, null, null, null);
    }
    /** DML transaction starting prior to DDL FINAL and committing after DDL FINAL. */
    protected void dmlPreToPostFinal(OperatorCreator dmlCreator, List<Row> expectedRows, boolean isDMLFailing,
                                     List<DataTypeDescriptor> descriptors, List<String> columnNames,
                                     OnlineCreateTableAsMT.TestSession  server, String sqlQuery) {
        List<MonitoredThread> threads = ConcurrentTestBuilderImpl
            .create()
            .add("DDL", getDDLSchema(), getDDL())
            .sync("a", OnlineDDLMonitor.Stage.POST_TRANSFORM)
            .sync("b", ThreadMonitor.Stage.FINISH)
            .mark(OnlineDDLMonitor.Stage.PRE_FINAL, OnlineDDLMonitor.Stage.POST_FINAL)
            .add("DML", dmlCreator)
            .sync("a", ThreadMonitor.Stage.PRE_SCAN)
            .sync("b", ThreadMonitor.Stage.POST_SCAN)
            .mark(ThreadMonitor.Stage.POST_BEGIN, ThreadMonitor.Stage.PRE_COMMIT)
            .build(this, descriptors, columnNames, server, sqlQuery);
        if(isDMLFailing) {
            UncaughtHandler handler = ThreadHelper.startAndJoin(threads);
            assertEquals("ddl failure", null, handler.thrown.get(threads.get(0)));
        } else {
            ThreadHelper.runAndCheck(threads);
        }
        new TimeMarkerComparison(threads).verify("DML:POST_BEGIN",
                                                 "DDL:PRE_FINAL",
                                                 "DDL:POST_FINAL",
                                                 "DML:PRE_COMMIT",
                                                 isDMLFailing ? "DML:"+getFailingMarkString() : null);
        assertEquals("DML row count", 1, threads.get(1).getScannedRows().size());
        checkExpectedRows(expectedRows);
    }

    protected void checkExpectedRows(List<Row> expectedRows) {
        checkExpectedRows(expectedRows, getGroupCreator());
    }

    protected void checkExpectedRows(List<Row> expectedRows, OperatorCreator groupCreator) {
        compareRows(expectedRows, runPlanTxn(groupCreator));
        postCheckAIS(ais());
        List<Row> otherExpected = getOtherExpected();
        if(otherExpected != null) {
            compareRows(otherExpected, runPlanTxn(getOtherCreator()));
        }
    }
}
