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
import com.foundationdb.server.error.ConcurrentViolationException;
import com.foundationdb.server.service.dxl.OnlineDDLMonitor;
import com.foundationdb.server.test.mt.util.ConcurrentTestBuilder;
import com.foundationdb.server.test.mt.util.ConcurrentTestBuilderImpl;
import com.foundationdb.server.test.mt.util.MonitoredThread;
import com.foundationdb.server.test.mt.util.OperatorCreator;
import com.foundationdb.server.test.mt.util.ThreadHelper;
import com.foundationdb.server.test.mt.util.ThreadHelper.UncaughtHandler;
import com.foundationdb.server.test.mt.util.ThreadMonitor;
import com.foundationdb.server.test.mt.util.TimeMarkerComparison;
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

    protected abstract OperatorCreator getGroupCreator();

    protected abstract OperatorCreator getOtherCreator();

    protected abstract void postCheckAIS(AkibanInformationSchema ais);

    protected Class<? extends Exception> getFailingDMLExceptionClass() {
        return store().getOnlineDMLFailureException();
    }

    protected Class<? extends Exception> getFailingDDLExceptionClass() {
        return ConcurrentViolationException.class;
    }

    protected String getFailingDMLMarkString() {
        return getFailingDMLExceptionClass().getSimpleName();
    }

    protected String getFailingDDLMarkString() {
        return getFailingDDLExceptionClass().getSimpleName();
    }

    //
    // Used by derived
    //

    /** As {@link #dmlPostMetaToPreFinal(OperatorCreator, List)} defaulting to expected failure. */
    protected void dmlPreToPostMetadata(OperatorCreator dmlCreator) {
        dmlPreToPostMetadata(dmlCreator, getGroupExpected(), true);
    }

    /** DML transaction starting prior to DDL METADATA and committing after DDL METADATA. */
    protected  void dmlPreToPostMetadata(OperatorCreator dmlCreator,
                                         List<Row> expectedRows,
                                         boolean isDMLFailing) {
        dmlPreToPostMetadata_Check(dmlPreToPostMetadata_Build(dmlCreator, isDMLFailing, null, null, null, null), expectedRows, isDMLFailing);
    }

    protected  void dmlPreToPostMetadata(OperatorCreator dmlCreator,
                                         List<Row> expectedRows,
                                         boolean isDMLFailing,
                                         List<DataTypeDescriptor> descriptors,
                                         List<String> columnNames,
                                         OnlineCreateTableAsMT.TestSession  server,
                                         String sqlQuery,
                                         boolean ignoreNewPK) {
        dmlPreToPostMetadata_Check(dmlPreToPostMetadata_Build(dmlCreator, isDMLFailing, descriptors, columnNames, server, sqlQuery), expectedRows, isDMLFailing, ignoreNewPK);
    }


        /**This creates a ConcurrentTestBuidlerIMpl that each of these calls adds or modifies then the final call builds it into
         * a monitor list
         */
    protected List<MonitoredThread> dmlPreToPostMetadata_Build(OperatorCreator dmlCreator,
                                                               boolean isDMLFailing,
                                                               List<DataTypeDescriptor> descriptors,
                                                               List<String> columnNames,
                                                               OnlineCreateTableAsMT.TestSession  server,
                                                               String sqlQuery) {
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
    protected  void dmlPreToPostMetadata_Check(List<MonitoredThread> threads,
                                               List<Row> expectedRows,
                                               boolean isDMLFailing) {
        dmlPreToPostMetadata_Check(threads, expectedRows, isDMLFailing, false);
    }

    protected  void dmlPreToPostMetadata_Check(List<MonitoredThread> threads,
                                               List<Row> expectedRows,
                                               boolean isDMLFailing,
                                               boolean ignoreNewPK) {
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
                                                 isDMLFailing ? "DML:"+ getFailingDMLMarkString() : null);
        assertEquals("DML row count", 1, threads.get(1).getScannedRows().size());
        checkExpectedRows(expectedRows, ignoreNewPK);
    }

    /** As {@link #dmlPreToPostFinal(OperatorCreator, List, boolean)} with default expected pass. */
    protected void dmlPostMetaToPreFinal(OperatorCreator dmlCreator,
                                         List<Row> finalGroupRows) {
        dmlPostMetaToPreFinal(dmlCreator, finalGroupRows, true, true);
    }

    /** As {@link #dmlPreToPostFinal(OperatorCreator, List, boolean)} with default expected DML pass, DDL fail. */
    protected void dmlViolationPostMetaToPreFinal(OperatorCreator dmlCreator,
                                                  List<Row> finalGroupRows) {
        dmlPostMetaToPreFinal(dmlCreator, finalGroupRows, true, false);
    }

    /** DML transaction starting after DDL METADATA and committing prior DDL FINAL. */
    protected void dmlPostMetaToPreFinal(OperatorCreator dmlCreator,
                                         List<Row> finalGroupRows,
                                         boolean isDMLPassing,
                                         boolean isDDLPassing) {
        dmlPostMetaToPreFinal(dmlCreator, finalGroupRows, isDMLPassing, isDDLPassing, null, null, null, null, false);
    }

        /** DML transaction starting after DDL METADATA and committing prior DDL FINAL. */
    protected void dmlPostMetaToPreFinal(OperatorCreator dmlCreator,
                                         List<Row> finalGroupRows,
                                         boolean isDMLPassing,
                                         boolean isDDLPassing,
                                         List<DataTypeDescriptor> descriptors,
                                         List<String> columnNames,
                                         OnlineCreateTableAsMT.TestSession  server,
                                         String sqlQuery,
                                         boolean ignoreNewPK){

        // In the interest of determinism, DDL transform runs completely *before* DML starts.
        // The opposite ordering would fail the DDL directly instead (e.g. NotNullViolation vs ConcurrentViolation).
        ConcurrentTestBuilder builder = ConcurrentTestBuilderImpl
            .create()
            .add("DDL", getDDLSchema(), getDDL())
            .sync("a", OnlineDDLMonitor.Stage.POST_METADATA)
            .sync("b", OnlineDDLMonitor.Stage.POST_TRANSFORM)
            .sync("c", OnlineDDLMonitor.Stage.PRE_FINAL)
            .mark(OnlineDDLMonitor.Stage.POST_METADATA, OnlineDDLMonitor.Stage.PRE_FINAL)
            .add("DML", dmlCreator)
            .sync("a", ThreadMonitor.Stage.START)
            .sync("b", ThreadMonitor.Stage.PRE_BEGIN)
            .sync("c", ThreadMonitor.Stage.FINISH)
            .mark(ThreadMonitor.Stage.PRE_BEGIN, ThreadMonitor.Stage.POST_COMMIT)
            .rollbackRetry(isDMLPassing);

        final List<MonitoredThread> threads = builder.build(this, descriptors, columnNames, server, sqlQuery);
        ThreadHelper.startAndJoin(threads);
        new TimeMarkerComparison(threads).verify("DDL:POST_METADATA",
                                                 "DML:PRE_BEGIN",
                                                 "DML:" + (isDMLPassing ? "POST_COMMIT" : getFailingDMLMarkString()),
                                                 "DDL:PRE_FINAL",
                                                 isDDLPassing ? null : "DDL:" + getFailingDDLMarkString());
        if(isDMLPassing) {
            assertEquals("DML row count", 1, threads.get(1).getScannedRows().size());
        }
        checkExpectedRows(finalGroupRows, ignoreNewPK);
    }

    /** As {@link #dmlPreToPostFinal(OperatorCreator, List, boolean)} with default expected failure. */
    protected void dmlPreToPostFinal(OperatorCreator dmlCreator) {
        dmlPreToPostFinal(dmlCreator, getGroupExpected(), true);
    }
    protected void dmlPreToPostFinal(OperatorCreator dmlCreator,
                                     List<Row> expectedRows,
                                     boolean isDMLFailing){
        dmlPreToPostFinal(dmlCreator, expectedRows, isDMLFailing, null, null, null, null, false);
    }

    /** DML transaction starting prior to DDL FINAL and committing after DDL FINAL. */
    protected void dmlPreToPostFinal(OperatorCreator dmlCreator,
                                     List<Row> expectedRows,
                                     boolean isDMLFailing,
                                     List<DataTypeDescriptor> descriptors,
                                     List<String> columnNames,
                                     OnlineCreateTableAsMT.TestSession  server,
                                     String sqlQuery,
                                     boolean ignoreNewPK) {
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
            .rollbackRetry(!isDMLFailing)
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
                                                 isDMLFailing ? "DML:"+getFailingDMLMarkString() : null);
        assertEquals("DML row count", 1, threads.get(1).getScannedRows().size());
        checkExpectedRows(expectedRows, ignoreNewPK);
    }

    protected void checkExpectedRows(List<Row> expectedRows,
                                     boolean ignoreNewPK) {
        checkExpectedRows(expectedRows, getGroupCreator(), ignoreNewPK);
    }

    protected void checkExpectedRows(List<Row> expectedRows,
                                     OperatorCreator groupCreator) {
        checkExpectedRows(expectedRows, groupCreator, false);
    }



    protected void checkExpectedRows(List<Row> expectedRows,
                                     OperatorCreator groupCreator,
                                     boolean ignoreNewPK) {
        compareRows(expectedRows, runPlanTxn(groupCreator));
        postCheckAIS(ais());
        List<Row> otherExpected = getOtherExpected();
        if(otherExpected != null) {
            compareRows(otherExpected, runPlanTxn(getOtherCreator()), ignoreNewPK);
        }
    }
}
