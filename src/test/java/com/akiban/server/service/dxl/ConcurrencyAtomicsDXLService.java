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

package com.akiban.server.service.dxl;

import com.akiban.ais.model.TableName;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.dml.scan.BufferFullException;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.LegacyRowOutput;
import com.akiban.server.api.dml.scan.RowOutput;
import com.akiban.server.error.CursorIsUnknownException;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;
import com.akiban.server.store.statistics.IndexStatisticsService;
import com.google.inject.Inject;

import java.util.Collection;

public final class ConcurrencyAtomicsDXLService extends DXLServiceImpl {

    private final static Session.Key<BeforeAndAfter> DELAY_ON_DROP_INDEX = Session.Key.named("DELAY_ON_DROP_INDEX");
    private final static Session.Key<ScanHooks> SCANHOOKS_KEY = Session.Key.named("SCANHOOKS");
    private static final Session.Key<BeforeAndAfter> DELAY_ON_DROP_TABLE = Session.Key.named("DELAY_ON_DROP_TABLE");

    private static class BeforeAndAfter {
        private final Runnable before;
        private final Runnable after;

        private BeforeAndAfter(Runnable before, Runnable after) {
            this.before = before;
            this.after = after;
        }

        public void doBefore() {
            if (before != null) {
                before.run();
            }
        }

        public void doAfter() {
            if (after != null) {
                after.run();
            }
        }
    }

    public interface ScanHooks extends BasicDMLFunctions.ScanHooks {
        // not adding anything, just promoting visibility
    }

    @Override
    DMLFunctions createDMLFunctions(BasicDXLMiddleman middleman, DDLFunctions newlyCreatedDDLF) {
        return new ScanhooksDMLFunctions(middleman, schemaManager(), store(), treeService(), newlyCreatedDDLF);
    }

    @Override
    DDLFunctions createDDLFunctions(BasicDXLMiddleman middleman) {
        return new ConcurrencyAtomicsDDLFunctions(middleman, schemaManager(), store(), treeService(), indexStatisticsService());
    }

    public static ScanHooks installScanHook(Session session, ScanHooks hook) {
        return session.put(SCANHOOKS_KEY, hook);
    }

    public static ScanHooks removeScanHook(Session session) {
        return session.remove(SCANHOOKS_KEY);
    }

    public static boolean isScanHookInstalled(Session session) {
        return session.get(SCANHOOKS_KEY) != null;
    }

    public static void hookNextDropIndex(Session session, Runnable beforeRunnable, Runnable afterRunnable)
    {
        session.put(DELAY_ON_DROP_INDEX, new BeforeAndAfter(beforeRunnable, afterRunnable));
    }

    public static boolean isDropIndexHookInstalled(Session session) {
        return session.get(DELAY_ON_DROP_INDEX) != null;
    }

    public static void hookNextDropTable(Session session, Runnable beforeRunnable, Runnable afterRunnable)
    {
        session.put(DELAY_ON_DROP_TABLE, new BeforeAndAfter(beforeRunnable, afterRunnable));
    }

    public static boolean isDropTableHookInstalled(Session session) {
        return session.get(DELAY_ON_DROP_TABLE) != null;
    }

    @Inject
    public ConcurrencyAtomicsDXLService(SchemaManager schemaManager, Store store, TreeService treeService, SessionService sessionService, IndexStatisticsService indexStatisticsService) {
        super(schemaManager, store, treeService, sessionService, indexStatisticsService);
    }

    public class ScanhooksDMLFunctions extends BasicDMLFunctions {
        ScanhooksDMLFunctions(BasicDXLMiddleman middleman, SchemaManager schemaManager, Store store, TreeService treeService, DDLFunctions ddlFunctions) {
            super(middleman, schemaManager, store, treeService, ddlFunctions);
        }

        @Override
        public void scanSome(Session session, CursorId cursorId, LegacyRowOutput output)
                throws CursorIsUnknownException,
                BufferFullException {
            ScanHooks hooks = session.remove(SCANHOOKS_KEY);
            if (hooks == null) {
                hooks = BasicDMLFunctions.DEFAULT_SCAN_HOOK;
            }
            super.scanSome(session, cursorId, output, hooks);
        }

        @Override
        public void scanSome(Session session, CursorId cursorId, RowOutput output)
                throws CursorIsUnknownException
        {
            ScanHooks hooks = session.remove(SCANHOOKS_KEY);
            if (hooks == null) {
                hooks = BasicDMLFunctions.DEFAULT_SCAN_HOOK;
            }
            super.scanSome(session, cursorId, output, hooks);
        }
    }

    private static class ConcurrencyAtomicsDDLFunctions extends BasicDDLFunctions {
        @Override
        public void dropTableIndexes(Session session, TableName tableName, Collection<String> indexNamesToDrop) {
            BeforeAndAfter hook = session.remove(DELAY_ON_DROP_INDEX);
            if (hook != null) {
                hook.doBefore();
            }
            super.dropTableIndexes(session, tableName, indexNamesToDrop);
            if (hook != null) {
                hook.doAfter();
            }
        }

        @Override
        public void dropTable(Session session, TableName tableName) {
            BeforeAndAfter hook = session.remove(DELAY_ON_DROP_TABLE);
            if (hook != null) {
                hook.doBefore();
            }
            super.dropTable(session, tableName);
            if (hook != null) {
                hook.doAfter();
            }
        }

        private ConcurrencyAtomicsDDLFunctions(BasicDXLMiddleman middleman, SchemaManager schemaManager, Store store, TreeService treeService, IndexStatisticsService indexStatisticsService) {
            super(middleman, schemaManager, store, treeService, indexStatisticsService);
        }
    }
}
