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

package com.akiban.server.service.dxl;

import com.akiban.ais.model.TableName;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.dml.scan.BufferFullException;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.LegacyRowOutput;
import com.akiban.server.api.dml.scan.RowOutput;
import com.akiban.server.error.CursorIsUnknownException;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;
import com.akiban.server.store.statistics.IndexStatisticsService;
import com.akiban.server.t3expressions.T3RegistryService;
import com.google.inject.Inject;
import com.persistit.Transaction;

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
        return new ConcurrencyAtomicsDDLFunctions(middleman, schemaManager(), store(), treeService(), indexStatisticsService(), configService(), t3Registry());
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
    public ConcurrencyAtomicsDXLService(SchemaManager schemaManager, Store store, TreeService treeService, SessionService sessionService,
                                        IndexStatisticsService indexStatisticsService, ConfigurationService configService,
                                        T3RegistryService t3Registry, TransactionService txnService) {
        super(schemaManager, store, treeService, sessionService, indexStatisticsService, configService, t3Registry, txnService, null);
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

        private ConcurrencyAtomicsDDLFunctions(BasicDXLMiddleman middleman, SchemaManager schemaManager, Store store, TreeService treeService,
                                               IndexStatisticsService indexStatisticsService, ConfigurationService configService, T3RegistryService t3Registry) {
            super(middleman, schemaManager, store, treeService, indexStatisticsService, configService, t3Registry, null, null);
        }
    }
}
