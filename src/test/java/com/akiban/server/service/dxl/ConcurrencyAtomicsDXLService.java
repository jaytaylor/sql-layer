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
import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.GenericInvalidOperationException;
import com.akiban.server.api.common.NoSuchTableException;
import com.akiban.server.api.dml.scan.BufferFullException;
import com.akiban.server.api.dml.scan.ConcurrentScanAndUpdateException;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.CursorIsFinishedException;
import com.akiban.server.api.dml.scan.CursorIsUnknownException;
import com.akiban.server.api.dml.scan.LegacyRowOutput;
import com.akiban.server.api.dml.scan.RowOutput;
import com.akiban.server.api.dml.scan.RowOutputException;
import com.akiban.server.mttests.mtutil.Timing;
import com.akiban.server.service.session.Session;

import java.util.Collection;

public final class ConcurrencyAtomicsDXLService extends DXLServiceImpl {

    private final static Session.Key<Long> DELAY_ON_DROP_INDEX = Session.Key.of("DELAY_ON_DROP_INDEX");
    private final static Session.Key<ScanHooks> SCANHOOKS_KEY = Session.Key.of("SCANHOOKS");

    public interface ScanHooks extends BasicDMLFunctions.ScanHooks {
        // not adding anything, just promoting visibility
    }

    @Override
    DMLFunctions createDMLFunctions(DDLFunctions newlyCreatedDDLF) {
        return new ScanhooksDMLFunctions(newlyCreatedDDLF);
    }

    @Override
    DDLFunctions createDDLFunctions() {
        return new ConcurrencyAtomicsDDLFunctions();
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

    public static void delayNextDropIndex(Session session, long amount) {
        session.put(DELAY_ON_DROP_INDEX, amount);
    }

    public static boolean isDropIndexDelayInstalled(Session session) {
        return session.get(DELAY_ON_DROP_INDEX) != null;
    }

    public class ScanhooksDMLFunctions extends BasicDMLFunctions {
        ScanhooksDMLFunctions(DDLFunctions ddlFunctions) {
            super(ddlFunctions);
        }

        @Override
        public boolean scanSome(Session session, CursorId cursorId, LegacyRowOutput output)
                throws CursorIsFinishedException,
                CursorIsUnknownException,
                RowOutputException,
                BufferFullException,
                ConcurrentScanAndUpdateException,
                GenericInvalidOperationException
        {
            ScanHooks hooks = session.remove(SCANHOOKS_KEY);
            if (hooks == null) {
                hooks = BasicDMLFunctions.NONE;
            }
            return super.scanSome(session, cursorId, output, hooks);
        }

        @Override
        public boolean scanSome(Session session, CursorId cursorId, RowOutput output)
                throws CursorIsFinishedException,
                CursorIsUnknownException,
                RowOutputException,
                ConcurrentScanAndUpdateException,
                NoSuchTableException,
                GenericInvalidOperationException
        {
            ScanHooks hooks = session.remove(SCANHOOKS_KEY);
            if (hooks == null) {
                hooks = BasicDMLFunctions.NONE;
            }
            return super.scanSome(session, cursorId, output, hooks);
        }
    }

    private static class ConcurrencyAtomicsDDLFunctions extends BasicDDLFunctions {
        @Override
        public void dropIndexes(Session session, TableName tableName, Collection<String> indexNamesToDrop) throws InvalidOperationException {
            Long shouldDelay = session.remove(DELAY_ON_DROP_INDEX);
            if (shouldDelay != null) {
                Timing.sleep(shouldDelay);
            }
            super.dropIndexes(session, tableName, indexNamesToDrop);
        }
    }
}
