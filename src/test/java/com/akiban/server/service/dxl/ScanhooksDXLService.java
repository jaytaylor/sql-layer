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
import com.akiban.server.service.session.Session;

public final class ScanhooksDXLService extends DXLServiceImpl {

    private final static Session.Key<ScanHooks> SCANHOOKS_KEY = Session.Key.of("SCANHOOKS");

    public interface ScanHooks extends BasicDMLFunctions.ScanHooks {
        // not adding anything, just promoting visibility
    }

    @Override
    DMLFunctions createDMLFunctions(DDLFunctions newlyCreatedDDLF) {
        return new ScanhooksDMLFunctions(newlyCreatedDDLF);
    }

    public ScanHooks installHook(Session session, ScanHooks hook) {
        return session.put(SCANHOOKS_KEY, hook);
    }

    public ScanHooks removeHook(Session session) {
        return session.remove(SCANHOOKS_KEY);
    }

    public boolean isHookInstalled(Session session) {
        return session.get(SCANHOOKS_KEY) != null;
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
}
