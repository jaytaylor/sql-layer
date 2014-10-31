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

package com.foundationdb.server.service.dxl;

import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.api.DMLFunctions;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.service.dxl.DXLFunctionsHook.DXLFunction;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.session.SessionService;

import java.util.List;

import static com.foundationdb.util.Exceptions.throwAlways;

public final class HookableDMLFunctions implements DMLFunctions {

    private final DMLFunctions delegate;
    private final DXLFunctionsHook hook;
    private final SessionService sessionService;

    public HookableDMLFunctions(DMLFunctions delegate, List<DXLFunctionsHook> hooks, SessionService sessionService) {
        this.delegate = delegate;
        this.sessionService = sessionService;
        this.hook = hooks.size() == 1 ? hooks.get(0) : new CompositeHook(hooks);
    }

    @Override
    public NewRow wrapRowData(Session session, RowData rowData) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.CONVERT_ROW_DATA);
            return delegate.wrapRowData(session, rowData);
        } catch (RuntimeException t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.CONVERT_ROW_DATA, t);
            throw t;
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.CONVERT_ROW_DATA, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunctionsHook.DXLFunction.CONVERT_ROW_DATA, thrown);
        }
    }

    @Override
    public NewRow convertRowData(Session session, RowData rowData) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.CONVERT_ROW_DATA);
            return delegate.convertRowData(session, rowData);
        } catch (RuntimeException t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.CONVERT_ROW_DATA, t);
            throw t;
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.CONVERT_ROW_DATA, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunctionsHook.DXLFunction.CONVERT_ROW_DATA, thrown);
        }
    }

    @Override
    public List<NewRow> convertRowDatas(Session session, List<RowData> rowDatas) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.CONVERT_ROW_DATAS);
            return delegate.convertRowDatas(session, rowDatas);
        } catch (RuntimeException t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.CONVERT_ROW_DATAS, t);
            throw t;
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.CONVERT_ROW_DATAS, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.CONVERT_ROW_DATAS, thrown);
        }
    }

    @Override
    public void truncateTable(final Session session, final int tableId) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunctionsHook.DXLFunction.TRUNCATE_TABLE);
            delegate.truncateTable(session, tableId);
        } catch (RuntimeException t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.TRUNCATE_TABLE, t);
            throw t;
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.TRUNCATE_TABLE, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.TRUNCATE_TABLE, thrown);
        }
    }

    @Override
    public void truncateTable(final Session session, final int tableId, boolean descendants) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunctionsHook.DXLFunction.TRUNCATE_TABLE);
            delegate.truncateTable(session, tableId, descendants);
        } catch (RuntimeException t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.TRUNCATE_TABLE, t);
            throw t;
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.TRUNCATE_TABLE, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.TRUNCATE_TABLE, thrown);
        }
    }
}
