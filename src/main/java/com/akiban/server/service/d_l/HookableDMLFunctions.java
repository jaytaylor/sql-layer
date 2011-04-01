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

package com.akiban.server.service.d_l;

import com.akiban.server.RowData;
import com.akiban.server.TableStatistics;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.GenericInvalidOperationException;
import com.akiban.server.api.common.NoSuchTableException;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.DuplicateKeyException;
import com.akiban.server.api.dml.ForeignKeyConstraintDMLException;
import com.akiban.server.api.dml.NoSuchColumnException;
import com.akiban.server.api.dml.NoSuchIndexException;
import com.akiban.server.api.dml.NoSuchRowException;
import com.akiban.server.api.dml.TableDefinitionMismatchException;
import com.akiban.server.api.dml.UnsupportedModificationException;
import com.akiban.server.api.dml.scan.BufferFullException;
import com.akiban.server.api.dml.scan.ConcurrentScanAndUpdateException;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.CursorIsFinishedException;
import com.akiban.server.api.dml.scan.CursorIsUnknownException;
import com.akiban.server.api.dml.scan.CursorState;
import com.akiban.server.api.dml.scan.LegacyRowOutput;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.RowOutput;
import com.akiban.server.api.dml.scan.RowOutputException;
import com.akiban.server.api.dml.scan.ScanRequest;
import com.akiban.server.service.d_l.DStarLFunctionsHook.DDLFunction;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;

import java.util.List;
import java.util.Set;

import static com.akiban.server.service.d_l.HookUtil.throwIf;
import static com.akiban.server.service.d_l.HookUtil.throwAlways;

public final class HookableDMLFunctions implements DMLFunctions {

    private final DMLFunctions delegate;
    private final DStarLFunctionsHook hook;

    public HookableDMLFunctions(DMLFunctions delegate, List<DStarLFunctionsHook> hooks) {
        this.delegate = delegate;
        this.hook = hooks.size() == 1 ? hooks.get(0) : new CompositeHook(hooks);
    }

    @Override
    public TableStatistics getTableStatistics(Session session, int tableId, boolean updateFirst) throws NoSuchTableException, GenericInvalidOperationException {
        try {
            hook.hookFunctionIn(session, DDLFunction.GET_TABLE_STATISTICS);
            return delegate.getTableStatistics(session, tableId, updateFirst);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.GET_TABLE_STATISTICS, t);
            throwIf(t, NoSuchTableException.class);
            throwIf(t, GenericInvalidOperationException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.GET_TABLE_STATISTICS);
        }
    }

    @Override
    public CursorId openCursor(Session session, ScanRequest request) throws NoSuchTableException, NoSuchColumnException, NoSuchIndexException, GenericInvalidOperationException {
        try {
            hook.hookFunctionIn(session, DDLFunction.OPEN_CURSOR);
            return delegate.openCursor(session, request);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.OPEN_CURSOR, t);
            throwIf(t, NoSuchTableException.class);
            throwIf(t, NoSuchColumnException.class);
            throwIf(t, NoSuchIndexException.class);
            throwIf(t, GenericInvalidOperationException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.OPEN_CURSOR);
        }
    }

    @Override
    public CursorState getCursorState(Session session, CursorId cursorId) {
        try {
            hook.hookFunctionIn(session, DDLFunction.GET_CURSOR_STATE);
            return delegate.getCursorState(session, cursorId);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.GET_CURSOR_STATE, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.GET_CURSOR_STATE);
        }
    }

    @Override
    public boolean scanSome(Session session, CursorId cursorId, LegacyRowOutput output) throws CursorIsFinishedException, CursorIsUnknownException, RowOutputException, BufferFullException, ConcurrentScanAndUpdateException, GenericInvalidOperationException {
        try {
            hook.hookFunctionIn(session, DDLFunction.SCAN_SOME);
            return delegate.scanSome(session, cursorId, output);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.SCAN_SOME, t);
            throwIf(t, CursorIsFinishedException.class);
            throwIf(t, CursorIsUnknownException.class);
            throwIf(t, RowOutputException.class);
            throwIf(t, BufferFullException.class);
            throwIf(t, ConcurrentScanAndUpdateException.class);
            throwIf(t, GenericInvalidOperationException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.SCAN_SOME);
        }
    }

    @Override
    public boolean scanSome(Session session, CursorId cursorId, RowOutput output) throws CursorIsFinishedException, CursorIsUnknownException, RowOutputException, NoSuchTableException, ConcurrentScanAndUpdateException, GenericInvalidOperationException {
        try {
            hook.hookFunctionIn(session, DDLFunction.SCAN_SOME);
            return delegate.scanSome(session, cursorId, output);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.SCAN_SOME, t);
            throwIf(t, CursorIsFinishedException.class);
            throwIf(t, CursorIsUnknownException.class);
            throwIf(t, RowOutputException.class);
            throwIf(t, NoSuchTableException.class);
            throwIf(t, ConcurrentScanAndUpdateException.class);
            throwIf(t, GenericInvalidOperationException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.SCAN_SOME);
        }
    }

    @Override
    public void closeCursor(Session session, CursorId cursorId) throws CursorIsUnknownException {
        try {
            hook.hookFunctionIn(session, DDLFunction.CLOSE_CURSOR);
            delegate.closeCursor(session, cursorId);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.CLOSE_CURSOR, t);
            throwIf(t, CursorIsUnknownException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.CLOSE_CURSOR);
        }
    }

    @Override
    public Set<CursorId> getCursors(Session session) {
        try {
            hook.hookFunctionIn(session, DDLFunction.GET_CURSORS);
            return delegate.getCursors(session);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.GET_CURSORS, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.GET_CURSORS);
        }
    }

    @Override
    public RowData convertNewRow(NewRow row) throws NoSuchTableException {
        Session session = new SessionImpl();
        try {
            hook.hookFunctionIn(session, DDLFunction.CONVERT_NEW_ROW);
            return delegate.convertNewRow(row);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.CONVERT_NEW_ROW, t);
            throwIf(t, NoSuchTableException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.CONVERT_NEW_ROW);
        }
    }

    @Override
    public NewRow convertRowData(RowData rowData) throws NoSuchTableException {
        Session session = new SessionImpl();
        try {
            hook.hookFunctionIn(session, DDLFunction.CONVERT_ROW_DATA);
            return delegate.convertRowData(rowData);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.CONVERT_ROW_DATA, t);
            throwIf(t, NoSuchTableException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.CONVERT_ROW_DATA);
        }
    }

    @Override
    public List<NewRow> convertRowDatas(List<RowData> rowDatas) throws NoSuchTableException {
        Session session = new SessionImpl();
        try {
            hook.hookFunctionIn(session, DDLFunction.CONVERT_ROW_DATAS);
            return delegate.convertRowDatas(rowDatas);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.CONVERT_ROW_DATAS, t);
            throwIf(t, NoSuchTableException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.CONVERT_ROW_DATAS);
        }
    }

    @Override
    public Long writeRow(Session session, NewRow row) throws NoSuchTableException, UnsupportedModificationException, TableDefinitionMismatchException, DuplicateKeyException, GenericInvalidOperationException {
        try {
            hook.hookFunctionIn(session, DDLFunction.WRITE_ROW);
            return delegate.writeRow(session, row);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.WRITE_ROW, t);
            throwIf(t, NoSuchTableException.class);
            throwIf(t, UnsupportedModificationException.class);
            throwIf(t, TableDefinitionMismatchException.class);
            throwIf(t, DuplicateKeyException.class);
            throwIf(t, GenericInvalidOperationException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.WRITE_ROW);
        }
    }

    @Override
    public void deleteRow(Session session, NewRow row) throws NoSuchTableException, UnsupportedModificationException, ForeignKeyConstraintDMLException, NoSuchRowException, TableDefinitionMismatchException, GenericInvalidOperationException {
        try {
            hook.hookFunctionIn(session, DDLFunction.DELETE_ROW);
            delegate.deleteRow(session, row);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.DELETE_ROW, t);
            throwIf(t, NoSuchTableException.class);
            throwIf(t, UnsupportedModificationException.class);
            throwIf(t, ForeignKeyConstraintDMLException.class);
            throwIf(t, NoSuchRowException.class);
            throwIf(t, TableDefinitionMismatchException.class);
            throwIf(t, GenericInvalidOperationException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.DELETE_ROW);
        }
    }

    @Override
    public void updateRow(Session session, NewRow oldRow, NewRow newRow, ColumnSelector columnSelector) throws NoSuchTableException, DuplicateKeyException, TableDefinitionMismatchException, UnsupportedModificationException, ForeignKeyConstraintDMLException, NoSuchRowException, GenericInvalidOperationException {
        try {
            hook.hookFunctionIn(session, DDLFunction.UPDATE_ROW);
            delegate.updateRow(session, oldRow, newRow, columnSelector);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.UPDATE_ROW, t);
            throwIf(t, NoSuchTableException.class);
            throwIf(t, DuplicateKeyException.class);
            throwIf(t, TableDefinitionMismatchException.class);
            throwIf(t, UnsupportedModificationException.class);
            throwIf(t, ForeignKeyConstraintDMLException.class);
            throwIf(t, NoSuchRowException.class);
            throwIf(t, GenericInvalidOperationException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.UPDATE_ROW);
        }
    }

    @Override
    public void truncateTable(final Session session, final int tableId) throws NoSuchTableException, UnsupportedModificationException, ForeignKeyConstraintDMLException, GenericInvalidOperationException {
        try {
            hook.hookFunctionIn(session, DDLFunction.TRUNCATE_TABLE);
            delegate.truncateTable(session, tableId);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.TRUNCATE_TABLE, t);
            throwIf(t, NoSuchTableException.class);
            throwIf(t, UnsupportedModificationException.class);
            throwIf(t, ForeignKeyConstraintDMLException.class);
            throwIf(t, GenericInvalidOperationException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.TRUNCATE_TABLE);
        }
    }
}
