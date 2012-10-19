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

import com.akiban.server.rowdata.RowData;
import com.akiban.server.TableStatistics;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.BufferFullException;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.CursorState;
import com.akiban.server.api.dml.scan.LegacyRowOutput;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.RowOutput;
import com.akiban.server.api.dml.scan.ScanRequest;
import com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;

import java.util.List;
import java.util.Set;

import static com.akiban.util.Exceptions.throwAlways;

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
    public TableStatistics getTableStatistics(Session session, int tableId, boolean updateFirst) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunctionsHook.DXLFunction.GET_TABLE_STATISTICS);
            return delegate.getTableStatistics(session, tableId, updateFirst);
        } catch (RuntimeException t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.GET_TABLE_STATISTICS, t);
            throw t;
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.GET_TABLE_STATISTICS, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunctionsHook.DXLFunction.GET_TABLE_STATISTICS, thrown);
        }
    }

    @Override
    public CursorId openCursor(Session session, int knownAIS, ScanRequest request) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.OPEN_CURSOR);
            return delegate.openCursor(session, knownAIS, request);
        } catch (RuntimeException t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.OPEN_CURSOR, t);
            throw t;
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.OPEN_CURSOR, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunctionsHook.DXLFunction.OPEN_CURSOR, thrown);
        }
    }

    @Override
    public CursorState getCursorState(Session session, CursorId cursorId) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunctionsHook.DXLFunction.GET_CURSOR_STATE);
            return delegate.getCursorState(session, cursorId);
        } catch (RuntimeException t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.GET_CURSOR_STATE, t);
            throw t;
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.GET_CURSOR_STATE, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunctionsHook.DXLFunction.GET_CURSOR_STATE, thrown);
        }
    }

    @Override
    public void scanSome(Session session, CursorId cursorId, LegacyRowOutput output) throws BufferFullException {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunctionsHook.DXLFunction.SCAN_SOME);
            delegate.scanSome(session, cursorId, output);
        } catch (RuntimeException t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.SCAN_SOME, t);
            throw t;
        } catch (BufferFullException ex) {
            thrown = ex; 
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.SCAN_SOME, ex);
            throw ex;
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.SCAN_SOME, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunctionsHook.DXLFunction.SCAN_SOME, thrown);
        }
    }

    @Override
    public void scanSome(Session session, CursorId cursorId, RowOutput output) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunctionsHook.DXLFunction.SCAN_SOME);
            delegate.scanSome(session, cursorId, output);
        } catch (RuntimeException t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.SCAN_SOME, t);
            throw t;
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.SCAN_SOME, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunctionsHook.DXLFunction.SCAN_SOME, thrown);
        }
    }

    @Override
    public void closeCursor(Session session, CursorId cursorId) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.CLOSE_CURSOR);
            delegate.closeCursor(session, cursorId);
        } catch (RuntimeException t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.CLOSE_CURSOR, t);
            throw t;
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.CLOSE_CURSOR, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.CLOSE_CURSOR, thrown);
        }
    }

    @Override
    public Set<CursorId> getCursors(Session session) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.GET_CURSORS);
            return delegate.getCursors(session);
        } catch (RuntimeException t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.GET_CURSORS, t);
            throw t;
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.GET_CURSORS, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.GET_CURSORS, thrown);
        }
    }

    @Override
    public RowData convertNewRow(NewRow row){
        Session session = sessionService.createSession();
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunctionsHook.DXLFunction.CONVERT_NEW_ROW);
            return delegate.convertNewRow(row);
        } catch (RuntimeException t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.CONVERT_NEW_ROW, t);
            throw t;
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.CONVERT_NEW_ROW, t);
            throw throwAlways(t);
        } finally {
            try {
                hook.hookFunctionFinally(session, DXLFunctionsHook.DXLFunction.CONVERT_NEW_ROW, thrown);
            } finally {
                session.close();
            }
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
    public Long writeRow(Session session, NewRow row) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.WRITE_ROW);
            return delegate.writeRow(session, row);
        } catch (RuntimeException t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.WRITE_ROW, t);
            throw t;
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.WRITE_ROW, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunctionsHook.DXLFunction.WRITE_ROW, thrown);
        }
    }

    @Override
    public void deleteRow(Session session, NewRow row) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunctionsHook.DXLFunction.DELETE_ROW);
            delegate.deleteRow(session, row);
        } catch (RuntimeException t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.DELETE_ROW, t);
            throw t;
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.DELETE_ROW, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunctionsHook.DXLFunction.DELETE_ROW, thrown);
        }
    }

    @Override
    public void updateRow(Session session, NewRow oldRow, NewRow newRow, ColumnSelector columnSelector) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunctionsHook.DXLFunction.UPDATE_ROW);
            delegate.updateRow(session, oldRow, newRow, columnSelector);
        } catch (RuntimeException t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.UPDATE_ROW, t);
            throw t;
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.UPDATE_ROW, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunctionsHook.DXLFunction.UPDATE_ROW, thrown);
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
}
