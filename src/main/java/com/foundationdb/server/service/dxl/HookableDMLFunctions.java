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

import com.foundationdb.server.api.DMLFunctions;
import com.foundationdb.server.service.dxl.DXLFunctionsHook.DXLFunction;
import com.foundationdb.server.service.session.Session;

import java.util.List;

import static com.foundationdb.util.Exceptions.throwAlways;

public final class HookableDMLFunctions implements DMLFunctions {

    private final DMLFunctions delegate;
    private final DXLFunctionsHook hook;

    public HookableDMLFunctions(DMLFunctions delegate, List<DXLFunctionsHook> hooks) {
        this.delegate = delegate;
        this.hook = hooks.size() == 1 ? hooks.get(0) : new CompositeHook(hooks);
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
