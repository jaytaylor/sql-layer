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

import com.akiban.server.service.session.Session;
import com.akiban.util.MultipleCauseException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

public final class CompositeHook implements DXLFunctionsHook {
    private final Session.Key<Integer> COUNT = Session.Key.named("SUCCESS_COUNT");

    private final List<DXLFunctionsHook> hooks;

    public CompositeHook(List<DXLFunctionsHook> hooks) {
        this.hooks = Collections.unmodifiableList(new ArrayList<DXLFunctionsHook>(hooks));
    }

    @Override
    public void hookFunctionIn(Session session, DXLFunction function) {
        assert session.get(COUNT) == null : session.get(COUNT);

        int successes = 0;
        try {
            for (DXLFunctionsHook hook : hooks) {
                hook.hookFunctionIn(session, function);
                ++successes;
            }
        } catch (RuntimeException e) {
            Integer old = session.put(COUNT, successes);
            assert old == null : old;
            throw e;
        }
    }

    @Override
    public void hookFunctionCatch(Session session, DXLFunction function, Throwable throwable) {
        RuntimeException eToThrow = null;
        List<DXLFunctionsHook> allHooks = hooks(session);
        ListIterator<DXLFunctionsHook> revIt = allHooks.listIterator(allHooks.size());
        while (revIt.hasPrevious()) {
            try {
                DXLFunctionsHook hook = revIt.previous();
                hook.hookFunctionCatch(session, function, throwable);
            } catch (RuntimeException e) {
                eToThrow = forException(eToThrow, e);
            }
        }
        if (eToThrow != null) {
            throw eToThrow;
        }
    }

    @Override
    public void hookFunctionFinally(Session session, DXLFunction function, Throwable throwable) {
        RuntimeException eToThrow = null;
        List<DXLFunctionsHook> allHooks = hooks(session);
        ListIterator<DXLFunctionsHook> revIt = allHooks.listIterator(allHooks.size());
        while (revIt.hasPrevious()) {
            try {
                DXLFunctionsHook hook = revIt.previous();
                hook.hookFunctionFinally(session, function, throwable);
            } catch (RuntimeException e) {
                eToThrow = forException(eToThrow, e);
            }
        }
        session.remove(COUNT);
        if (eToThrow != null) {
            throw eToThrow;
        }
    }

    private List<DXLFunctionsHook> hooks(Session session) {
        Integer previousSuccesses = session.get(COUNT);
        return previousSuccesses == null ? this.hooks : hooks.subList(0, previousSuccesses + 1);
    }

    private RuntimeException forException(RuntimeException aggregate, RuntimeException exception) {
        if (aggregate == null) {
            return exception;
        }
        if (aggregate instanceof MultipleCauseException) {
            ((MultipleCauseException)aggregate).addCause(exception);
            return aggregate;
        }
        MultipleCauseException multipleCauseException = new MultipleCauseException();
        multipleCauseException.addCause(aggregate);
        multipleCauseException.addCause(exception);
        return multipleCauseException;
    }
}
