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

import com.akiban.server.service.session.Session;
import com.akiban.util.MultipleCauseException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class CompositeHook implements DStarLFunctionsHook {
    private final Session.Key<Integer> COUNT = Session.Key.of("SUCCESS_COUNT");

    private final List<DStarLFunctionsHook> hooks;

    public CompositeHook(List<DStarLFunctionsHook> hooks) {
        this.hooks = Collections.unmodifiableList(new ArrayList<DStarLFunctionsHook>(hooks));
    }

    @Override
    public void hookFunctionIn(Session session, DDLFunction function) {
        assert session.get(COUNT) == null : session.get(COUNT);

        int successes = 0;
        try {
            for (DStarLFunctionsHook hook : hooks) {
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
    public void hookFunctionCatch(Session session, DDLFunction function, Throwable throwable) {
        RuntimeException eToThrow = null;
        for (DStarLFunctionsHook hook : hooks(session) ) {
            try {
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
    public void hookFunctionFinally(Session session, DDLFunction function, Throwable throwable) {
        RuntimeException eToThrow = null;
        for (DStarLFunctionsHook hook : hooks(session) ) {
            try {
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

    private List<DStarLFunctionsHook> hooks(Session session) {
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
